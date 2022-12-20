%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([add_topic_trie_binding/4, delete_topic_trie_bindings_for_exchange/1,
         delete_topic_trie_bindings/1, route_delivery_for_exchange_type_topic/2]).

-export([mnesia_write_to_khepri/2, mnesia_delete_to_khepri/2, clear_data_in_khepri/1]).

%% These functions are used to process mnesia deletion events generated during the
%% migration from mnesia to khepri
-export([split_topic_key/1, trie_binding_to_key/1, trie_records_to_key/1]).

-define(HASH, <<"#">>).
-define(STAR, <<"*">>).
-define(DOT, <<"\\.">>).
-define(ONE_WORD, <<"[^.]+">>).
-define(ANYTHING, <<".*">>).
-define(ZERO_OR_MORE, <<"(\\..+)?">>).

%% API
%% --------------------------------------------------------------

add_topic_trie_binding(XName, RoutingKey, Destination, Args) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        FinalNode = follow_down_create(XName, split_topic_key(RoutingKey)),
                        trie_add_binding(XName, FinalNode, Destination, Args),
                        ok
                end)
      end,
      fun () ->
              add_topic_trie_binding_in_khepri(XName, RoutingKey, Destination, Args)
      end).

delete_topic_trie_bindings_for_exchange(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        trie_remove_all_nodes(XName),
                        trie_remove_all_edges(XName),
                        trie_remove_all_bindings(XName),
                        ok
                end)
      end,
      fun() ->
              ok = rabbit_khepri:delete(khepri_exchange_type_topic_path(XName))
      end).

delete_topic_trie_bindings(Bs) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() -> remove_bindings_in_mnesia(Bs) end)
      end,
      fun() ->
              delete_topic_trie_bindings_in_khepri(Bs)
      end).

route_delivery_for_exchange_type_topic(XName, RoutingKey) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Words = split_topic_key(RoutingKey),
              mnesia:async_dirty(fun trie_match/2, [XName, Words])
      end,
      fun() ->
              route_delivery_for_exchange_type_topic_in_khepri(XName, RoutingKey)
      end).

%% Migration
%% --------------------------------------------------------------

mnesia_write_to_khepri(rabbit_topic_trie_binding, TrieBindings0) ->
    %% There isn't enough information to rebuild the tree as the routing key is split
    %% along the trie tree on mnesia. But, we can query the bindings table (migrated
    %% previosly) and migrate the entries that match this <X, D> combo.
    %% Multiple bindings to the same exchange/destination combo need to be migrated only once.
    %% Remove here the duplicates and use a temporary ets table to keep track of those already
    %% migrated to really speed up things.
    Table = ensure_topic_migration_ets(),
    TrieBindings1 =
        lists:uniq([{X, D} || #topic_trie_binding{trie_binding = #trie_binding{exchange_name = X,
                                                                               destination   = D}}
                                  <- TrieBindings0]),
    TrieBindings = lists:filter(fun({X, D}) ->
                                        ets:insert_new(Table, {{X, D}, empty})
                                end, TrieBindings1),
    ets:delete(Table),
    rabbit_khepri:transaction(
      fun() ->
              [begin
                   Values = rabbit_db_binding:match_source_and_destination_in_khepri_tx(X, D),
                   Bindings = lists:foldl(fun(SetOfBindings, Acc) ->
                                                  sets:to_list(SetOfBindings) ++ Acc
                                          end, [], Values),
                   [add_topic_trie_binding_tx(X, K, D, Args) || #binding{key = K,
                                                                         args = Args} <- Bindings]
               end || {X, D} <- TrieBindings]
      end);
mnesia_write_to_khepri(rabbit_topic_trie_node, _) ->
    %% Nothing to do, the `rabbit_topic_trie_binding` is enough to perform the migration
    %% as Khepri stores each topic binding as a single path
    ok;
mnesia_write_to_khepri(rabbit_topic_trie_edge, _) ->
    %% Nothing to do, the `rabbit_topic_trie_binding` is enough to perform the migration
    %% as Khepri stores each topic binding as a single path
    ok.

mnesia_delete_to_khepri(rabbit_topic_trie_binding, #topic_trie_binding{}) ->
    %% TODO No routing keys here, how do we do? Use the node_id to search on the tree?
    %% Can we still query mnesia content?
    ok;
mnesia_delete_to_khepri(rabbit_topic_trie_node, #topic_trie_node{}) ->
    %% TODO see above
    ok;
mnesia_delete_to_khepri(rabbit_topic_trie_edge, #topic_trie_edge{}) ->
    %% TODO see above
    ok.

clear_data_in_khepri(rabbit_topic_trie_binding) ->
    case rabbit_khepri:delete(khepri_exchange_type_topic_path()) of
        ok -> ok;
        Error -> throw(Error)
    end;
%% There is a single khepri entry for topics and it should be already deleted
clear_data_in_khepri(rabbit_topic_trie_node) ->
    ok;
clear_data_in_khepri(rabbit_topic_trie_edge) ->
    ok.

split_topic_key(Key) ->
    split_topic_key(Key, [], []).

trie_binding_to_key(#topic_trie_binding{trie_binding = #trie_binding{node_id = NodeId}}) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              follow_up_get_path(mnesia, rabbit_topic_trie_edge, NodeId)
      end).

trie_records_to_key(Records) ->
    Tab = ensure_topic_deletion_ets(),
    TrieBindings = lists:foldl(fun(#topic_trie_binding{} = R, Acc) ->
                                       [R | Acc];
                                  (#topic_trie_edge{} = R, Acc) ->
                                       ets:insert(Tab, R),
                                       Acc;
                                  (_, Acc) ->
                                       Acc
                               end, [], Records),
    List = lists:foldl(
             fun(#topic_trie_binding{trie_binding = #trie_binding{node_id = Node} = TB} = B,
                 Acc) ->
                     case follow_up_get_path(ets, Tab, Node) of
                         {error, not_found} -> [{TB, trie_binding_to_key(B)} | Acc];
                         RK -> [{TB, RK} | Acc]
                     end
             end, [], TrieBindings),
    ets:delete(Tab),
    List.

%% Internal
%% --------------------------------------------------------------

add_topic_trie_binding_in_khepri(XName, RoutingKey, Destination, Args) ->
    Path = khepri_exchange_type_topic_path(XName) ++ split_topic_key_binary(RoutingKey),
    {Path0, [Last]} = lists:split(length(Path) - 1, Path),
    Binding = #{destination => Destination, arguments => Args},
    rabbit_store:retry(
      fun() ->
              case rabbit_khepri:adv_get(Path) of
                  {ok, #{data := Set0, payload_version := Vsn}} ->
                      Set = sets:add_element(Binding, Set0),
                      Conditions = #if_all{conditions = [Last, #if_payload_version{version = Vsn}]},
                      UpdatePath = Path0 ++ [Conditions],
                      rabbit_khepri:put(UpdatePath, Set);
                  _ ->
                      Set = sets:add_element(Binding, sets:new()),
                      rabbit_khepri:put(Path, Set)
              end
      end).

add_topic_trie_binding_tx(XName, RoutingKey, Destination, Args) ->
    Path = khepri_exchange_type_topic_path(XName) ++ split_topic_key_binary(RoutingKey),
    Binding = #{destination => Destination, arguments => Args},
    Set0 = case khepri_tx:get(Path) of
               {ok, undefined} -> sets:new();
               {ok, S} -> S;
               _ -> sets:new()
           end,
    Set = sets:add_element(Binding, Set0),
    ok = khepri_tx:put(Path, Set).

route_delivery_for_exchange_type_topic_in_khepri(XName, RoutingKey) ->
    Root = khepri_exchange_type_topic_path(XName) ++ [rabbit_store:if_has_data_wildcard()],
    case rabbit_khepri:fold(
           Root,
           fun(Path0, #{data := Set}, Acc) ->
                   Path = lists:nthtail(4, Path0),
                   case is_re_topic_match(Path, RoutingKey) of
                       true ->
                           Bindings = sets:to_list(Set),
                           [maps:get(destination, B) || B <- Bindings] ++ Acc;
                       false ->
                           Acc
                   end
           end,
           []) of
        {ok, List} -> List;
        _ -> []
    end.

trie_remove_all_nodes(X) ->
    remove_all(rabbit_topic_trie_node,
               #topic_trie_node{trie_node = #trie_node{exchange_name = X,
                                                       _             = '_'},
                                _         = '_'}).

trie_remove_all_edges(X) ->
    remove_all(rabbit_topic_trie_edge,
               #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                       _             = '_'},
                                _         = '_'}).

trie_remove_all_bindings(X) ->
    remove_all(rabbit_topic_trie_binding,
               #topic_trie_binding{
                 trie_binding = #trie_binding{exchange_name = X, _ = '_'},
                 _            = '_'}).

remove_all(Table, Pattern) ->
    lists:foreach(fun (R) -> mnesia:delete_object(Table, R, write) end,
                  mnesia:match_object(Table, Pattern, write)).

remove_bindings_in_mnesia(Bs) ->
    %% See rabbit_binding:lock_route_tables for the rationale for
    %% taking table locks.
    case Bs of
        [_] -> ok;
        _   -> [mnesia:lock({table, T}, write) ||
                   T <- [rabbit_topic_trie_node,
                         rabbit_topic_trie_edge,
                         rabbit_topic_trie_binding]]
    end,
    [case follow_down_get_path(X, split_topic_key(K)) of
         {ok, Path = [{FinalNode, _} | _]} ->
             trie_remove_binding(X, FinalNode, D, Args),
             remove_path_if_empty(X, Path);
         {error, _Node, _RestW} ->
             %% We're trying to remove a binding that no longer exists.
             %% That's unexpected, but shouldn't be a problem.
             ok
     end ||  #binding{source = X, key = K, destination = D, args = Args} <- Bs],
    ok.

delete_topic_trie_bindings_in_khepri(Bs) ->
    %% Let's handle bindings data outside of the transaction for efficiency
    Data = [begin
                Path = khepri_exchange_type_topic_path(X) ++ split_topic_key_binary(K),
                {Path, #{destination => D, arguments => Args}}
            end || #binding{source = X, key = K, destination = D, args = Args} <- Bs],
    rabbit_khepri:transaction(
      fun() ->
              [begin
                   case khepri_tx:get(Path) of
                       {ok, undefined} ->
                           ok;
                       {ok, Set0} ->
                           Set = sets:del_element(Binding, Set0),
                           case sets:size(Set) of
                               0 -> khepri_tx:clear_payload(Path);
                               _ -> khepri_tx:put(Path, Set)
                           end;
                       _ ->
                           ok
                   end
               end || {Path, Binding} <- Data]
      end, rw),
    ok.

is_re_topic_match([?HASH], _) ->
    true;
is_re_topic_match(A, A) ->
    true;
is_re_topic_match([], <<>>) ->
    true;
is_re_topic_match([], _) ->
    false;
is_re_topic_match(Path00, RoutingKey) ->
    Path0 = path_to_re(Path00),
    Path = << <<B/binary >> || B <- Path0 >>,
    case Path of
        ?ANYTHING -> true;
        _ ->
            RE = <<$^,Path/binary,$$>>,
            case re:run(RoutingKey, RE, [{capture, none}]) of
                nomatch -> false;
                _ -> true
            end
    end.

path_to_re([?STAR | Rest]) ->
    path_to_re(Rest, [?ONE_WORD]);
path_to_re([?HASH | Rest]) ->
    path_to_re(Rest, [?ANYTHING]);
path_to_re([Bin | Rest]) ->
    path_to_re(Rest, [Bin]).

path_to_re([], Acc) ->
    lists:reverse(Acc);
path_to_re([?STAR | Rest], [?ANYTHING | _] = Acc) ->
    path_to_re(Rest, [?ONE_WORD | Acc]);
path_to_re([?STAR | Rest], Acc) ->
    path_to_re(Rest, [?ONE_WORD, ?DOT | Acc]);
path_to_re([?HASH | Rest], [?HASH | _] = Acc) ->
    path_to_re(Rest, Acc);
path_to_re([?HASH | Rest], [?ANYTHING | _] = Acc) ->
    path_to_re(Rest, Acc);
path_to_re([?HASH | Rest], Acc) ->
    path_to_re(Rest, [?ZERO_OR_MORE | Acc]);
path_to_re([Bin | Rest], [?ANYTHING | _] = Acc) ->
    path_to_re(Rest, [Bin | Acc]);
path_to_re([Bin | Rest], Acc) ->
    path_to_re(Rest, [Bin, ?DOT | Acc]).

split_topic_key_binary(<<>>) ->
    [<<>>];
split_topic_key_binary(Key) ->
    Words = split_topic_key(Key, [], []),
    [list_to_binary(W) || W <- Words].

split_topic_key(<<>>, [], []) ->
    [];
split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
    lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [C | RevWordAcc], RevResAcc).

follow_up_get_path(Mod, Tab, Node) ->
    follow_up_get_path(Mod, Tab, Node, []).

follow_up_get_path(_Mod, _Tab, root, Acc) ->
    Acc;
follow_up_get_path(Mod, Tab, Node, Acc) ->
    MatchHead = #topic_trie_edge{node_id = Node,
                                 trie_edge = '$1'},
    case Mod:select(Tab, [{MatchHead, [], ['$1']}]) of
        [#trie_edge{node_id = PreviousNode,
                    word = Word}] ->
            follow_up_get_path(Mod, Tab, PreviousNode, [Word | Acc]);
        [] ->
            {error, not_found}
    end.

trie_match(X, Words) ->
    trie_match(X, root, Words, []).

trie_match(X, Node, [], ResAcc) ->
    trie_match_part(X, Node, "#", fun trie_match_skip_any/4, [],
                    trie_bindings(X, Node) ++ ResAcc);
trie_match(X, Node, [W | RestW] = Words, ResAcc) ->
    lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
                        trie_match_part(X, Node, WArg, MatchFun, RestWArg, Acc)
                end, ResAcc, [{W, fun trie_match/4, RestW},
                              {"*", fun trie_match/4, RestW},
                              {"#", fun trie_match_skip_any/4, Words}]).

trie_match_part(X, Node, Search, MatchFun, RestW, ResAcc) ->
    case trie_child(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, ResAcc);
        error          -> ResAcc
    end.

trie_match_skip_any(X, Node, [], ResAcc) ->
    trie_match(X, Node, [], ResAcc);
trie_match_skip_any(X, Node, [_ | RestW] = Words, ResAcc) ->
    trie_match_skip_any(X, Node, RestW,
                        trie_match(X, Node, Words, ResAcc)).

follow_down_create(X, Words) ->
    case follow_down_last_node(X, Words) of
        {ok, FinalNode}      -> FinalNode;
        {error, Node, RestW} -> lists:foldl(
                                  fun (W, CurNode) ->
                                          NewNode = new_node_id(),
                                          trie_add_edge(X, CurNode, NewNode, W),
                                          NewNode
                                  end, Node, RestW)
    end.

new_node_id() ->
    rabbit_guid:gen().

follow_down_last_node(X, Words) ->
    follow_down(X, fun (_, Node, _) -> Node end, root, Words).

follow_down_get_path(X, Words) ->
    follow_down(X, fun (W, Node, PathAcc) -> [{Node, W} | PathAcc] end,
                [{root, none}], Words).

follow_down(X, AccFun, Acc0, Words) ->
    follow_down(X, root, AccFun, Acc0, Words).

follow_down(_X, _CurNode, _AccFun, Acc, []) ->
    {ok, Acc};
follow_down(X, CurNode, AccFun, Acc, Words = [W | RestW]) ->
    case trie_child(X, CurNode, W) of
        {ok, NextNode} -> follow_down(X, NextNode, AccFun,
                                      AccFun(W, NextNode, Acc), RestW);
        error          -> {error, Acc, Words}
    end.

remove_path_if_empty(_, [{root, none}]) ->
    ok;
remove_path_if_empty(X, [{Node, W} | [{Parent, _} | _] = RestPath]) ->
    case mnesia:read(rabbit_topic_trie_node,
                     #trie_node{exchange_name = X, node_id = Node}, write) of
        [] -> trie_remove_edge(X, Parent, Node, W),
              remove_path_if_empty(X, RestPath);
        _  -> ok
    end.

trie_child(X, Node, Word) ->
    case mnesia:read({rabbit_topic_trie_edge,
                      #trie_edge{exchange_name = X,
                                 node_id       = Node,
                                 word          = Word}}) of
        [#topic_trie_edge{node_id = NextNode}] -> {ok, NextNode};
        []                                     -> error
    end.

trie_bindings(X, Node) ->
    MatchHead = #topic_trie_binding{
      trie_binding = #trie_binding{exchange_name = X,
                                   node_id       = Node,
                                   destination   = '$1',
                                   arguments     = '_'}},
    mnesia:select(rabbit_topic_trie_binding, [{MatchHead, [], ['$1']}]).

trie_update_node_counts(X, Node, Field, Delta) ->
    E = case mnesia:read(rabbit_topic_trie_node,
                         #trie_node{exchange_name = X,
                                    node_id       = Node}, write) of
            []   -> #topic_trie_node{trie_node = #trie_node{
                                       exchange_name = X,
                                       node_id       = Node},
                                     edge_count    = 0,
                                     binding_count = 0};
            [E0] -> E0
        end,
    case setelement(Field, E, element(Field, E) + Delta) of
        #topic_trie_node{edge_count = 0, binding_count = 0} ->
            ok = mnesia:delete_object(rabbit_topic_trie_node, E, write);
        EN ->
            ok = mnesia:write(rabbit_topic_trie_node, EN, write)
    end.

trie_add_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, +1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:write/3).

trie_remove_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, -1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:delete_object/3).

trie_edge_op(X, FromNode, ToNode, W, Op) ->
    ok = Op(rabbit_topic_trie_edge,
            #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                    node_id       = FromNode,
                                                    word          = W},
                             node_id   = ToNode},
            write).

trie_add_binding(X, Node, D, Args) ->
    trie_update_node_counts(X, Node, #topic_trie_node.binding_count, +1),
    trie_binding_op(X, Node, D, Args, fun mnesia:write/3).

trie_remove_binding(X, Node, D, Args) ->
    trie_update_node_counts(X, Node, #topic_trie_node.binding_count, -1),
    trie_binding_op(X, Node, D, Args, fun mnesia:delete_object/3).

trie_binding_op(X, Node, D, Args, Op) ->
    ok = Op(rabbit_topic_trie_binding,
            #topic_trie_binding{
              trie_binding = #trie_binding{exchange_name = X,
                                           node_id       = Node,
                                           destination   = D,
                                           arguments     = Args}},
            write).

ensure_topic_deletion_ets() ->
    Tab = rabbit_db_topic_exchange_delete_table,
    case ets:whereis(Tab) of
        undefined ->
            ets:new(Tab, [public, named_table, {keypos, #topic_trie_edge.trie_edge}]);
        Tid ->
            Tid
    end.

ensure_topic_migration_ets() ->
    Tab = rabbit_db_topic_exchange_write_table,
    case ets:whereis(Tab) of
        undefined ->
            ets:new(Tab, [public, named_table]);
        Tid ->
            Tid
    end.

khepri_exchange_type_topic_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, topic_trie_binding, VHost, Name].

khepri_exchange_type_topic_path() ->
    [?MODULE, topic_trie_binding].

