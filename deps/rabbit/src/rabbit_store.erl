%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_store).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("amqqueue.hrl").

-export([exists_binding/1, add_binding/2, delete_binding/2, list_bindings/1,
         list_bindings_for_source/1, list_bindings_for_destination/1,
         list_bindings_for_source_and_destination/2, list_explicit_bindings/0,
         recover_bindings/0, recover_bindings/1,
         index_route_table_definition/0, populate_index_route_table/0,
         fold_bindings/2]).

%% Routing. These functions are in the hot code path
-export([match_bindings/2, match_routing_key/3]).

-export([update_policies/3]).

-export([init/0, sync/0]).
-export([set_migration_flag/1, is_migration_done/1]).

%% Exported to be used by various rabbit_db_* modules
-export([
         list_in_mnesia/2,
         list_in_khepri/1,
         list_in_khepri/2,
         remove_bindings_for_destination_in_mnesia/2,
         remove_bindings_for_destination_in_khepri/2,
         remove_bindings_in_mnesia/3,
         remove_bindings_in_khepri/3,
         remove_transient_bindings_for_destination_in_mnesia/1,
         retry/1,
         if_has_data/1,
         if_has_data_wildcard/0,
         match_source_and_destination_in_khepri_tx/2,
         binding_has_for_source_in_mnesia/1,
         binding_has_for_source_in_khepri/1
        ]).

-export([mnesia_write_to_khepri/2,
         mnesia_delete_to_khepri/2,
         clear_data_in_khepri/1]).

-define(WAIT_SECONDS, 30).

%% Clustering used on the boot steps

init() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              recover_mnesia_tables(),
              rabbit_mnesia:init()
      end,
      fun() ->
              khepri_init()
      end).

khepri_init() ->
    case rabbit_khepri:members() of
        [] ->
            timer:sleep(1000),
            khepri_init();
        Members ->
            rabbit_log:warning("Found the following metadata store members: ~p", [Members])
    end.

sync() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_sup:start_child(mnesia_sync)
      end,
      fun() ->
              ok
      end).

set_migration_flag(FeatureName) ->
    rabbit_khepri:put([?MODULE, migration_done, FeatureName], true).

is_migration_done(FeatureName) ->
    case rabbit_khepri:get([?MODULE, migration_done, FeatureName]) of
        {ok, Flag} ->
            Flag;
        _ ->
            false
    end.

%% Paths
%% --------------------------------------------------------------
%% Bindings

khepri_route_path(#binding{source = #resource{virtual_host = VHost, name = SrcName},
                           destination = #resource{kind = Kind, name = DstName},
                           key = RoutingKey}) ->
    [?MODULE, routes, VHost, SrcName, Kind, DstName, RoutingKey].

khepri_routes_path() ->
    [?MODULE, routes].

%% Routing optimisation, probably the most relevant on the hot code path.
%% It only needs to store a list of destinations to be used by rabbit_router.
%% Unless there is a high queue churn, this should barely change. Thus, the small
%% penalty for updates should be worth it.
khepri_routing_path() ->
    [?MODULE, routing].

khepri_routing_path(#binding{source = Src, key = RoutingKey}) ->
    khepri_routing_path(Src, RoutingKey);
khepri_routing_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, routing, VHost, Name].

khepri_routing_path(#resource{virtual_host = VHost, name = Name}, RoutingKey) ->
    [?MODULE, routing, VHost, Name, RoutingKey].

%% API
%% --------------------------------------------------------------
%% Bindings

exists_binding(#binding{source = SrcName,
                        destination = DstName} = Binding) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              binding_action_in_mnesia(
                Binding, fun (_Src, _Dst) ->
                                 rabbit_misc:const(mnesia:read({rabbit_route, Binding}) /= [])
                         end, fun not_found_or_absent_errs_in_mnesia/1)
      end,
      fun() ->
              Path = khepri_route_path(Binding),
              case rabbit_khepri:transaction(
                     fun () ->
                             case {lookup_resource_in_khepri_tx(SrcName),
                                   lookup_resource_in_khepri_tx(DstName)} of
                                 {[_Src], [_Dst]} ->
                                     case khepri_tx:get(Path) of
                                         {ok, Set} ->
                                             {ok, Set};
                                         _ ->
                                             {ok, not_found}
                                     end;
                                 Errs ->
                                     Errs
                             end
                     end) of
                  {ok, not_found} -> false;
                  {ok, Set} -> sets:is_element(Binding, Set);
                  Errs -> not_found_errs_in_khepri(not_found(Errs, SrcName, DstName))
              end
      end).

add_binding(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> add_binding_in_mnesia(Binding, ChecksFun) end,
      fun() -> add_binding_in_khepri(Binding, ChecksFun) end).

delete_binding(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> remove_binding_in_mnesia(Binding, ChecksFun) end,
      fun() -> remove_binding_in_khepri(Binding, ChecksFun) end).

list_bindings(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              VHostResource = rabbit_misc:r(VHost, '_'),
              Match = #route{binding = #binding{source      = VHostResource,
                                                destination = VHostResource,
                                                _           = '_'},
                             _       = '_'},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Match)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

fold_bindings(Fun, Acc) ->
    %% Used by prometheus_rabbitmq_core_metrics_collector to iterate over the bindings.
    %% It used to query `rabbit_route` directly, which isn't valid when using Khepri
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ets:foldl(fun(#route{binding = Binding}, Acc0) ->
                                Fun(Binding, Acc0)
                        end, Acc, rabbit_route)
      end,
      fun() ->
              Path = khepri_routes_path() ++ [?KHEPRI_WILDCARD_STAR, if_has_data_wildcard()],
              {ok, Res} = rabbit_khepri:fold(
                            Path,
                            fun(_, #{data := SetOfBindings}, Acc0) ->
                                    lists:foldl(fun(Binding, Acc1) ->
                                                        Fun(Binding, Acc1)
                                                end, Acc0, sets:to_list(SetOfBindings))
                            end, Acc),
              Res
      end).

list_bindings_for_source(#resource{virtual_host = VHost, name = Name} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source = Resource, _ = '_'}},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, Name, if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

list_bindings_for_destination(#resource{virtual_host = VHost, name = Name,
                                        kind = Kind} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = rabbit_binding:reverse_route(#route{binding = #binding{destination = Resource,
                                                                             _ = '_'}}),
              [rabbit_binding:reverse_binding(B) ||
                  #reverse_route{reverse_binding = B} <- list_in_mnesia(rabbit_reverse_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, ?KHEPRI_WILDCARD_STAR, Kind, Name, if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

list_bindings_for_source_and_destination(SrcName, DstName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source      = SrcName,
                                                destination = DstName,
                                                _           = '_'}},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Values = match_source_and_destination_in_khepri(SrcName, DstName),
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], Values)
      end).

list_explicit_bindings() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:async_dirty(
                fun () ->
                        AllRoutes = mnesia:dirty_match_object(rabbit_route, #route{_ = '_'}),
                        [B || #route{binding = B} <- AllRoutes]
                end)
      end,
      fun() ->
              Condition = #if_not{condition = #if_name_matches{regex = "^$"}},
              Path = khepri_routes_path() ++ [?KHEPRI_WILDCARD_STAR, Condition, if_has_data_wildcard()],
              {ok, Data} = rabbit_khepri:match(Path),
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], maps:values(Data))
      end).

recover_bindings() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> recover_bindings_in_mnesia() end,
      %% Nothing to do in khepri, single table storage
      fun() -> ok end).

recover_bindings(RecoverFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              [RecoverFun(Route, Src, Dst, fun recover_semi_durable_route_txn/3, mnesia) ||
                  #route{binding = #binding{destination = Dst,
                                            source = Src}} = Route <-
                      rabbit_misc:dirty_read_all(rabbit_semi_durable_route)]
      end,
      fun() ->
              ok
      end).

%% Routing - HOT CODE PATH

match_bindings(SrcName, Match) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              MatchHead = #route{binding = #binding{source      = SrcName,
                                                    _           = '_'}},
              Routes = ets:select(rabbit_route, [{MatchHead, [], [['$_']]}]),
              [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
                           Routes, Match(Binding)]
      end,
      fun() ->
              Data = match_source_in_khepri(SrcName),
              Bindings = lists:foldl(fun(SetOfBindings, Acc) ->
                                             sets:to_list(SetOfBindings) ++ Acc
                                     end, [], maps:values(Data)),
              [Dest || Binding = #binding{destination = Dest} <- Bindings, Match(Binding)]
      end).

match_routing_key(SrcName, RoutingKeys, UseIndex) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              case UseIndex of
                  true ->
                      route_in_mnesia_v2(SrcName, RoutingKeys);
                  _ ->
                      route_in_mnesia_v1(SrcName, RoutingKeys)
              end
      end,
      fun() ->
              match_source_and_key_in_khepri(SrcName, RoutingKeys)
      end).

route_in_mnesia_v1(SrcName, [RoutingKey]) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = RoutingKey,
                                          _           = '_'}},
    ets:select(rabbit_route, [{MatchHead, [], ['$1']}]);
route_in_mnesia_v1(SrcName, [_|_] = RoutingKeys) ->
    %% Normally we'd call mnesia:dirty_select/2 here, but that is quite
    %% expensive for the same reasons as above, and, additionally, due to
    %% mnesia 'fixing' the table with ets:safe_fixtable/2, which is wholly
    %% unnecessary. According to the ets docs (and the code in erl_db.c),
    %% 'select' is safe anyway ("Functions that internally traverse over a
    %% table, like select and match, will give the same guarantee as
    %% safe_fixtable.") and, furthermore, even the lower level iterators
    %% ('first' and 'next') are safe on ordered_set tables ("Note that for
    %% tables of the ordered_set type, safe_fixtable/2 is not necessary as
    %% calls to first/1 and next/2 will always succeed."), which
    %% rabbit_route is.
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = '$2',
                                          _           = '_'}},
    Conditions = [list_to_tuple(['orelse' | [{'=:=', '$2', RKey} ||
                                                RKey <- RoutingKeys]])],
    ets:select(rabbit_route, [{MatchHead, Conditions, ['$1']}]).

%% rabbit_router:match_routing_key/2 uses ets:select/2 to get destinations.
%% ets:select/2 is expensive because it needs to compile the match spec every
%% time and lookup does not happen by a hash key.
%%
%% In contrast, route_v2/2 increases end-to-end message sending throughput
%% (i.e. from RabbitMQ client to the queue process) by up to 35% by using ets:lookup_element/3.
%% Only the direct exchange type uses the rabbit_index_route table to store its
%% bindings by table key tuple {SourceExchange, RoutingKey}.
-spec route_in_mnesia_v2(rabbit_types:binding_source(), [rabbit_router:routing_key(), ...]) ->
    rabbit_router:match_result().
route_in_mnesia_v2(SrcName, [RoutingKey]) ->
    %% optimization
    destinations(SrcName, RoutingKey);
route_in_mnesia_v2(SrcName, [_|_] = RoutingKeys) ->
    lists:flatmap(fun(Key) ->
                          destinations(SrcName, Key)
                  end, RoutingKeys).

destinations(SrcName, RoutingKey) ->
    %% Prefer try-catch block over checking Key existence with ets:member/2.
    %% The latter reduces throughput by a few thousand messages per second because
    %% of function db_member_hash in file erl_db_hash.c.
    %% We optimise for the happy path, that is the binding / table key is present.
    try
        ets:lookup_element(rabbit_index_route,
                           {SrcName, RoutingKey},
                           #index_route.destination)
    catch
        error:badarg ->
            []
    end.


%% Policies
%% --------------------------------------------------------------

update_policies(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              update_policies_in_mnesia(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun)
      end,
      fun() ->
              update_policies_in_khepri(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun)
      end).


%% [1] We need to prevent this from becoming O(n^2) in a similar
%% manner to rabbit_binding:remove_for_{source,destination}. So see
%% the comment in rabbit_binding:lock_route_tables/0 for more rationale.
update_policies_in_mnesia(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    Tabs = [rabbit_queue,    rabbit_durable_queue,
            rabbit_exchange, rabbit_durable_exchange],
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              [mnesia:lock({table, T}, write) || T <- Tabs], %% [1]
              Exchanges0 = rabbit_db_exchange:list_exchanges(VHost),
              Queues0 = rabbit_db_queue:get_all(VHost),
              Exchanges = [GetUpdatedExchangeFun(X) || X <- Exchanges0],
              Queues = [GetUpdatedQueueFun(Q) || Q <- Queues0],
              {[update_exchange_policies(Map, fun rabbit_db_exchange:update_in_mnesia/2)
                || Map <- Exchanges, is_map(Map)],
               [update_queue_policies(Map, fun rabbit_db_queue:update_in_mnesia/2)
                || Map <- Queues, is_map(Map)]}
      end).

update_policies_in_khepri(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    Exchanges0 = rabbit_db_exchange:list_exchanges(VHost),
    Queues0 = rabbit_db_queue:get_all(VHost),
    Exchanges = [GetUpdatedExchangeFun(X) || X <- Exchanges0],
    Queues = [GetUpdatedQueueFun(Q) || Q <- Queues0],
    rabbit_khepri:transaction(
      fun() ->
              {[update_exchange_policies(Map, fun rabbit_db_exchange:update_in_khepri/2)
                || Map <- Exchanges, is_map(Map)],
               [update_queue_policies(Map, fun rabbit_db_queue:update_in_khepri/2)
                || Map <- Queues, is_map(Map)]}
      end, rw).

update_exchange_policies(#{exchange := X = #exchange{name = XName},
                           update_function := UpdateFun}, StoreFun) ->
    NewExchange = StoreFun(XName, UpdateFun),
    case NewExchange of
        #exchange{} = X1 -> {X, X1};
        not_found        -> {X, X }
    end.

update_queue_policies(#{queue := Q0, update_function := UpdateFun}, StoreFun) ->
    QName = amqqueue:get_name(Q0),
    NewQueue = StoreFun(QName, UpdateFun),
    case NewQueue of
        Q1 when ?is_amqqueue(Q1) ->
            {Q0, Q1};
        not_found ->
            {Q0, Q0}
    end.

%% Feature flags
%% --------------------------------------------------------------
mnesia_write_to_khepri(rabbit_route, Routes)->
    rabbit_khepri:transaction(
      fun() ->
              [begin
                   #route{binding = Binding} = Route,
                   Path = khepri_route_path(Binding),
                   add_binding_tx(Path, Binding)
               end || Route <- Routes]
      end, rw);
mnesia_write_to_khepri(rabbit_durable_route, _)->
    ok;
mnesia_write_to_khepri(rabbit_semi_durable_route, _)->
    ok;
mnesia_write_to_khepri(rabbit_reverse_route, _) ->
    ok.

mnesia_delete_to_khepri(rabbit_route, Route) when is_record(Route, route) ->
    khepri_delete(khepri_route_path(Route#route.binding));
mnesia_delete_to_khepri(rabbit_route, Name) ->
    khepri_delete(khepri_route_path(Name));
mnesia_delete_to_khepri(rabbit_durable_route, Route) when is_record(Route, route) ->
    khepri_delete(khepri_route_path(Route#route.binding));
mnesia_delete_to_khepri(rabbit_durable_route, Name) ->
    khepri_delete(khepri_route_path(Name));
mnesia_delete_to_khepri(rabbit_semi_durable_route, Route) when is_record(Route, route) ->
    khepri_delete(khepri_route_path(Route#route.binding));
mnesia_delete_to_khepri(rabbit_semi_durable_route, Name) ->
    khepri_delete(khepri_route_path(Name)).

clear_data_in_khepri(rabbit_route) ->
    khepri_delete(khepri_routes_path()),
    khepri_delete(khepri_routing_path());
%% There is a single khepri entry for routes and it should be already deleted
clear_data_in_khepri(rabbit_durable_route) ->
    ok;
clear_data_in_khepri(rabbit_semi_durable_route) ->
    ok;
clear_data_in_khepri(rabbit_reverse_route) ->
    ok.

%% Internal
%% -------------------------------------------------------------
list_in_mnesia(Table, Match) ->
    %% Not dirty_match_object since that would not be transactional when used in a
    %% tx context
    mnesia:async_dirty(fun () -> mnesia:match_object(Table, Match, read) end).

list_in_khepri(Path) ->
    list_in_khepri(Path, #{}).

list_in_khepri(Path, Options) ->
    case rabbit_khepri:match(Path, Options) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

list_in_khepri_tx(Path) ->
    case khepri_tx:get_many(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

remove_bindings_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> remove_bindings_for_source_in_mnesia(XName);
                   false -> []
               end,
    {deleted, X, Bindings, remove_bindings_for_destination_in_mnesia(XName, OnlyDurable, fun remove_routes/1)}.

remove_bindings_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> remove_bindings_for_source_in_khepri(XName);
                   false -> []
               end,
    {deleted, X, Bindings, remove_bindings_for_destination_in_khepri(XName, OnlyDurable)}.

if_has_data_wildcard() ->
    if_has_data([?KHEPRI_WILDCARD_STAR_STAR]).

if_has_data(Conditions) ->
    #if_all{conditions = Conditions ++ [#if_has_data{has_data = true}]}.

binding_action_in_mnesia(#binding{source      = SrcName,
                                  destination = DstName}, Fun, ErrFun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:read({SrcTable, SrcName}),
                    mnesia:read({DstTable, DstName})} of
                  {[Src], [Dst]} -> Fun(Src, Dst);
                  {[],    [_]  } -> ErrFun([SrcName]);
                  {[_],   []   } -> ErrFun([DstName]);
                  {[],    []   } -> ErrFun([SrcName, DstName])
              end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

lookup_resource(#resource{kind = queue} = Name) ->
    case rabbit_db_queue:get(Name) of
        {error, _} -> [];
        {ok, Q} -> [Q]
    end;
lookup_resource(#resource{kind = exchange} = Name) ->
    case rabbit_db_exchange:get(Name) of
        {ok, X} -> [X];
        _ -> []
    end.

lookup_resource_in_khepri_tx(#resource{kind = queue} = Name) ->
    rabbit_db_queue:get_in_khepri_tx(Name);
lookup_resource_in_khepri_tx(#resource{kind = exchange} = Name) ->
    rabbit_db_exchange:get_in_khepri_tx(Name).

exists_binding_in_khepri(Path, Binding) ->
    case khepri_tx:get(Path) of
        {ok, Set} ->
            sets:is_element(Binding, Set);
        _ ->
            false
    end.

not_found({[], [_]}, SrcName, _) ->
    [SrcName];
not_found({[_], []}, _, DstName) ->
    [DstName];
not_found({[], []}, SrcName, DstName) ->
    [SrcName, DstName].

not_found_or_absent_errs_in_mnesia(Names) ->
    Errs = [not_found_or_absent_in_mnesia(Name) || Name <- Names],
    rabbit_misc:const({error, {resources_missing, Errs}}).

not_found_errs_in_khepri(Names) ->
    Errs = [{not_found, Name} || Name <- Names],
    {error, {resources_missing, Errs}}.

absent_errs_only_in_mnesia(Names) ->
    Errs = [E || Name <- Names,
                 {absent, _Q, _Reason} = E <- [not_found_or_absent_in_mnesia(Name)]],
    rabbit_misc:const(case Errs of
                          [] -> ok;
                          _  -> {error, {resources_missing, Errs}}
                      end).

not_found_or_absent_in_mnesia(#resource{kind = exchange} = Name) ->
    {not_found, Name};
not_found_or_absent_in_mnesia(#resource{kind = queue}    = Name) ->
    case rabbit_db_queue:not_found_or_absent_queue_in_mnesia(Name) of
        not_found                 -> {not_found, Name};
        {absent, _Q, _Reason} = R -> R
    end.

serial_in_mnesia(false, _) ->
    none;
serial_in_mnesia(true, X) ->
    rabbit_db_exchange:next_serial_in_mnesia(X).

serial_in_khepri(false, _) ->
    none;
serial_in_khepri(true, X) ->
    rabbit_db_exchange:next_serial_in_khepri(X).

bindings_data(Path) ->
    case khepri_tx:get(Path) of
        {ok, Set} ->
            Set;
        _ ->
            sets:new()
    end.

add_binding_tx(Path, Binding) ->
    Set = bindings_data(Path),
    ok = khepri_tx:put(Path, sets:add_element(Binding, Set)),
    add_routing(Binding),
    ok.

add_routing(#binding{destination = Dst, source = Src} = Binding) ->
    Path = khepri_routing_path(Binding),
    KeepWhile = #{rabbit_db_exchange:path(Src) => #if_node_exists{exists = true}},
    Options = #{keep_while => KeepWhile},
    case khepri_tx:get(Path) of
        {ok, Data} ->
            ok = khepri_tx:put(Path, sets:add_element(Dst, Data), Options);
        _ ->
            ok = khepri_tx:put(Path, sets:add_element(Dst, sets:new()), Options)
    end.

add_binding_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              case ChecksFun(Src, Dst) of
                  {ok, BindingType} ->
                      case mnesia:read({rabbit_route, Binding}) of
                          []  ->
                              ok = sync_route(#route{binding = Binding}, BindingType,
                                              should_index_table(Src), fun mnesia:write/3),
                              MaybeSerial = rabbit_exchange:serialise_events(Src),
                              Serial = serial_in_mnesia(MaybeSerial, Src),
                              fun () ->
                                      rabbit_exchange:callback(Src, add_binding, Serial, [Src, Binding])
                              end;
                          [_] -> fun () -> ok end
                      end;
                  {error, _} = Err ->
                      rabbit_misc:const(Err)
              end
      end, fun not_found_or_absent_errs_in_mnesia/1).

add_binding_in_khepri(#binding{source = SrcName,
                               destination = DstName} = Binding, ChecksFun) ->
    case {lookup_resource(SrcName), lookup_resource(DstName)} of
        {[Src], [Dst]} ->
            case ChecksFun(Src, Dst) of
                {ok, _BindingType} ->
                    Path = khepri_route_path(Binding),
                    MaybeSerial = rabbit_exchange:serialise_events(Src),
                    Serial = rabbit_khepri:transaction(
                               fun() ->
                                       case khepri_tx:get(Path) of
                                           {ok, Set} ->
                                               case sets:is_element(Binding, Set) of
                                                   true ->
                                                       already_exists;
                                                   false ->
                                                       ok = khepri_tx:put(Path, sets:add_element(Binding, Set)),
                                                       add_routing(Binding),
                                                       serial_in_khepri(MaybeSerial, Src)
                                               end;
                                           _ ->
                                               ok = khepri_tx:put(Path, sets:add_element(Binding, sets:new())),
                                               add_routing(Binding),
                                               serial_in_khepri(MaybeSerial, Src)
                                       end
                               end, rw),
                    case Serial of
                        already_exists -> ok;
                        _ ->
                            rabbit_exchange:callback(Src, add_binding, Serial, [Src, Binding])
                    end,
                    ok;
                {error, _} = Err ->
                    Err
            end;
        Errs ->
            not_found_errs_in_khepri(not_found(Errs, SrcName, DstName))
    end.

remove_binding_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              lock_resource(Src, read),
              lock_resource(Dst, read),
              case mnesia:read(rabbit_route, Binding, write) of
                  [] -> case mnesia:read(rabbit_durable_route, Binding, write) of
                            [] -> rabbit_misc:const(ok);
                            %% We still delete the binding and run
                            %% all post-delete functions if there is only
                            %% a durable route in the database
                            _  -> remove_binding_in_mnesia(Src, Dst, Binding)
                        end;
                  _  -> case ChecksFun(Dst) of
                            ok               -> remove_binding_in_mnesia(Src, Dst, Binding);
                            {error, _} = Err -> rabbit_misc:const(Err)
                        end
              end
      end, fun absent_errs_only_in_mnesia/1).

remove_binding_in_mnesia(Src, Dst, B) ->
    ok = sync_route(#route{binding = B}, rabbit_binding:binding_type(Src, Dst),
                    should_index_table(Src), fun delete/3),
    Deletions0 = maybe_auto_delete_exchange_in_mnesia(
                   B#binding.source, [B], rabbit_binding:new_deletions(), false),
    fun() -> rabbit_binding:process_deletions(Deletions0) end.

sync_route(Route, durable, ShouldIndexTable, Fun) ->
    ok = Fun(rabbit_durable_route, Route, write),
    sync_route(Route, semi_durable, ShouldIndexTable, Fun);

sync_route(Route, semi_durable, ShouldIndexTable, Fun) ->
    ok = Fun(rabbit_semi_durable_route, Route, write),
    sync_route(Route, transient, ShouldIndexTable, Fun);

sync_route(Route, transient, ShouldIndexTable, Fun) ->
    sync_transient_route(Route, ShouldIndexTable, Fun).

sync_transient_route(Route, ShouldIndexTable, Fun) ->
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, rabbit_binding:reverse_route(Route), write),
    sync_index_route(Route, ShouldIndexTable, Fun).

sync_index_route(Route, true, Fun) ->
    %% Do not block as blocking will cause a dead lock when
    %% function rabbit_binding:populate_index_route_table/0
    %% (i.e. feature flag migration) runs in parallel.
    case rabbit_feature_flags:is_enabled(direct_exchange_routing_v2, non_blocking) of
        true ->
            ok = Fun(rabbit_index_route, rabbit_binding:index_route(Route), write);
       false ->
            ok;
        state_changing ->
            case rabbit_table:exists(rabbit_index_route) of
                true ->
                    ok = Fun(rabbit_index_route, rabbit_binding:index_route(Route), write);
                false ->
                    ok
            end
    end;
sync_index_route(_, _, _) ->
    ok.

remove_transient_routes(Routes) ->
    lists:map(fun(#route{binding = #binding{source = Src} = Binding} = Route) ->
                      {ok, X} = rabbit_db_exchange:get(Src),
                      ok = sync_transient_route(Route, should_index_table(X), fun delete/3),
                      Binding
              end, Routes).

remove_routes(Routes) ->
    remove_routes(Routes, undefined).

remove_routes(Routes, ShouldIndexTable) ->
    %% This partitioning allows us to suppress unnecessary delete
    %% operations on disk tables, which require an fsync.
    {RamRoutes, DiskRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     rabbit_durable_route, R#route.binding, read) == [] end,
                        Routes),
    {RamOnlyRoutes, SemiDurableRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     rabbit_semi_durable_route, R#route.binding, read) == [] end,
                        RamRoutes),
    %% Of course the destination might not really be durable but it's
    %% just as easy to try to delete it from the semi-durable table
    %% than check first
    [ok = sync_route(R, durable, ShouldIndexTable, fun delete/3) ||
        R <- DiskRoutes],
    [ok = sync_route(R, semi_durable, ShouldIndexTable, fun delete/3) ||
        R <- SemiDurableRoutes],
    [ok = sync_route(R, transient, ShouldIndexTable, fun delete/3) ||
        R <- RamOnlyRoutes],
    case ShouldIndexTable of
        B when is_boolean(B) ->
            ok;
        undefined ->
            [begin
                 case rabbit_db_exchange:get(Src) of
                     {ok, X} ->
                         ok = sync_index_route(R, should_index_table(X), fun delete/3);
                     _ ->
                         ok
                 end
             end || #route{binding = #binding{source = Src}} = R <- Routes]
    end,
    [R#route.binding || R <- Routes].

delete(Tab, #route{binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #reverse_route{reverse_binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #index_route{} = Record, LockKind) ->
    mnesia:delete_object(Tab, Record, LockKind).

remove_binding_in_khepri(#binding{source = SrcName,
                                  destination = DstName} = Binding, ChecksFun) ->
    Path = khepri_route_path(Binding),
    case rabbit_khepri:transaction(
           fun () ->
                   case {lookup_resource_in_khepri_tx(SrcName),
                         lookup_resource_in_khepri_tx(DstName)} of
                       {[_Src], [Dst]} ->
                           case exists_binding_in_khepri(Path, Binding) of
                               false ->
                                   ok;
                               true ->
                                   case ChecksFun(Dst) of
                                       ok ->
                                           ok = delete_binding_in_khepri(Binding),
                                           ok = delete_routing(Binding),
                                           maybe_auto_delete_exchange_in_khepri(Binding#binding.source, [Binding], rabbit_binding:new_deletions(), false);
                                       {error, _} = Err ->
                                           Err
                                   end
                           end;
                       _Errs ->
                           %% No absent queues, always present on disk
                           ok
                   end
           end) of
        ok ->
            ok;
        {error, _} = Err ->
            Err;
        Deletions ->
            rabbit_binding:process_deletions(Deletions)
    end.

-spec index_route_table_definition() -> list(tuple()).
index_route_table_definition() ->
    maps:to_list(
      #{
        record_name => index_route,
        attributes  => record_info(fields, index_route),
        type => bag,
        ram_copies => rabbit_nodes:all(),
        storage_properties => [{ets, [{read_concurrency, true}]}]
       }).

populate_index_route_table() ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              mnesia:lock({table, rabbit_route}, read),
              mnesia:lock({table, rabbit_index_route}, write),
              Routes = rabbit_misc:dirty_read_all(rabbit_route),
              lists:foreach(fun(#route{binding = #binding{source = Exchange}} = Route) ->
                                    case rabbit_db_exchange:get(Exchange) of
                                        {ok, X} ->
                                            case should_index_table(X) of
                                                true ->
                                                    mnesia:dirty_write(rabbit_index_route,
                                                                       rabbit_binding:index_route(Route));
                                                false ->
                                                    ok
                                            end;
                                        _ ->
                                            ok
                                    end
                            end, Routes)
      end).

%% Only the direct exchange type uses the rabbit_index_route table to store its
%% bindings by table key tuple {SourceExchange, RoutingKey}.
%% Other built-in exchange types lookup destinations by SourceExchange, and
%% therefore will not need to read from the rabbit_index_route index table.
%% Therefore, we avoid inserting and deleting into rabbit_index_route for other exchange
%% types. This reduces write lock conflicts on the same tuple {SourceExchange, RoutingKey}
%% reducing the number of restarted Mnesia transactions.
should_index_table(#exchange{name = #resource{name = Name},
                      type = direct})
  when Name =/= <<>> ->
    true;
should_index_table(_) ->
    false.

delete_binding_in_khepri(Binding) ->
    Path = khepri_route_path(Binding),
    case khepri_tx:get(Path) of
        {ok, Set0} ->
            Set = sets:del_element(Binding, Set0),
            case sets:is_empty(Set) of
                true ->
                    ok = khepri_tx:delete(Path);
                false ->
                    ok = khepri_tx:put(Path, Set)
            end;
        _ ->
            ok
    end.

delete_routing(#binding{destination = Dst} = Binding) ->
    Path = khepri_routing_path(Binding),
    case khepri_tx:get(Path) of
        {ok, Data0} ->
            Data = sets:del_element(Dst, Data0),
            case sets:is_empty(Data) of
                true ->
                    ok = khepri_tx:delete(Path);
                false ->
                    ok = khepri_tx:put(Path, Data)
            end;
        _ ->
            ok
    end.

%% Instead of locking entire table on remove operations we can lock the
%% affected resource only.
lock_resource(Name) -> lock_resource(Name, write).

lock_resource(Name, LockKind) ->
    mnesia:lock({global, Name, mnesia:table_info(rabbit_route, where_to_write)},
                LockKind).

-spec binding_has_for_source_in_mnesia(rabbit_types:binding_source()) -> boolean().

binding_has_for_source_in_mnesia(SrcName) ->
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    %% we need to check for semi-durable routes (which subsumes
    %% durable routes) here too in case a bunch of routes to durable
    %% queues have been removed temporarily as a result of a node
    %% failure
    contains(rabbit_route, Match) orelse
        contains(rabbit_semi_durable_route, Match).

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

binding_has_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, if_has_data_wildcard()],
    case khepri_tx:get_many(Path) of
        {ok, Map} ->
            maps:size(Map) > 0;
        Error ->
            Error
    end.

match_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, if_has_data_wildcard()],
    {ok, Map} = rabbit_khepri:match(Path),
    Map.

match_source_and_key_in_khepri(Src, ['_']) ->
    Path = khepri_routing_path(Src, if_has_data_wildcard()),
    case rabbit_khepri:match(Path) of
        {ok, Map} ->
            maps:fold(fun(_, Dsts, Acc) ->
                              sets:to_list(Dsts) ++ Acc
                      end, [], Map);
        {error, {khepri, node_not_found, _}} ->
            []
    end;
match_source_and_key_in_khepri(Src, RoutingKeys) ->
    lists:foldl(
      fun(RK, Acc) ->
              Path = khepri_routing_path(Src, RK),
              %% Don't use transaction if we want to hit the cache
              case rabbit_khepri:get(Path) of
                  {ok, Dsts} ->
                      sets:to_list(Dsts) ++ Acc;
                  {error, {khepri, node_not_found, _}} ->
                      Acc
              end
      end, [], RoutingKeys).

match_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, ?KHEPRI_WILDCARD_STAR, Kind, Name, ?KHEPRI_WILDCARD_STAR_STAR],
    {ok, Map} = khepri_tx:get_many(Path),
    Map.

match_source_and_destination_in_khepri(#resource{virtual_host = VHost, name = Name},
                                       #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName, if_has_data_wildcard()],
    list_in_khepri(Path).

match_source_and_destination_in_khepri_tx(#resource{virtual_host = VHost, name = Name},
                                       #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName, if_has_data_wildcard()],
    list_in_khepri_tx(Path).

remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable, Fun) ->
    lock_resource(DstName),
    MatchFwd = #route{binding = #binding{destination = DstName, _ = '_'}},
    MatchRev = rabbit_binding:reverse_route(MatchFwd),
    Routes = case OnlyDurable of
                 false ->
                        [rabbit_binding:reverse_route(R) ||
                              R <- mnesia:dirty_match_object(
                                     rabbit_reverse_route, MatchRev)];
                 true  -> lists:usort(
                            mnesia:dirty_match_object(
                              rabbit_durable_route, MatchFwd) ++
                                mnesia:dirty_match_object(
                                  rabbit_semi_durable_route, MatchFwd))
             end,
    Bindings = Fun(Routes),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_mnesia/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable) ->
    remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable, fun remove_routes/1).

remove_bindings_for_destination_in_khepri(DstName, OnlyDurable) ->
    BindingsMap = match_destination_in_khepri(DstName),
    maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, BindingsMap),
    Bindings = maps:fold(fun(_, Set, Acc) ->
                                 sets:to_list(Set) ++ Acc
                         end, [], BindingsMap),
    lists:foreach(fun(Binding) -> delete_routing(Binding) end, Bindings),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

remove_bindings_for_source_in_mnesia(#exchange{name = SrcName} = SrcX) ->
    remove_bindings_for_source_in_mnesia(SrcName, should_index_table(SrcX));
remove_bindings_for_source_in_mnesia(SrcName) ->
    remove_bindings_for_source_in_mnesia(SrcName, undefined).

-spec remove_bindings_for_source_in_mnesia(rabbit_types:binding_source(),
                                           boolean() | undefined) -> [rabbit_types:binding()].
remove_bindings_for_source_in_mnesia(SrcName, ShouldIndexTable) ->
    lock_resource(SrcName),
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    remove_routes(
      lists:usort(
        mnesia:dirty_match_object(rabbit_route, Match) ++
            mnesia:dirty_match_object(rabbit_semi_durable_route, Match)),
      ShouldIndexTable).

remove_bindings_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name],
    {ok, Bindings} = khepri_tx:get_many(Path ++ [if_has_data_wildcard()]),
    ok = khepri_tx:delete(Path),
    maps:fold(fun(_, Set, Acc) ->
                      sets:to_list(Set) ++ Acc
              end, [], Bindings).

maybe_auto_delete_exchange_in_mnesia(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case rabbit_db_exchange:maybe_auto_delete_in_mnesia(XName, OnlyDurable) of
            {not_deleted, X} ->
                {{X, not_deleted, Bindings}, Deletions};
            {deleted, X, Deletions2} ->
                {{X, deleted, Bindings},
                 rabbit_binding:combine_deletions(Deletions, Deletions2)}
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

maybe_auto_delete_exchange_in_khepri(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case rabbit_db_exchange:maybe_auto_delete_in_khepri(XName, OnlyDurable) of
            {not_deleted, X} ->
                {{X, not_deleted, Bindings}, Deletions};
            {deleted, X, Deletions2} ->
                {{X, deleted, Bindings},
                 rabbit_binding:combine_deletions(Deletions, Deletions2)}
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

-spec remove_transient_bindings_for_destination_in_mnesia(rabbit_types:binding_destination()) -> rabbit_binding:deletions().
remove_transient_bindings_for_destination_in_mnesia(DstName) ->
    remove_bindings_for_destination_in_mnesia(DstName, false, fun remove_transient_routes/1).

recover_bindings_in_mnesia() ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              mnesia:lock({table, rabbit_durable_route}, read),
              mnesia:lock({table, rabbit_semi_durable_route}, write),
              Routes = rabbit_misc:dirty_read_all(rabbit_durable_route),
              Fun = fun(Route) ->
                            mnesia:dirty_write(rabbit_semi_durable_route, Route)
                    end,
              lists:foreach(Fun, Routes),
              ok
    end).

recover_semi_durable_route_txn(#route{binding = B} = Route, X, mnesia) ->
    MaybeSerial = rabbit_exchange:serialise_events(X),
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(rabbit_semi_durable_route, B, read) of
                  [] -> no_recover;
                  _  -> ok = sync_transient_route(Route, should_index_table(X), fun mnesia:write/3),
                        serial_in_mnesia(MaybeSerial, X)
              end
      end,
      fun (no_recover, _) -> ok;
          (_Serial, true) -> ok;
          (Serial, false) -> rabbit_exchange:callback(X, add_binding, Serial, [X, B])
      end);
recover_semi_durable_route_txn(_Path, _X, khepri) ->
    ok.

retry(Fun) ->
    Until = erlang:system_time(millisecond) + (?WAIT_SECONDS * 1000),
    retry(Fun, Until).

retry(Fun, Until) ->
    case Fun() of
        {error, Reason} ->
            case erlang:system_time(millisecond) of
                V when V >= Until ->
                    throw({error, Reason});
                _ ->
                    retry(Fun, Until)
            end;
        Reply ->
            Reply
    end.

recover_mnesia_tables() ->
    %% A failed migration can leave tables in read-only mode before enabling
    %% the feature flag. See rabbit_core_ff:final_sync_from_mnesia_to_khepri/2
    %% Unlock them here as mnesia is still fully functional.
    Tables = rabbit_channel_tracking:get_all_tracked_channel_table_names_for_node(node())
        ++ rabbit_connection_tracking:get_all_tracked_connection_table_names_for_node(node())
        ++ [Table || {Table, _} <- rabbit_table:definitions()],
    [mnesia:change_table_access_mode(Table, read_write) || Table <- Tables],
    ok.

khepri_delete(Path) ->
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.
