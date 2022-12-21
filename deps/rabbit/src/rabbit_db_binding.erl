%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_binding).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([exists/1, create/2, delete/2, get_all/1, get_all_for_source/1,
         get_all_for_destination/1, get_all/2, get_all_explicit/0,
         fold/2]).

%% Routing. These functions are in the hot code path
-export([match/2, match_routing_key/3]).

%% Exported to be used by various rabbit_db_* modules
-export([
         delete_for_destination_in_mnesia/2,
         delete_for_destination_in_khepri/2,
         delete_all_for_exchange_in_mnesia/3,
         delete_all_for_exchange_in_khepri/3,
         delete_transient_for_destination_in_mnesia/1,
         has_for_source_in_mnesia/1,
         has_for_source_in_khepri/1,
         match_source_and_destination_in_khepri_tx/2
        ]).

%% Recovery is only needed for transient entities. Once mnesia is removed, these
%% functions can be deleted
-export([recover/0, recover/1]).

%% Used by the direct_exchange_routing_v2 feature flag. It can be deleted once mnesia
%% is removed.
-export([create_index_route_table/0]).

-export([mnesia_write_to_khepri/2,
         mnesia_delete_to_khepri/2,
         clear_data_in_khepri/1]).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(Binding) -> Exists when
      Binding :: rabbit_types:binding(),
      Exists :: boolean().
%% @doc Indicates if the binding `Binding' exists.
%%
%% @returns true if the binding exists, false otherwise.
%%
%% @private

exists(#binding{source = SrcName,
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

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(Binding, ChecksFun) -> Ret when
      Binding :: rabbit_types:binding(),
      Src :: rabbit_types:r('exchange'),
      Dst :: rabbit_types:r('exchange') | rabbit_types:r('queue'),
      BindingType :: durable | semi_durable | transient,
      ChecksFun :: fun((Src, Dst) -> {ok, BindingType} | {error, Reason :: any()}),
      Ret :: ok | {error, Reason :: any()}.
%% @doc Writes a binding if it doesn't exist already and passes the validation in
%% `ChecksFun` i.e. exclusive access
%%
%% @returns ok, or an error if the validation has failed.
%%
%% @private

create(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> create_in_mnesia(Binding, ChecksFun) end,
      fun() -> create_in_khepri(Binding, ChecksFun) end).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Binding, ChecksFun) -> Ret when
      Binding :: rabbit_types:binding(),
      Src :: rabbit_types:r('exchange'),
      Dst :: rabbit_types:r('exchange') | rabbit_types:r('queue'),
      BindingType :: durable | semi_durable | transient,
      ChecksFun :: fun((Src, Dst) -> {ok, BindingType} | {error, Reason :: any()}),
      Ret :: ok | {error, Reason :: any()}.
%% @doc Deletes a binding record from the database if it passes the validation in
%% `ChecksFun`. It also triggers the deletion of auto-delete exchanges if needed.
%%
%% @private

delete(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> delete_in_mnesia(Binding, ChecksFun) end,
      fun() -> delete_in_khepri(Binding, ChecksFun) end).

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all(VHostName) -> [Binding] when
      VHostName :: vhost:name(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              VHostResource = rabbit_misc:r(VHost, '_'),
              Match = #route{binding = #binding{source      = VHostResource,
                                                destination = VHostResource,
                                                _           = '_'},
                             _       = '_'},
              [B || #route{binding = B} <- rabbit_db:list_in_mnesia(rabbit_route, Match)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, rabbit_db:if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], rabbit_db:list_in_khepri(Path))
      end).

-spec get_all_for_source(Src) -> [Binding] when
      Src :: rabbit_types:r('exchange'),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given exchange in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all_for_source(#resource{virtual_host = VHost, name = Name} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source = Resource, _ = '_'}},
              [B || #route{binding = B} <- rabbit_db:list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, Name,
                                              rabbit_db:if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], rabbit_db:list_in_khepri(Path))
      end).

-spec get_all_for_destination(Dst) -> [Binding] when
      Dst :: rabbit_types:r('exchange') | rabbit_types:r('queue'),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given exchange or queue destination
%%  in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all_for_destination(#resource{virtual_host = VHost, name = Name,
                                        kind = Kind} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = rabbit_binding:reverse_route(#route{binding = #binding{destination = Resource,
                                                                             _ = '_'}}),
              [rabbit_binding:reverse_binding(B) ||
                  #reverse_route{reverse_binding = B} <- rabbit_db:list_in_mnesia(rabbit_reverse_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, ?KHEPRI_WILDCARD_STAR, Kind, Name,
                                              rabbit_db:if_has_data_wildcard()],
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], rabbit_db:list_in_khepri(Path))
      end).

-spec get_all(Src, Dst) -> [Binding] when
      Src :: rabbit_types:r('exchange'),
      Dst :: rabbit_types:r('exchange') | rabbit_types:r('queue'),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given source and destination
%% in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all(SrcName, DstName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source      = SrcName,
                                                destination = DstName,
                                                _           = '_'}},
              [B || #route{binding = B} <- rabbit_db:list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Values = get_all_in_khepri(SrcName, DstName),
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], Values)
      end).

-spec get_all_explicit() -> [Binding] when
      Binding :: rabbit_types:binding().
%% @doc Returns all explicit binding records, the bindings explicitly added and not
%% automatically generated to the default exchange.
%%
%% @returns the list of binding records.
%%
%% @private

get_all_explicit() ->
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
              Path = khepri_routes_path() ++ [?KHEPRI_WILDCARD_STAR, Condition,
                                              rabbit_db:if_has_data_wildcard()],
              {ok, Data} = rabbit_khepri:match(Path),
              lists:foldl(fun(SetOfBindings, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], maps:values(Data))
      end).

-spec fold(Fun, Acc) -> Acc when
      Fun :: fun((Binding :: rabbit_types:binding(), Acc) -> Acc),
      Acc :: any().
%% @doc Folds over all the bindings, making it more efficient than `get_all()` and
%% folding over the returned binding list.
%% Just used by prometheus_rabbitmq_core_metrics_collector to iterate over the bindings.
%%
%% @returns the fold accumulator
%%
%% @private

fold(Fun, Acc) ->
    %% It used to query `rabbit_route` directly, which isn't valid when using Khepri
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ets:foldl(fun(#route{binding = Binding}, Acc0) ->
                                Fun(Binding, Acc0)
                        end, Acc, rabbit_route)
      end,
      fun() ->
              Path = khepri_routes_path() ++ [?KHEPRI_WILDCARD_STAR,
                                              rabbit_db:if_has_data_wildcard()],
              {ok, Res} = rabbit_khepri:fold(
                            Path,
                            fun(_, #{data := SetOfBindings}, Acc0) ->
                                    lists:foldl(fun(Binding, Acc1) ->
                                                        Fun(Binding, Acc1)
                                                end, Acc0, sets:to_list(SetOfBindings))
                            end, Acc),
              Res
      end).

recover() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> recover_in_mnesia() end,
      %% Nothing to do in khepri, single table storage
      fun() -> ok end).

recover(RecoverFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              [RecoverFun(Route, Src, Dst, fun recover_semi_durable_route/2, mnesia) ||
                  #route{binding = #binding{destination = Dst,
                                            source = Src}} = Route <-
                      rabbit_misc:dirty_read_all(rabbit_semi_durable_route)]
      end,
      fun() ->
              ok
      end).

create_index_route_table() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              TableName = rabbit_index_route,
              ok = rabbit_table:wait([rabbit_route], _Retry = true),
              ok = rabbit_table:create(
                     TableName, rabbit_table:rabbit_index_route_definition()),
              case rabbit_table:ensure_table_copy(TableName, node(), ram_copies) of
                  ok ->
                      ok = populate_index_route_table_in_mnesia();
                  Error ->
                      Error
              end
      end,
      fun() ->
              ok
      end).

%% Routing - HOT CODE PATH

match(SrcName, Match) ->
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

delete_all_for_exchange_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> delete_for_source_in_mnesia(XName);
                   false -> []
               end,
    {deleted, X, Bindings, delete_for_destination_in_mnesia(XName, OnlyDurable, fun delete_routes/1)}.

delete_all_for_exchange_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> delete_for_source_in_khepri(XName);
                   false -> []
               end,
    {deleted, X, Bindings, delete_for_destination_in_khepri(XName, OnlyDurable)}.

delete_for_destination_in_mnesia(DstName, OnlyDurable) ->
    delete_for_destination_in_mnesia(DstName, OnlyDurable, fun delete_routes/1).

delete_for_destination_in_khepri(DstName, OnlyDurable) ->
    BindingsMap = match_destination_in_khepri(DstName),
    maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, BindingsMap),
    Bindings = maps:fold(fun(_, Set, Acc) ->
                                 sets:to_list(Set) ++ Acc
                         end, [], BindingsMap),
    lists:foreach(fun(Binding) -> delete_routing(Binding) end, Bindings),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

-spec delete_transient_for_destination_in_mnesia(rabbit_types:binding_destination()) -> rabbit_binding:deletions().
delete_transient_for_destination_in_mnesia(DstName) ->
    delete_for_destination_in_mnesia(DstName, false, fun delete_transient_routes/1).

-spec has_for_source_in_mnesia(rabbit_types:binding_source()) -> boolean().

has_for_source_in_mnesia(SrcName) ->
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    %% we need to check for semi-durable routes (which subsumes
    %% durable routes) here too in case a bunch of routes to durable
    %% queues have been removed temporarily as a result of a node
    %% failure
    contains(rabbit_route, Match) orelse
        contains(rabbit_semi_durable_route, Match).

has_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, rabbit_db:if_has_data_wildcard()],
    case khepri_tx:get_many(Path) of
        {ok, Map} ->
            maps:size(Map) > 0;
        Error ->
            Error
    end.

match_source_and_destination_in_khepri_tx(#resource{virtual_host = VHost, name = Name},
                                       #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName, rabbit_db:if_has_data_wildcard()],
    case khepri_tx:get_many(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

%% Migration
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

%% Paths
%% --------------------------------------------------------------
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


%% Internal
%% --------------------------------------------------------------
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

match_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, rabbit_db:if_has_data_wildcard()],
    {ok, Map} = rabbit_khepri:match(Path),
    Map.

match_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, ?KHEPRI_WILDCARD_STAR, Kind, Name, ?KHEPRI_WILDCARD_STAR_STAR],
    {ok, Map} = khepri_tx:get_many(Path),
    Map.

match_source_and_key_in_khepri(Src, ['_']) ->
    Path = khepri_routing_path(Src, rabbit_db:if_has_data_wildcard()),
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

create_in_mnesia(Binding, ChecksFun) ->
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

create_in_khepri(#binding{source = SrcName,
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

populate_index_route_table_in_mnesia() ->
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

get_all_in_khepri(#resource{virtual_host = VHost, name = Name},
                  #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName,
                                    rabbit_db:if_has_data_wildcard()],
    rabbit_db:list_in_khepri(Path).

delete_in_mnesia(Binding, ChecksFun) ->
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
                            _  -> delete_in_mnesia(Src, Dst, Binding)
                        end;
                  _  -> case ChecksFun(Dst) of
                            ok               -> delete_in_mnesia(Src, Dst, Binding);
                            {error, _} = Err -> rabbit_misc:const(Err)
                        end
              end
      end, fun absent_errs_only_in_mnesia/1).

delete_in_mnesia(Src, Dst, B) ->
    ok = sync_route(#route{binding = B}, rabbit_binding:binding_type(Src, Dst),
                    should_index_table(Src), fun delete/3),
    Deletions0 = maybe_auto_delete_exchange_in_mnesia(
                   B#binding.source, [B], rabbit_binding:new_deletions(), false),
    fun() -> rabbit_binding:process_deletions(Deletions0) end.

delete_in_khepri(#binding{source = SrcName,
                          destination = DstName} = Binding, ChecksFun) ->
    Path = khepri_route_path(Binding),
    case rabbit_khepri:transaction(
           fun () ->
                   case {lookup_resource_in_khepri_tx(SrcName),
                         lookup_resource_in_khepri_tx(DstName)} of
                       {[_Src], [Dst]} ->
                           case exists_in_khepri(Path, Binding) of
                               false ->
                                   ok;
                               true ->
                                   case ChecksFun(Dst) of
                                       ok ->
                                           ok = delete_in_khepri(Binding),
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

delete_in_khepri(Binding) ->
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

delete_for_destination_in_mnesia(DstName, OnlyDurable, Fun) ->
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

delete_for_source_in_mnesia(#exchange{name = SrcName} = SrcX) ->
    delete_for_source_in_mnesia(SrcName, should_index_table(SrcX));
delete_for_source_in_mnesia(SrcName) ->
    delete_for_source_in_mnesia(SrcName, undefined).

-spec delete_for_source_in_mnesia(rabbit_types:binding_source(),
                                           boolean() | undefined) -> [rabbit_types:binding()].
delete_for_source_in_mnesia(SrcName, ShouldIndexTable) ->
    lock_resource(SrcName),
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    delete_routes(
      lists:usort(
        mnesia:dirty_match_object(rabbit_route, Match) ++
            mnesia:dirty_match_object(rabbit_semi_durable_route, Match)),
      ShouldIndexTable).

delete_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name],
    {ok, Bindings} = khepri_tx:get_many(Path ++ [rabbit_db:if_has_data_wildcard()]),
    ok = khepri_tx:delete(Path),
    maps:fold(fun(_, Set, Acc) ->
                      sets:to_list(Set) ++ Acc
              end, [], Bindings).

delete_routes(Routes) ->
    delete_routes(Routes, undefined).

delete_routes(Routes, ShouldIndexTable) ->
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

delete_transient_routes(Routes) ->
    lists:map(fun(#route{binding = #binding{source = Src} = Binding} = Route) ->
                      {ok, X} = rabbit_db_exchange:get(Src),
                      ok = sync_transient_route(Route, should_index_table(X), fun delete/3),
                      Binding
              end, Routes).

delete(Tab, #route{binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #reverse_route{reverse_binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #index_route{} = Record, LockKind) ->
    mnesia:delete_object(Tab, Record, LockKind).

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

exists_in_khepri(Path, Binding) ->
    case khepri_tx:get(Path) of
        {ok, Set} ->
            sets:is_element(Binding, Set);
        _ ->
            false
    end.

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

recover_in_mnesia() ->
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

recover_semi_durable_route(#route{binding = B} = Route, X) ->
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
      end).

serial_in_mnesia(false, _) ->
    none;
serial_in_mnesia(true, X) ->
    rabbit_db_exchange:next_serial_in_mnesia(X).

serial_in_khepri(false, _) ->
    none;
serial_in_khepri(true, X) ->
    rabbit_db_exchange:next_serial_in_khepri(X).

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

%% Instead of locking entire table on remove operations we can lock the
%% affected resource only.
lock_resource(Name) -> lock_resource(Name, write).

lock_resource(Name, LockKind) ->
    mnesia:lock({global, Name, mnesia:table_info(rabbit_route, where_to_write)},
                LockKind).

khepri_delete(Path) ->
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

%% Routing. Hot code path
%% -------------------------------------------------------------------------
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

