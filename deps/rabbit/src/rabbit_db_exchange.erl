%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_exchange).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         get_all/0,
         get_all/1,
         get_all_durable/0,
         list/0,
         get/1,
         get_many/1,
         count/0,
         update/2,
         create_or_get/1,
         insert/1,
         peek_serial/1,
         next_serial/1,
         delete/2,
         delete_serial/1,
         recover/1,
         match/1,
         exists/1
        ]).

%% Used by other rabbit_db_* modules
-export([
         maybe_auto_delete_in_khepri/2,
         maybe_auto_delete_in_mnesia/2,
         next_serial_in_mnesia/1,
         next_serial_in_khepri/1,
         delete_in_khepri/3,
         delete_in_mnesia/3,
         get_in_khepri_tx/1,
         update_in_mnesia/2,
         update_in_khepri/2,
         path/1
         ]).

-export([mnesia_write_to_khepri/2,
         mnesia_delete_to_khepri/2,
         clear_data_in_khepri/1]).

-type name() :: rabbit_types:r('exchange').

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [Exchange] when
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchange records.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_db:list_in_mnesia(rabbit_exchange, #exchange{_ = '_'})
      end,
      fun() ->
              rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [rabbit_db:if_has_data_wildcard()])
      end).

-spec get_all(VHostName) -> [Exchange] when
      VHostName :: vhost:name(),
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchange records in the given virtual host.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_exchanges_in_mnesia(VHost)
      end,
      fun() ->
              list_exchanges_in_khepri(VHost)
      end).

-spec get_all_durable() -> [Exchange] when
      Exchange :: rabbit_types:exchange().
%% @doc Returns all durable exchange records.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all_durable() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_db:list_in_mnesia(rabbit_durable_exchange, #exchange{_ = '_'})
      end,
      fun() ->
              rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [rabbit_db:if_has_data_wildcard()])
      end).

%% -------------------------------------------------------------------
%% list().
%% -------------------------------------------------------------------

-spec list() -> [Exchange] when
      Exchange :: rabbit_types:exchange().
%% @doc Lists the names of all exchanges.
%%
%% @returns a list of exchange names.
%%
%% @private

list() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:dirty_all_keys(rabbit_exchange)
      end,
      fun() ->
              case rabbit_khepri:match(khepri_exchanges_path() ++ [rabbit_db:if_has_data_wildcard()]) of
                  {ok, Map} ->
                      maps:fold(fun(_K, X, Acc) -> [X#exchange.name | Acc] end, [], Map);
                  _ ->
                      []
              end
      end).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(ExchangeName) -> Ret when
      ExchangeName :: name(),
      Ret :: {ok, Exchange :: rabbit_types:exchange()} | {error, not_found}.
%% @doc Returns the record of the exchange named `Name'.
%%
%% @returns the exchange record or `{error, not_found}' if no exchange is named
%% `Name'.
%%
%% @private

get(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:dirty_read({rabbit_exchange, Name})
      end,
      fun() ->
              get_in_khepri(Name)
      end).

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many([ExchangeName]) -> [Exchange] when
      ExchangeName :: name(),
      Exchange :: rabbit_types:exchange().
%% @doc Returns the records of the exchanges named `Name'.
%%
%% @returns a list of exchange records.
%%
%% @private

get_many(Names) when is_list(Names) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              get_many_in_mnesia(rabbit_exchange, Names)
      end,
      fun() ->
              get_many_in_khepri(Names)
      end).

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec count() -> Num :: integer().
%% @doc Counts the number of exchanges.
%%
%% @returns the number of exchange records.
%%
%% @private

count() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:table_info(rabbit_exchange, size)
      end,
      fun() ->
              rabbit_khepri:count_children(khepri_exchanges_path() ++ [?KHEPRI_WILDCARD_STAR])
      end).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(ExchangeName, UpdateFun) -> Ret when
      ExchangeName :: name(),
      UpdateFun :: fun((Exchange) -> Exchange),
      Ret :: Exchange :: rabbit_types:exchange() | not_found.
%% @doc Updates an existing exchange record using the result of
%% `UpdateFun'.
%%
%% @returns the updated exchange record if the record existed and the
%% update succeeded. It returns `not_found` if the transaction fails.
%%
%% @private

update(#resource{virtual_host = VHost, name = Name} = XName, Fun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        update_in_mnesia(XName, Fun)
                end)
      end,
      fun() ->
              Path = khepri_exchange_path(XName),
              rabbit_db:retry(
                fun () ->
                        case rabbit_khepri:adv_get(Path) of
                            {ok, #{data := X, payload_version := Vsn}} ->
                                X1 = Fun(X),
                                Conditions = #if_all{conditions = [Name, #if_payload_version{version = Vsn}]},
                                UpdatePath = khepri_exchanges_path() ++ [VHost, Conditions],
                                ok = rabbit_khepri:put(UpdatePath, X1),
                                X1;
                            _ ->
                                not_found
                        end
                end)
      end).

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(Exchange) -> Ret when
      Exchange :: rabbit_types:exchange(),
      Ret :: {existing | new, Exchange}.
%% @doc Writes an exchange record if it doesn't exist already or returns
%% the existing one.
%%
%% @returns the existing record if there is one in the database already, or
%% the newly created record.
%%
%% @private

create_or_get(#exchange{name = XName} = X) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() -> create_in_mnesia({rabbit_exchange, XName}, X) end)
      end,
      fun() ->
              create_in_khepri(khepri_exchange_path(XName), X)
      end).

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

-spec insert([Exchange]) -> ok when
      Exchange :: rabbit_types:exchange().
%% @doc Writes the exchange records.
%%
%% @returns ok.
%%
%% @private

insert(Xs) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () ->
                        [mnesia:write(rabbit_durable_exchange, X, write) || X <- Xs]
                end),
              ok
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        [store_exchange_in_khepri(X) || X <- Xs]
                end, rw),
              ok
      end).

%% -------------------------------------------------------------------
%% peek_serial().
%% -------------------------------------------------------------------

-spec peek_serial(ExchangeName) -> Serial when
      ExchangeName :: name(),
      Serial :: integer().
%% @doc Returns the next serial number without increasing it.
%%
%% @returns the next serial number
%%
%% @private

peek_serial(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        peek_serial_in_mnesia(XName, read)
                end)
      end,
      fun() ->
              Path = khepri_exchange_serial_path(XName),
              case rabbit_khepri:get(Path) of
                  {ok, Serial} ->
                      Serial;
                  _ ->
                      1
              end
      end).

%% -------------------------------------------------------------------
%% next_serial().
%% -------------------------------------------------------------------

-spec next_serial(ExchangeName) -> Serial when
      ExchangeName :: name(),
      Serial :: integer().
%% @doc Returns the next serial number and increases it.
%%
%% @returns the next serial number
%%
%% @private

next_serial(#exchange{name = #resource{name = Name, virtual_host = VHost} = XName} = X) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(fun() ->
                                                             next_serial_in_mnesia(X)
                                                     end)
      end,
      fun() ->
              %% Just storing the serial number is enough, no need to keep #exchange_serial{}
              Path = khepri_exchange_serial_path(XName),
              rabbit_db:retry(
                fun() ->
                        case rabbit_khepri:adv_get(Path) of
                            {ok, #{data := Serial,
                                   payload_version := Vsn}} ->
                                Conditions = #if_all{conditions = [Name, #if_payload_version{version = Vsn}]},
                                UpdatePath = khepri_exchange_serials_path() ++ [VHost, Conditions],
                                case rabbit_khepri:put(UpdatePath, Serial + 1) of
                                    ok ->
                                        Serial;
                                    Err ->
                                        Err
                                end;
                            _ ->
                                Serial = 1,
                                ok = rabbit_khepri:put(Path, Serial + 1),
                                Serial
                        end
                end)
      end).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(ExchangeName, IfUnused) -> Ret when
      ExchangeName :: name(),
      IfUnused :: boolean(),
      Exchange :: rabbit_types:exchange(),
      Binding :: rabbit_types:binding(),
      Deletions :: dict:dict(),
      Ret :: {error, not_found} | {deleted, Exchange, [Binding], Deletions}.
%% @doc Deletes an exchange record from the database. If `IfUnused` is set
%% to `true`, it is only deleted when there are no bindings present on the
%% exchange.
%%
%% @returns an error if the exchange does not exist or a tuple with the exchange,
%% bindings and deletions. Bindings need to be processed on the same transaction, and
%% are later used to generate notifications. Probably shouldn't be here, but not sure
%% how to split it while keeping it atomic. Maybe something about deletions could be
%% handled outside of the transaction.
%%
%% @private

delete(XName, IfUnused) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              DeletionFun = case IfUnused of
                                true  -> fun conditional_delete_in_mnesia/2;
                                false -> fun unconditional_delete_in_mnesia/2
                            end,
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        case mnesia:wread({rabbit_exchange, XName}) of
                            [X] -> DeletionFun(X, false);
                            [] -> {error, not_found}
                        end
                end)
      end,
      fun() ->
              DeletionFun = case IfUnused of
                                true  -> fun conditional_delete_in_khepri/2;
                                false -> fun unconditional_delete_in_khepri/2
                            end,
              rabbit_khepri:transaction(
                fun() ->
                        case khepri_tx:get(khepri_exchange_path(XName)) of
                            {ok, X} -> DeletionFun(X, false);
                            _ -> {error, not_found}
                        end
                end)
      end).

%% -------------------------------------------------------------------
%% delete_serial().
%% -------------------------------------------------------------------

-spec delete_serial(ExchangeName) -> ok when
      ExchangeName :: name().
%% @doc Deletes an exchange serial record from the database.
%%
%% @returns ok
%%
%% @private

delete_serial(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        mnesia:delete({rabbit_exchange_serial, XName})
                end)
      end,
      fun() ->
              Path = khepri_exchange_serial_path(XName),
              ok = rabbit_khepri:delete(Path)
      end).

%% -------------------------------------------------------------------
%% recover().
%% -------------------------------------------------------------------

-spec recover(VHostName) -> ok when
      VHostName :: vhost:name().
%% @doc Recovers all exchanges for a given vhost
%%
%% @returns ok
%%
%% @private

recover(VHost) ->
   rabbit_khepri:try_mnesia_or_khepri(
     fun() -> recover_in_mnesia(VHost) end,
     fun() -> recover_in_khepri(VHost) end).

%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(Pattern) -> [Exchange] when
      Pattern :: #exchange{},
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchanges that match a given pattern
%%
%% @returns a list of exchange records
%%
%% @private

match(Pattern0) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              case mnesia:transaction(
                     fun() ->
                             mnesia:match_object(rabbit_exchange, Pattern0, read)
                     end) of
                  {atomic, Xs} -> Xs;
                  {aborted, Err} -> {error, Err}
              end
      end,
      fun() ->
              %% TODO error handling?
              Pattern = #if_data_matches{pattern = Pattern0},
              rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [?KHEPRI_WILDCARD_STAR, Pattern])
      end).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(ExchangeName) -> Exists when
      ExchangeName :: name(),
      Exists :: boolean().
%% @doc Indicates if the exchange named `Name' exists.
%%
%% @returns true if the exchange exists, false otherwise.
%%
%% @private

exists(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ets:member(rabbit_exchange, Name)
      end,
      fun() ->
              rabbit_khepri:exists(khepri_exchange_path(Name))
      end).

%% Migration
%% --------------------------------------------------------------

%% Mnesia contains two tables if an exchange has been recovered:
%% rabbit_exchange (ram) and rabbit_durable_exchange (disc).
%% As all data in Khepri is persistent, there is no point on
%% having ram and data entries.
%% How do we then transform data from mnesia to khepri when
%% the feature flag is enabled?
%% Let's create the Khepri entry from the ram table.
mnesia_write_to_khepri(rabbit_exchange, Exchanges) ->
    rabbit_khepri:transaction(
      fun() ->
              [khepri_create_tx(khepri_exchange_path(Exchange#exchange.name), Exchange)
               || Exchange <- Exchanges]
      end, rw);                       
mnesia_write_to_khepri(rabbit_durable_exchange, _Exchange0) ->
    ok;
mnesia_write_to_khepri(rabbit_exchange_serial, Exchanges) ->
    rabbit_khepri:transaction(
      fun() ->
              [begin
                   #exchange_serial{name = Resource, next = Serial} = Exchange,
                   Path = khepri_path:combine_with_conditions(khepri_exchange_serial_path(Resource),
                                                              [#if_node_exists{exists = false}]),
                   case khepri_tx:put(Path, Serial) of
                       ok -> ok;
                       Error -> throw(Error)
                   end
               end || Exchange <- Exchanges]
      end, rw).

mnesia_delete_to_khepri(rabbit_exchange, Exchange) when is_record(Exchange, exchange) ->
    khepri_delete(khepri_exchange_path(Exchange#exchange.name));
mnesia_delete_to_khepri(rabbit_exchange, Name) ->
    khepri_delete(khepri_exchange_path(Name));
mnesia_delete_to_khepri(rabbit_durable_exchange, Exchange)
  when is_record(Exchange, exchange) ->
    khepri_delete(khepri_exchange_path(Exchange#exchange.name));
mnesia_delete_to_khepri(rabbit_durable_exchange, Name) ->
    khepri_delete(khepri_exchange_path(Name));
mnesia_delete_to_khepri(rabbit_exchange_serial, ExchangeSerial)
  when is_record(ExchangeSerial, exchange_serial) ->
    khepri_delete(khepri_exchange_serial_path(ExchangeSerial#exchange_serial.name));
mnesia_delete_to_khepri(rabbit_exchange_serial, Name) ->
    khepri_delete(khepri_exchange_serial_path(Name)).

clear_data_in_khepri(rabbit_exchange) ->
    khepri_delete(khepri_exchanges_path());
%% There is a single khepri entry for exchanges and it should be already deleted
clear_data_in_khepri(rabbit_durable_exchange) ->
    ok;
clear_data_in_khepri(rabbit_exchange_serial) ->
    khepri_delete(khepri_exchange_serials_path()).

%% Internal
%% --------------------------------------------------------------
khepri_exchanges_path() ->
    [?MODULE, exchanges].

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchanges, VHost, Name].

khepri_exchange_serials_path() ->
    [?MODULE, exchange_serials].

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchange_serials, VHost, Name].

path(Name) ->
    khepri_exchange_path(Name).

get_in_khepri_tx(Name) ->
    case khepri_tx:get(khepri_exchange_path(Name)) of
        {ok, X} -> [X];
        _ -> []
    end.

list_exchanges_in_mnesia(VHost) ->
    Match = #exchange{name = rabbit_misc:r(VHost, exchange), _ = '_'},
    rabbit_db:list_in_mnesia(rabbit_exchange, Match).

list_exchanges_in_khepri(VHost) ->
    rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [VHost, rabbit_db:if_has_data_wildcard()]).

peek_serial_in_mnesia(XName, LockType) ->
    case mnesia:read(rabbit_exchange_serial, XName, LockType) of
        [#exchange_serial{next = Serial}]  -> Serial;
        _                                  -> 1
    end.

next_serial_in_mnesia(#exchange{name = XName}) ->
    Serial = peek_serial_in_mnesia(XName, write),
    ok = mnesia:write(rabbit_exchange_serial,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial.

next_serial_in_khepri(#exchange{name = XName}) ->
    Path = khepri_exchange_serial_path(XName),
    Serial = case khepri_tx:get(Path) of
                 {ok, Serial0} -> Serial0;
                 _ -> 1
             end,
    ok = khepri_tx:put(Path, Serial + 1),
    Serial.

update_in_mnesia(Name, Fun) ->
    Table = {rabbit_exchange, Name},
    case mnesia:wread(Table) of
        [X] -> X1 = Fun(X),
               store_exchange_in_mnesia(X1);
        [] -> not_found
    end.

update_in_khepri(Name, Fun) ->
    Path = khepri_exchange_path(Name),
    case khepri_tx:get(Path) of
        {ok, X} ->
            X1 = Fun(X),
            ok = khepri_tx:put(Path, X1),
            X1;
        _ -> not_found
    end.

delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = mnesia:delete({rabbit_exchange, XName}),
    mnesia:delete({rabbit_durable_exchange, XName}),
    rabbit_db_binding:delete_all_for_exchange_in_mnesia(X, OnlyDurable, RemoveBindingsForSource).

delete_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = khepri_tx:delete(khepri_exchange_path(XName)),
    rabbit_db_binding:delete_all_for_exchange_in_khepri(X, OnlyDurable, RemoveBindingsForSource).

get_in_khepri(Name) ->
    Path = khepri_exchange_path(Name),
    case rabbit_khepri:get(Path) of
        {ok, X} -> {ok, X};
        _ -> {error, not_found}
    end.

get_many_in_mnesia(Table, [Name]) -> ets:lookup(Table, Name);
get_many_in_mnesia(Table, Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_misc:dirty_read/1.
    lists:append([ets:lookup(Table, Name) || Name <- Names]).

get_many_in_khepri(Names) when is_list(Names) ->
    lists:foldl(fun(Name, Acc) ->
                        case get_in_khepri(Name) of
                            {ok, X} -> [X | Acc];
                            _ -> Acc
                        end
                end, [], Names).

conditional_delete_in_khepri(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_db_binding:has_for_source_in_khepri(XName) of
        false  -> delete_in_khepri(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

conditional_delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_db_binding:has_for_source_in_mnesia(XName) of
        false  -> delete_in_mnesia(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

unconditional_delete_in_mnesia(X, OnlyDurable) ->
    delete_in_mnesia(X, OnlyDurable, true).

unconditional_delete_in_khepri(X, OnlyDurable) ->
    delete_in_khepri(X, OnlyDurable, true).

-spec maybe_auto_delete_in_mnesia
        (rabbit_types:exchange(), boolean())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}.
maybe_auto_delete_in_mnesia(XName, OnlyDurable) ->
    case mnesia:read({case OnlyDurable of
                          true  -> rabbit_durable_exchange;
                          false -> rabbit_exchange
                      end, XName}) of
        []  -> {not_deleted, undefined};
        [#exchange{auto_delete = false} = X] -> {not_deleted, X};
        [#exchange{auto_delete = true} = X] ->
            case conditional_delete_in_mnesia(X, OnlyDurable) of
                {error, in_use}             -> {not_deleted, X};
                {deleted, X, [], Deletions} -> {deleted, X, Deletions}
            end
    end.

maybe_auto_delete_in_khepri(XName, OnlyDurable) ->
    case khepri_tx:get(khepri_exchange_path(XName)) of
        {ok, #exchange{auto_delete = false} = X} ->
            {not_deleted, X};
        {ok, #exchange{auto_delete = true} = X} ->
            case conditional_delete_in_khepri(X, OnlyDurable) of
                {error, in_use}             -> {not_deleted, X};
                {deleted, X, [], Deletions} -> {deleted, X, Deletions}
            end;
        [] ->
            {not_deleted, undefined}
    end.

create_in_mnesia(Name, X) ->
    case mnesia:wread(Name) of
        [] ->
            {new, store_exchange_in_mnesia(X)};
        [ExistingX] ->
            {existing, ExistingX}
    end.

create_in_khepri(Path, X) ->
    case rabbit_khepri:create(Path, X) of
        ok ->
            {new, X};
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingX}}}} ->
            {existing, ExistingX}
    end.

store_exchange_in_mnesia(X = #exchange{durable = true}) ->
    mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
                 write),
    store_ram_exchange(X);
store_exchange_in_mnesia(X = #exchange{durable = false}) ->
    store_ram_exchange(X).

store_exchange_in_khepri(X) ->
    Path = khepri_exchange_path(X#exchange.name),
    ok = khepri_tx:put(Path, X),
    X.

store_ram_exchange(X) ->
    X1 = rabbit_exchange_decorator:set(X),
    ok = mnesia:write(rabbit_exchange, X1, write),
    X1.

recover_in_mnesia(VHost) ->
    rabbit_misc:table_filter(
      fun (#exchange{name = XName}) ->
              XName#resource.virtual_host =:= VHost andalso
                  mnesia:read({rabbit_exchange, XName}) =:= []
      end,
      fun (X, true) ->
              X;
          (X, false) ->
              X1 = rabbit_misc:execute_mnesia_transaction(
                     fun() -> store_exchange_in_mnesia(X) end),
              Serial = rabbit_exchange:serial(X1),
              rabbit_exchange:callback(X1, create, Serial, [X1])
      end,
      rabbit_durable_exchange).

recover_in_khepri(VHost) ->
    %% Transient exchanges are deprecated in Khepri, all exchanges are recovered
    Exchanges0 = rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [VHost, rabbit_db:if_has_data_wildcard()],
                                #{timeout => infinity}),
    Exchanges = [rabbit_exchange_decorator:set(X) || X <- Exchanges0],

    rabbit_khepri:transaction(
      fun() ->
              [_ = store_exchange_in_khepri(X) || X <- Exchanges]
      end, rw, #{timeout => infinity}),
    %% TODO once mnesia is gone, this callback should go back to `rabbit_exchange`
    [begin
         Serial = rabbit_exchange:serial(X),
         rabbit_exchange:callback(X, create, Serial, [X])
     end || X <- Exchanges],
    Exchanges.

khepri_delete(Path) ->
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

khepri_create_tx(Path, Value) ->
    case khepri_tx:create(Path, Value) of
        ok -> ok;
        {error, {khepri, mismatching_node, _}} -> ok;
        Error -> throw(Error)
    end.
