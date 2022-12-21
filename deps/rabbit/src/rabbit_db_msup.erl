%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_msup).

-include_lib("khepri/include/khepri.hrl").

-define(TABLE, mirrored_sup_childspec).
-define(TABLE_DEF,
        {?TABLE,
         [{record_name, mirrored_sup_childspec},
          {type, ordered_set},
          {attributes, record_info(fields, mirrored_sup_childspec)}]}).
-define(TABLE_MATCH, {match, #mirrored_sup_childspec{ _ = '_' }}).

-export([create_tables/0, table_definitions/0,
         create_or_update/5, delete/2,
         find_mirror/2, update_all/2, delete_all/1]).

-record(mirrored_sup_childspec, {key, mirroring_pid, childspec}).

-spec create_tables() -> Ret when
      Ret :: 'ok' | {error, Reason :: term()}.

create_tables() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              create_tables([?TABLE_DEF])
      end,
      fun() ->
              ok
      end).

-spec table_definitions() -> [Def] when
      Def :: {Name :: atom(), term()}.

table_definitions() ->
    {Name, Attributes} = ?TABLE_DEF,
    [{Name, [?TABLE_MATCH | Attributes]}].

-spec create_or_update(Group, Overall, Delegate, ChildSpec, Id) -> Ret when
      Group :: any(),
      Overall :: pid(),
      Delegate :: pid() | undefined,
      ChildSpec :: supervisor2:child_spec(),
      Id :: {any(), any()},
      Ret :: start | undefined | pid().

create_or_update(Group, Overall, Delegate, ChildSpec, Id) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              create_or_update_in_mnesia(Group, Overall, Delegate, ChildSpec, Id)
      end,
      fun() ->
              create_or_update_in_khepri(Group, Overall, Delegate, ChildSpec, Id)
      end).

-spec delete(Group, Id) -> ok when
      Group :: any(),
      Id :: {any(), any()}.

delete(Group, {SimpleId, _} = Id) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() -> delete_in_mnesia(Group, Id) end)
      end,
      fun() ->
              ok = rabbit_khepri:delete(khepri_mirrored_supervisor_path(Group, SimpleId))
      end).

-spec find_mirror(Group, Id) -> Ret when
      Group :: any(),
      Id :: {any(), any()},
      Ret :: {ok, pid()} | {error, not_found}.

find_mirror(Group, {SimpleId, _} = Id) ->
    %% If we did this inside a tx we could still have failover
    %% immediately after the tx - we can't be 100% here. So we may as
    %% well dirty_select.
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              MatchHead = #mirrored_sup_childspec{mirroring_pid = '$1',
                                                  key           = {Group, Id},
                                                  _             = '_'},
              case mnesia:dirty_select(?TABLE, [{MatchHead, [], ['$1']}]) of
                  [Mirror] -> {ok, Mirror};
                  _ -> {error, not_found}
              end
      end,
      fun() ->
              case rabbit_khepri:get(khepri_mirrored_supervisor_path(Group, SimpleId)) of
                  {ok, #mirrored_sup_childspec{mirroring_pid = Pid}} ->
                      {ok, Pid};
                  _ ->
                      {error, not_found}
              end
      end).

-spec update_all(Overall, Overall) -> [ChildSpec] when
      Overall :: pid(),
      ChildSpec :: supervisor2:child_spec().

update_all(Overall, OldOverall) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              update_all_in_mnesia(Overall, OldOverall)
      end,
      fun() ->
              update_all_in_khepri(Overall, OldOverall)
      end).

-spec delete_all(Group) -> ok when
      Group :: any().

delete_all(Group) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              delete_all_in_mnesia(Group)
      end,
      fun() ->
              delete_all_in_khepri(Group)
      end).

%%----------------------------------------------------------------------------

create_tables([]) ->
    ok;
create_tables([{Table, Attributes} | Ts]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                        -> create_tables(Ts);
        {aborted, {already_exists, ?TABLE}} -> create_tables(Ts);
        Err                                 -> Err
    end.

create_or_update_in_mnesia(Group, Overall, Delegate, ChildSpec, Id) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              ReadResult = mnesia:wread({?TABLE, {Group, Id}}),
              rabbit_log:debug("Mirrored supervisor: check_start table ~ts read for key ~tp returned ~tp",
                               [?TABLE, {Group, Id}, ReadResult]),
              case ReadResult of
                  []  -> _ = write_in_mnesia(Group, Overall, ChildSpec, Id),
                         start;
                  [S] -> #mirrored_sup_childspec{key           = {Group, Id},
                                                 mirroring_pid = Pid} = S,
                         case Overall of
                             Pid ->
                                 rabbit_log:debug("Mirrored supervisor: overall matched mirrored pid ~tp", [Pid]),
                                 Delegate;
                             _   ->
                                 rabbit_log:debug("Mirrored supervisor: overall ~tp did not match mirrored pid ~tp", [Overall, Pid]),
                                 Sup = mirrored_supervisor:supervisor(Pid),
                                 rabbit_log:debug("Mirrored supervisor: supervisor(~tp) returned ~tp", [Pid, Sup]),
                                 case Sup of
                                     dead      ->
                                         _ = write_in_mnesia(Group, Overall, ChildSpec, Id),
                                         start;
                                     Delegate0 ->
                                         Delegate0
                                 end
                         end
              end
      end).

create_or_update_in_khepri(Group, Overall, Delegate, ChildSpec, {SimpleId, _} = Id) ->
    Path = khepri_mirrored_supervisor_path(Group, SimpleId),
    Path0 = khepri_mirrored_supervisor_path(Group),
    S = #mirrored_sup_childspec{key           = {Group, Id},
                                mirroring_pid = Overall,
                                childspec     = ChildSpec},
    rabbit_db:retry(
      fun() ->
              case rabbit_khepri:adv_get(Path) of
                  {ok, #{data := #mirrored_sup_childspec{mirroring_pid = Pid},
                         payload_version := Vsn}} ->
                      case Overall of
                          Pid ->
                              Delegate;
                          _   ->
                              %% The supervisor(Pid) call can't happen inside of a transaction.
                              %% We have to read and update the record in two different khepri calls
                              case mirrored_supervisor:supervisor(Pid) of
                                  dead ->
                                      Conditions = #if_all{conditions = [SimpleId, #if_payload_version{version = Vsn}]},
                                      UpdatePath = Path0 ++ [Conditions],
                                      ok = rabbit_khepri:put(UpdatePath, S),
                                      start;
                                  Delegate0 ->
                                      Delegate0
                              end
                      end;
                  _  ->
                      ok = rabbit_khepri:put(Path, S),
                      start
              end
      end).

update_all_in_mnesia(Overall, OldOverall) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              MatchHead = #mirrored_sup_childspec{mirroring_pid = OldOverall,
                                                  key           = '$1',
                                                  childspec     = '$2',
                                                  _             = '_'},
              [write_in_mnesia(Group, Overall, C, Id) ||
                  [{Group, Id}, C] <- mnesia:select(?TABLE, [{MatchHead, [], ['$$']}])]
      end).

update_all_in_khepri(Overall, OldOverall) ->
    Pattern = #mirrored_sup_childspec{mirroring_pid = OldOverall,
                                      _             = '_'},
    Conditions = [?KHEPRI_WILDCARD_STAR_STAR, #if_data_matches{pattern = Pattern}],
    PathPattern = khepri_mirrored_supervisor_path() ++ [#if_all{conditions = Conditions}],
    rabbit_khepri:transaction(
      fun() ->
              case khepri_tx:get_many(PathPattern) of
                  {ok, Map} ->
                      [begin
                           S = S0#mirrored_sup_childspec{mirroring_pid = Overall},
                           ok = khepri_tx:put(Path, S),
                           S0#mirrored_sup_childspec.childspec
                       end || {Path, S0} <- maps:to_list(Map)]
              end
      end).

delete_all_in_mnesia(Group) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              MatchHead = #mirrored_sup_childspec{key       = {Group, '$1'},
                                                  _         = '_'},
              [delete_in_mnesia(Group, Id) ||
                  Id <- mnesia:select(?TABLE, [{MatchHead, [], ['$1']}])]
      end),
    ok.

delete_all_in_khepri(Group) ->
    Pattern = #mirrored_sup_childspec{key = {Group, '_'},
                                      _   = '_'},
    Conditions = [?KHEPRI_WILDCARD_STAR_STAR, #if_data_matches{pattern = Pattern}],
    rabbit_khepri:delete(khepri_mirrored_supervisor_path() ++
                             [#if_all{conditions = Conditions}]).

write_in_mnesia(Group, Overall, ChildSpec, Id) ->
    S = #mirrored_sup_childspec{key           = {Group, Id},
                                mirroring_pid = Overall,
                                childspec     = ChildSpec},
    ok = mnesia:write(?TABLE, S, write),
    ChildSpec.

delete_in_mnesia(Group, Id) ->
    ok = mnesia:delete({?TABLE, {Group, Id}}).

khepri_mirrored_supervisor_path() ->
    [?MODULE, mirrored_supervisor_childspec].

khepri_mirrored_supervisor_path(Group) ->
    [?MODULE, mirrored_supervisor_childspec, Group].

khepri_mirrored_supervisor_path(Group, Id) ->
    [?MODULE, mirrored_supervisor_childspec, Group] ++ Id.
