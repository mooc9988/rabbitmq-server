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

-export([init/0, sync/0]).
-export([set_migration_flag/1, is_migration_done/1]).

%% Exported to be used by various rabbit_db_* modules
-export([
         list_in_mnesia/2,
         list_in_khepri/1,
         list_in_khepri/2,
         retry/1,
         if_has_data/1,
         if_has_data_wildcard/0
        ]).

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

if_has_data_wildcard() ->
    if_has_data([?KHEPRI_WILDCARD_STAR_STAR]).

if_has_data(Conditions) ->
    #if_all{conditions = Conditions ++ [#if_has_data{has_data = true}]}.

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
