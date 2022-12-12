%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, broker_tests}
    ].

groups() ->
    [
     {broker_tests, [], [
                         build_key_from_topic_trie_binding_record,
                         build_key_from_deletion_events,
                         build_key_from_binding_deletion_event,
                         build_multiple_key_from_deletion_events
                        ]}
    ].

%% follow back key to root through #topic_trie_edge deletion events

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    {ok, X} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, lookup, [XName]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{exchange_name, XName},
                                                    {exchange, X}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    Exchange = ?config(exchange, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_exchange_type_topic, delete, [none, Exchange]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

build_key_from_topic_trie_binding_record(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_topic_trie_binding_record1, [Config]).

build_key_from_topic_trie_binding_record1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    ok = rabbit_exchange_type_topic:add_binding(none, X, #binding{source = XName,
                                                                  destination = QName,
                                                                  key = RK,
                                                                  args = []}),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    [TopicTrieBinding] = ets:tab2list(rabbit_topic_trie_binding),
    ?assertEqual(SplitRK, rabbit_db_topic_exchange:trie_binding_to_key(TopicTrieBinding)),
    passed.

build_key_from_deletion_events(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_deletion_events1, [Config]).

build_key_from_deletion_events1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    Binding = #binding{source = XName,
                       destination = QName,
                       key = RK,
                       args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:remove_bindings(none, X, [Binding]),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    ?assertMatch([{_, SplitRK}],
                 rabbit_db_topic_exchange:trie_records_to_key(Records)),
    passed.

build_key_from_binding_deletion_event(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_binding_deletion_event1, [Config]).

build_key_from_binding_deletion_event1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    Binding0 = #binding{source = XName,
                        destination = QName,
                        key = RK,
                        args = [some_args]},
    Binding = #binding{source = XName,
                       destination = QName,
                       key = RK,
                       args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding0),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:remove_bindings(none, X, [Binding]),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    ?assertMatch([{_, SplitRK}],
                 rabbit_db_topic_exchange:trie_records_to_key(Records)),
    passed.

build_multiple_key_from_deletion_events(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_multiple_key_from_deletion_events1, [Config]).

build_multiple_key_from_deletion_events1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK0 = <<"a.b.c.d.e.f">>,
    RK1 = <<"a.b.c.d">>,
    RK2 = <<"a.b.c.g.e.f">>,
    RK3 = <<"hare.rabbit.ho">>,
    Binding0 = #binding{source = XName, destination = QName, key = RK0, args = []},
    Binding1 = #binding{source = XName, destination = QName, key = RK1, args = []},
    Binding2 = #binding{source = XName, destination = QName, key = RK2, args = []},
    Binding3 = #binding{source = XName, destination = QName, key = RK3, args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding0),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding1),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding2),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding3),
    SplitRK0 = rabbit_db_topic_exchange:split_topic_key(RK0),
    SplitRK1 = rabbit_db_topic_exchange:split_topic_key(RK1),
    SplitRK2 = rabbit_db_topic_exchange:split_topic_key(RK2),
    SplitRK3 = rabbit_db_topic_exchange:split_topic_key(RK3),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:delete(none, X),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    RKs = lists:sort([SplitRK0, SplitRK1, SplitRK2, SplitRK3]),
    ?assertMatch(
       RKs,
       lists:sort([RK || {_, RK} <- rabbit_db_topic_exchange:trie_records_to_key(Records)])),
    passed.

subscribe_to_mnesia_changes([Table | Rest]) ->
    case mnesia:subscribe({table, Table, detailed}) of
        {ok, _} -> subscribe_to_mnesia_changes(Rest);
        Error -> Error
    end;
subscribe_to_mnesia_changes([]) ->
    ok.

unsubscribe_to_mnesia_changes([Table | Rest]) ->
    case mnesia:unsubscribe({table, Table, detailed}) of
        {ok, _} -> unsubscribe_to_mnesia_changes(Rest);
        Error   -> Error
    end;
unsubscribe_to_mnesia_changes([]) ->
    ok.

receive_delete_events(Num) ->
    receive_delete_events(Num, []).

receive_delete_events(0, Evts) ->
    receive
        {mnesia_table_event, {delete, _, Record, _, _}} ->
            receive_delete_events(0, [Record | Evts])
    after 0 ->
            Evts
    end;    
receive_delete_events(N, Evts) ->
    receive
        {mnesia_table_event, {delete, _, Record, _, _}} ->
            receive_delete_events(N - 1, [Record | Evts])
    after 10000 ->
            Evts
    end.
