-module(direct_exchange_routing_v2_SUITE).

%% Test suite for the feature flag direct_exchange_routing_v2

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(FEATURE_FLAG, direct_exchange_routing_v2).
-define(INDEX_TABLE_NAME, rabbit_index_route).

%% in file direct_exchange_routing_v2_SUITE_data/definition.json:
%% number of bindings where the source exchange is a direct exchange
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE, 620).
%% number of bindings where the source exchange is a direct exchange
%% and both source exchange and destination queue are durable
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, 220).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_2},
     {group, unclustered_cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [remove_binding_unbind_queue,
       remove_binding_delete_queue,
       remove_binding_delete_queue_multiple,
       remove_binding_delete_exchange,
       recover_bindings,
       route_exchange_to_exchange,
       reset]},
     {cluster_size_2, [],
      [remove_binding_node_down_transient_queue,
       keep_binding_node_down_durable_queue]},
     {unclustered_cluster_size_2, [],
      [join_cluster]}
    ].

suite() ->
    [
     %% If a test hangs, no need to wait for 30 minutes.
     {timetrap, {minutes, 8}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1}),
    start_broker(Group, Config1);
init_per_group(cluster_size_2 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 2}),
    start_broker(Group, Config1);
init_per_group(unclustered_cluster_size_2 = Group, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(Config0,
                                           [{rmq_nodes_count, 2},
                                            {rmq_nodes_clustered, false}]),
    start_broker(Group, Config1).

start_broker(Group, Config0) ->
    Size = rabbit_ct_helpers:get_config(Config0, rmq_nodes_count),
    Clustered = rabbit_ct_helpers:get_config(Config0, rmq_nodes_clustered, true),
    Config = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix,
                                                    io_lib:format("cluster_size_~b-clustered_~tp-~ts",
                                                                  [Size, Clustered, Group])}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    %% Test that all bindings got removed from the database.
    ?assertEqual([0,0,0,0,0],
                 lists:map(fun(Table) ->
                                   table_size(Config, Table)
                           end, [rabbit_durable_route,
                                 rabbit_semi_durable_route,
                                 rabbit_route,
                                 rabbit_reverse_route,
                                 ?INDEX_TABLE_NAME])
                ).

%%%===================================================================
%%% Test cases
%%%===================================================================

remove_binding_unbind_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    unbind_queue(Ch, Q, X, RKey),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    delete_queue(Ch, Q),
    ok.

remove_binding_delete_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

remove_binding_delete_queue_multiple(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q1, false),
    bind_queue(Ch, Q1, X, RKey),
    bind_queue(Ch, Q1, <<"amq.fanout">>, RKey),
    declare_queue(Ch, Q2, true),
    bind_queue(Ch, Q2, X, RKey),

    %% Table rabbit_index_route stores only bindings
    %% where the source exchange is a direct exchange.
    ?assertEqual(2, table_size(Config, ?INDEX_TABLE_NAME)),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q1),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q2),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

remove_binding_delete_exchange(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    DirectX = <<"x1">>,
    FanoutX = <<"x2">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_exchange(Ch, DirectX, <<"direct">>),
    declare_exchange(Ch, FanoutX, <<"fanout">>),
    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, DirectX, RKey),
    bind_queue(Ch, Q, FanoutX, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    %% Table rabbit_index_route stores only bindings
    %% where the source exchange is a direct exchange.
    delete_exchange(Ch, FanoutX),
    assert_index_table_non_empty(Config),

    delete_exchange(Ch, DirectX),
    assert_index_table_empty(Config),
    delete_queue(Ch, Q),
    ok.

remove_binding_node_down_transient_queue(Config) ->
    [_Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    %% transient classic queue on Server2
    declare_queue(Ch2, Q, false),
    bind_queue(Ch1, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% We expect no route to transient classic queue when its host node is down.
    assert_index_table_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    %% We expect route to NOT come back when transient queue's host node comes back.
    assert_index_table_empty(Config),
    delete_queue(Ch1, Q),
    ok.

keep_binding_node_down_durable_queue(Config) ->
    [_Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    %% durable classic queue on Server2
    declare_queue(Ch2, Q, true),
    bind_queue(Ch1, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% When the durable classic queue's host node is down,
    %% we expect that queue and its bindings still to exist
    %% (see https://github.com/rabbitmq/rabbitmq-server/pull/4563).
    assert_index_table_non_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    assert_index_table_non_empty(Config),
    delete_queue(Ch1, Q),
    ok.

recover_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Path = filename:join([?config(data_dir, Config), "definition.json"]),

    assert_index_table_empty(Config),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Server, ["import_definitions", Path], 10_000),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE, table_size(Config, ?INDEX_TABLE_NAME)),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, table_size(Config, ?INDEX_TABLE_NAME)),

    %% cleanup
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    delete_queue(Ch, <<"durable-q">>),
    ok.

%% Test that routing from a direct exchange to a fanout exchange works.
route_exchange_to_exchange(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    DirectX = <<"amq.direct">>,
    FanoutX = <<"amq.fanout">>,
    RKey = <<"k">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,

    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = FanoutX,
                                                                   source = DirectX,
                                                                   routing_key = RKey}),
    declare_queue(Ch, Q1, true),
    declare_queue(Ch, Q2, false),
    bind_queue(Ch, Q1, FanoutX, <<"ignored">>),
    bind_queue(Ch, Q2, FanoutX, <<"ignored">>),

    publish(Ch, DirectX, RKey),
    quorum_queue_utils:wait_for_messages(Config, [[Q1, <<"1">>, <<"1">>, <<"0">>]]),
    quorum_queue_utils:wait_for_messages(Config, [[Q2, <<"1">>, <<"1">>, <<"0">>]]),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME)),

    %% cleanup
    delete_queue(Ch, Q1),
    delete_queue(Ch, Q2),
    #'exchange.unbind_ok'{} = amqp_channel:call(Ch, #'exchange.unbind'{destination = FanoutX,
                                                                       source = DirectX,
                                                                       routing_key = RKey}),
    ok.

reset(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    ?assertEqual([Server], index_table_ram_copies(Config, 0)),
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(reset, Server),
    ok = rabbit_control_helper:command(start_app, Server),
    %% After reset, upon node boot, we expect that the table gets re-created.
    ?assertEqual([Server], index_table_ram_copies(Config, 0)).

join_cluster(Config) ->
    Servers0 = [Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Servers = lists:sort(Servers0),

    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server1),
    DirectX = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch1, Q, true),
    bind_queue(Ch1, Q, DirectX, RKey),

    %% Server1 and Server2 are not clustered yet.
    %% Hence, every node has their own table (copy) and only Server1's table contains the binding.
    ?assertEqual([Server1], index_table_ram_copies(Config, Server1)),
    ?assertEqual([Server2], index_table_ram_copies(Config, Server2)),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME, Server1)),
    ?assertEqual(0, table_size(Config, ?INDEX_TABLE_NAME, Server2)),

    ok = rabbit_control_helper:command(stop_app, Server2),
    %% For the purpose of this test it shouldn't matter whether Server2 is reset. Both should work.
    case erlang:system_time() rem 2 of
        0 ->
            ok = rabbit_control_helper:command(reset, Server2);
        1 ->
            ok
    end,
    ok = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server1)], []),
    ok = rabbit_control_helper:command(start_app, Server2),

    %% After Server2 joined Server1, the table should be clustered.
    ?assertEqual(Servers, index_table_ram_copies(Config, Server2)),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME, Server2)),

    %% Publishing via Server1 via "direct exchange routing v2" should work.
    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),
    publish(Ch1, DirectX, RKey),
    assert_confirm(),

    %% Publishing via Server2 via "direct exchange routing v2" should work.
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server2),
    amqp_channel:call(Ch2, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch2, self()),
    publish(Ch2, DirectX, RKey),
    assert_confirm(),
    delete_queue(Ch1, Q),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

declare_queue(Ch, QName, Durable) ->
    #'queue.declare_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                                    durable = Durable
                                                                                   }).

delete_queue(Ch, QName) ->
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

bind_queue(Ch, Queue, Exchange, RoutingKey) ->
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue       = Queue,
                                                             exchange    = Exchange,
                                                             routing_key = RoutingKey}).

unbind_queue(Ch, Queue, Exchange, RoutingKey) ->
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{queue       = Queue,
                                                                 exchange    = Exchange,
                                                                 routing_key = RoutingKey}).

declare_exchange(Ch, XName, Type) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = XName,
                                                                         type = Type}).

delete_exchange(Ch, XName) ->
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}).

publish(Ch, Exchange, RoutingKey) ->
    ok = amqp_channel:call(Ch, #'basic.publish'{exchange = Exchange,
                                                routing_key = RoutingKey,
                                                mandatory = true},
                           #amqp_msg{}).

assert_confirm() ->
    receive
        #'basic.ack'{} ->
            ok
    after 5000 ->
              throw(missing_confirm)
    end.

assert_return() ->
    receive
        {#'basic.return'{}, _} ->
            ok
    after 5000 ->
              throw(missing_return)
    end.

assert_index_table_empty(Config) ->
    ?awaitMatch(0, table_size(Config, ?INDEX_TABLE_NAME), 3000).

assert_index_table_non_empty(Config) ->
    ?assertNotEqual(0, table_size(Config, ?INDEX_TABLE_NAME)).

assert_no_index_table(Config) ->
    Tids = rabbit_ct_broker_helpers:rpc_all(Config, ets, whereis, [?INDEX_TABLE_NAME]),
    ?assert(lists:all(fun(Tid) -> Tid =:= undefined end, Tids)).

index_table_ram_copies(Config, Node) ->
    RamCopies = rabbit_ct_broker_helpers:rpc(Config, Node, mnesia, table_info,
                                             [?INDEX_TABLE_NAME, ram_copies]),
    lists:sort(RamCopies).

table_size(Config, Table) ->
    table_size(Config, Table, 0).

table_size(Config, Table, Server) ->
    %% Do not use
    %% mnesia:table_info(Table, size)
    %% as this returns 0 if the table doesn't exist.
    rabbit_ct_broker_helpers:rpc(Config, Server, ets, info, [Table, size], 5000).
