load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = ["ebin/amqp10_client.beam", "ebin/amqp10_client_app.beam", "ebin/amqp10_client_connection.beam", "ebin/amqp10_client_connection_sup.beam", "ebin/amqp10_client_connections_sup.beam", "ebin/amqp10_client_frame_reader.beam", "ebin/amqp10_client_session.beam", "ebin/amqp10_client_sessions_sup.beam", "ebin/amqp10_client_sup.beam", "ebin/amqp10_client_types.beam", "ebin/amqp10_msg.beam"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_app_beam",
        srcs = ["src/amqp10_client_app.erl"],
        outs = ["ebin/amqp10_client_app.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_beam",
        srcs = ["src/amqp10_client.erl"],
        outs = ["ebin/amqp10_client.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_connection_beam",
        srcs = ["src/amqp10_client_connection.erl"],
        outs = ["ebin/amqp10_client_connection.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_connection_sup_beam",
        srcs = ["src/amqp10_client_connection_sup.erl"],
        outs = ["ebin/amqp10_client_connection_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_connections_sup_beam",
        srcs = ["src/amqp10_client_connections_sup.erl"],
        outs = ["ebin/amqp10_client_connections_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_frame_reader_beam",
        srcs = ["src/amqp10_client_frame_reader.erl"],
        outs = ["ebin/amqp10_client_frame_reader.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_session_beam",
        srcs = ["src/amqp10_client_session.erl"],
        outs = ["ebin/amqp10_client_session.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_sessions_sup_beam",
        srcs = ["src/amqp10_client_sessions_sup.erl"],
        outs = ["ebin/amqp10_client_sessions_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_sup_beam",
        srcs = ["src/amqp10_client_sup.erl"],
        outs = ["ebin/amqp10_client_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_amqp10_client_types_beam",
        srcs = ["src/amqp10_client_types.erl"],
        outs = ["ebin/amqp10_client_types.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_amqp10_msg_beam",
        srcs = ["src/amqp10_msg.erl"],
        outs = ["ebin/amqp10_msg.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )

def all_test_beam_files(name = "all_test_beam_files"):
    erlang_bytecode(
        name = "test_amqp10_client_app_beam",
        testonly = True,
        srcs = ["src/amqp10_client_app.erl"],
        outs = ["test/amqp10_client_app.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_amqp10_client_beam",
        testonly = True,
        srcs = ["src/amqp10_client.erl"],
        outs = ["test/amqp10_client.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_amqp10_client_connection_beam",
        testonly = True,
        srcs = ["src/amqp10_client_connection.erl"],
        outs = ["test/amqp10_client_connection.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_amqp10_client_connection_sup_beam",
        testonly = True,
        srcs = ["src/amqp10_client_connection_sup.erl"],
        outs = ["test/amqp10_client_connection_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_amqp10_client_connections_sup_beam",
        testonly = True,
        srcs = ["src/amqp10_client_connections_sup.erl"],
        outs = ["test/amqp10_client_connections_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_amqp10_client_frame_reader_beam",
        testonly = True,
        srcs = ["src/amqp10_client_frame_reader.erl"],
        outs = ["test/amqp10_client_frame_reader.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_amqp10_client_session_beam",
        testonly = True,
        srcs = ["src/amqp10_client_session.erl"],
        outs = ["test/amqp10_client_session.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_amqp10_client_sessions_sup_beam",
        testonly = True,
        srcs = ["src/amqp10_client_sessions_sup.erl"],
        outs = ["test/amqp10_client_sessions_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_amqp10_client_sup_beam",
        testonly = True,
        srcs = ["src/amqp10_client_sup.erl"],
        outs = ["test/amqp10_client_sup.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_amqp10_client_types_beam",
        testonly = True,
        srcs = ["src/amqp10_client_types.erl"],
        outs = ["test/amqp10_client_types.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_amqp10_msg_beam",
        testonly = True,
        srcs = ["src/amqp10_msg.erl"],
        outs = ["test/amqp10_msg.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = ["test/amqp10_client.beam", "test/amqp10_client_app.beam", "test/amqp10_client_connection.beam", "test/amqp10_client_connection_sup.beam", "test/amqp10_client_connections_sup.beam", "test/amqp10_client_frame_reader.beam", "test/amqp10_client_session.beam", "test/amqp10_client_sessions_sup.beam", "test/amqp10_client_sup.beam", "test/amqp10_client_types.beam", "test/amqp10_msg.beam"],
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "all_srcs",
        srcs = [":public_and_private_hdrs", ":srcs"],
    )
    filegroup(
        name = "public_and_private_hdrs",
        srcs = [":private_hdrs", ":public_hdrs"],
    )
    filegroup(
        name = "priv",
        srcs = [],
    )
    filegroup(
        name = "licenses",
        srcs = ["LICENSE", "LICENSE-MPL-RabbitMQ"],
    )
    filegroup(
        name = "srcs",
        srcs = ["src/amqp10_client.erl", "src/amqp10_client_app.erl", "src/amqp10_client_connection.erl", "src/amqp10_client_connection_sup.erl", "src/amqp10_client_connections_sup.erl", "src/amqp10_client_frame_reader.erl", "src/amqp10_client_session.erl", "src/amqp10_client_sessions_sup.erl", "src/amqp10_client_sup.erl", "src/amqp10_client_types.erl", "src/amqp10_msg.erl"],
    )
    filegroup(
        name = "private_hdrs",
        srcs = ["src/amqp10_client.hrl"],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "msg_SUITE_beam_files",
        testonly = True,
        srcs = ["test/msg_SUITE.erl"],
        outs = ["test/msg_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_activemq_ct_helpers_beam",
        testonly = True,
        srcs = ["test/activemq_ct_helpers.erl"],
        outs = ["test/activemq_ct_helpers.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_mock_server_beam",
        testonly = True,
        srcs = ["test/mock_server.erl"],
        outs = ["test/mock_server.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
