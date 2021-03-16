-module(gunner_pool_loose_SUITE).

-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-type test_case_name() :: atom().
-type group_name() :: atom().
-type config() :: [{atom(), term()}].
-type test_return() :: _ | no_return().

-export([basic_lifetime_ok_test/1]).
-export([connection_failed_test/1]).
-export([different_connections_from_same_group_test/1]).
-export([different_connections_from_different_groups_test/1]).

-export([pool_resizing_test/1]).

-define(POOL_CLEANUP_INTERVAL, 100).
-define(POOL_MAX_CONNECTION_LOAD, 1).
-define(POOL_MAX_CONNECTION_IDLE_AGE, 200).
-define(POOL_MAX_SIZE, 25).
-define(POOL_MIN_SIZE, 5).

-define(POOL_ID_PROP, pool_id).
-define(POOL_ID(C), proplists:get_value(?POOL_ID_PROP, C)).

-define(COWBOY_HANDLER_MAX_SLEEP_DURATION, 1000).

-define(H_CLEANUP_TIMEOUT, ?POOL_MAX_CONNECTION_IDLE_AGE + ?POOL_CLEANUP_INTERVAL * 2).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, multipool_tests}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {multipool_tests, [parallel], [
            {group, single_pool_tests},
            {group, single_pool_tests},
            {group, single_pool_tests}
        ]},
        {single_pool_tests, [sequence], [
            {group, single_pool_group_tests},
            pool_resizing_test,
            {group, single_pool_group_tests}
        ]},
        {single_pool_group_tests, [shuffle, {repeat, 3}], [
            basic_lifetime_ok_test,
            connection_failed_test,
            different_connections_from_same_group_test,
            different_connections_from_different_groups_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    C ++ [{apps, [App || {ok, App} <- Apps]}].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, C),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(TestGroupName, C) when
    TestGroupName =:= single_pool_tests; TestGroupName =:= multiple_pool_group_tests
->
    {ok, PoolPid} = gunner:start_pool(#{
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_load => ?POOL_MAX_CONNECTION_LOAD,
        max_connection_idle_age => ?POOL_MAX_CONNECTION_IDLE_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE
    }),
    C ++ [{?POOL_ID_PROP, PoolPid}];
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(TestGroupName, C) when TestGroupName =:= single_pool_tests; TestGroupName =:= multiple_pool_group_tests ->
    ok = gunner:stop_pool(?POOL_ID(C));
end_per_group(_, _C) ->
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec basic_lifetime_ok_test(config()) -> test_return().
basic_lifetime_ok_test(C) ->
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    {ok, <<"ok/", Tag/binary>>} = get(
        ?POOL_ID(C),
        valid_host(),
        <<"/", Tag/binary>>,
        ?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2
    ).

-spec connection_failed_test(config()) -> test_return().
connection_failed_test(C) ->
    {error, {connection_failed, {shutdown, nxdomain}}} = get_request(?POOL_ID(C), {"localghost", 8080}, <<"/">>, 1000),
    {error, {connection_failed, {shutdown, econnrefused}}} = get_request(
        ?POOL_ID(C),
        {"localhost", 8090},
        <<"/">>,
        1000
    ).

-spec different_connections_from_same_group_test(config()) -> test_return().
different_connections_from_same_group_test(C) ->
    Host = valid_host(),
    {ok, {ConnectionPid1, _}} = get_request(?POOL_ID(C), Host, <<"/">>, 1000),
    %% Because of how pool is implemented, there is no guarantee that (almost) simultaneous requests
    %% to small pools and same connection groups will actually get different connection processes
    _ = timer:sleep(100),
    {ok, {ConnectionPid2, _}} = get_request(?POOL_ID(C), Host, <<"/">>, 1000),
    ?assertNotEqual(ConnectionPid1, ConnectionPid2).

-spec different_connections_from_different_groups_test(config()) -> test_return().
different_connections_from_different_groups_test(C) ->
    {ok, {ConnectionPid1, _}} = get_request(?POOL_ID(C), {"localhost", 8080}, <<"/">>, 1000),
    {ok, {ConnectionPid2, _}} = get_request(?POOL_ID(C), {"localhost", 8086}, <<"/">>, 1000),
    ?assertNotEqual(ConnectionPid1, ConnectionPid2).

-spec pool_resizing_test(config()) -> test_return().
pool_resizing_test(C) ->
    _ = lists:foreach(
        fun(_) ->
            case get_request(?POOL_ID(C), valid_host(), <<"/">>, 1000) of
                {ok, _} ->
                    ok;
                {error, pool_unavailable} ->
                    ok
            end
        end,
        lists:seq(0, ?POOL_MAX_SIZE * 2)
    ),
    ?assertMatch({ok, #{total_connections := ?POOL_MAX_SIZE}}, gunner:pool_status(?POOL_ID(C))),
    _ = timer:sleep(
        (?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2) + ?H_CLEANUP_TIMEOUT
    ),
    ?assertMatch({ok, #{total_connections := ?POOL_MIN_SIZE}}, gunner:pool_status(?POOL_ID(C))).

%%

get_request(PoolID, Endpoint, Path, Timeout) ->
    case gunner_pool:acquire(PoolID, Endpoint, false, Timeout) of
        {ok, ConnectionPid} ->
            StreamRef = gun:request(ConnectionPid, <<"GET">>, Path, [], <<>>, #{}),
            {ok, {ConnectionPid, StreamRef}};
        {error, _} = Error ->
            Error
    end.

get(PoolID, Endpoint, Path, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    {ok, {ConnectionPid, StreamRef}} = get_request(PoolID, Endpoint, Path, Timeout),
    TimeoutLeft1 = Deadline - erlang:monotonic_time(millisecond),
    case gun:await(ConnectionPid, StreamRef, TimeoutLeft1) of
        {response, nofin, 200, _Headers} ->
            TimeoutLeft2 = Deadline - erlang:monotonic_time(millisecond),
            case gun:await_body(ConnectionPid, StreamRef, TimeoutLeft2) of
                {ok, Response, _Trailers} ->
                    {ok, Response};
                {ok, Response} ->
                    {ok, Response};
                {error, Reason} ->
                    {error, {unknown, Reason}}
            end;
        {response, fin, 404, _Headers} ->
            {error, notfound};
        {error, Reason} ->
            {error, {unknown, Reason}}
    end.

%%

valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086},
        {"localhost", 8087}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).

start_mock_server() ->
    start_mock_server(fun(#{path := Path}) ->
        _ = timer:sleep(200 + rand:uniform(?COWBOY_HANDLER_MAX_SLEEP_DURATION)),
        {200, #{}, <<"ok", Path/binary>>}
    end).

start_mock_server(HandlerFun) ->
    Opts = #{request_timeout => infinity},
    _ = mock_http_server:start(default, 8080, HandlerFun, Opts),
    _ = mock_http_server:start(alternative_1, 8086, HandlerFun, Opts),
    _ = mock_http_server:start(alternative_2, 8087, HandlerFun, Opts),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative_1),
    ok = mock_http_server:stop(alternative_2).
