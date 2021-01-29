-module(gunner_pool_locking_SUITE).

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

-export([acquire_free_ok_test/1]).
-export([cant_free_multiple_times/1]).
-export([auto_free_on_client_death_test/1]).
-export([connection_uniqueness_test/1]).
-export([connection_reuse_test/1]).
-export([strict_connection_ownership_test/1]).
-export([pool_limits_test/1]).
-export([failed_connection_test/1]).
-export([connection_died_in_use/1]).
-export([connection_died_in_pool/1]).

-export([cancel_acquire_test/1]).
-export([pool_group_isolation_test/1]).
-export([pool_group_shared_free_limit_test/1]).

-define(POOL_ID_PROP, pool_id).
-define(POOL_ID(C), proplists:get_value(?POOL_ID_PROP, C)).

-define(POOL_MIN_SIZE, 2).
-define(POOL_MAX_SIZE, 25).

-define(POOL_CLEANUP_INTERVAL, 100).
-define(POOL_MAX_AGE, 200).

-define(H_CLEANUP_TIMEOUT, ?POOL_MAX_AGE + ?POOL_CLEANUP_INTERVAL * 2).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, multipool_tests},
        {group, multiple_pool_group_tests}
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
            {group, single_pool_group_tests}
        ]},
        {single_pool_group_tests, [shuffle, {repeat, 3}], [
            acquire_free_ok_test,
            connection_uniqueness_test,
            connection_reuse_test,
            cant_free_multiple_times,
            strict_connection_ownership_test,
            auto_free_on_client_death_test,
            pool_limits_test,
            failed_connection_test,
            connection_died_in_use,
            connection_died_in_pool
        ]},
        {multiple_pool_group_tests, [sequence], [
            cancel_acquire_test,
            pool_group_isolation_test,
            pool_group_shared_free_limit_test
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
    {ok, Pid} = gunner:start_pool(#{
        mode => locking,
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_idle_age => ?POOL_MAX_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE
    }),
    C ++ [{?POOL_ID_PROP, Pid}];
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
    _ = wait_pool_cleanup(),
    C.

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec acquire_free_ok_test(config()) -> test_return().
acquire_free_ok_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    %% Initial connection creation
    {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire]),
    ok = gunner_pool:free(?POOL_ID(C), Connection, 1000),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, free]).

-spec failed_connection_test(config()) -> test_return().
failed_connection_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    {error, {connection_failed, Reason1}} = gunner_pool:acquire(?POOL_ID(C), {"localghost", 8080}, 1000),
    ?assert(lists:member(Reason1, [{shutdown, nxdomain}, unknown])),
    {error, {connection_failed, Reason2}} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8081}, 1000),
    ?assert(lists:member(Reason2, [{shutdown, econnrefused}, unknown])),
    ok = assert_counters(?POOL_ID(C), Counters, []).

-spec connection_died_in_use(config()) -> test_return().
connection_died_in_use(C) ->
    Counters = get_counters(?POOL_ID(C)),
    {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
    ok = gun:close(Connection),
    ?assertEqual(
        {error, connection_not_found},
        gunner_pool:free(?POOL_ID(C), Connection, 1000)
    ),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, busy_down]).

-spec connection_died_in_pool(config()) -> test_return().
connection_died_in_pool(C) ->
    Counters = get_counters(?POOL_ID(C)),
    {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
    ok = gunner_pool:free(?POOL_ID(C), Connection, 1000),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, free]),
    ok = gun:close(Connection),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, free, free_down]).

-spec connection_uniqueness_test(config()) -> test_return().
connection_uniqueness_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    {ok, Connection1} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
    {ok, Connection2} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
    ?assertNotEqual(Connection1, Connection2),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
    ok = gunner_pool:free(?POOL_ID(C), Connection2, 1000),
    ok = gunner_pool:free(?POOL_ID(C), Connection1, 1000),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, {free, 2}]).

-spec connection_reuse_test(config()) -> test_return().
connection_reuse_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    _ = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {ok, Connection2} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        ?assertNotEqual(Connection1, Connection2),
        ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
        ok = gunner_pool:free(?POOL_ID(C), Connection2, 1000),
        ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, free]),
        {ok, Connection3} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        ?assertEqual(Connection3, Connection2),
        ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, free, acquire])
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, free, acquire, {free, 2}]).

-spec cant_free_multiple_times(config()) -> test_return().
cant_free_multiple_times(C) ->
    Counters = get_counters(?POOL_ID(C)),
    ok = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {ok, _Connection2} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
        ?assertEqual(ok, gunner_pool:free(?POOL_ID(C), Connection1, 1000)),
        ?assertEqual(
            {error, invalid_connection_state},
            gunner_pool:free(?POOL_ID(C), Connection1, 1000)
        ),
        ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, free])
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, {free, 2}]).

-spec strict_connection_ownership_test(config()) -> test_return().
strict_connection_ownership_test(C) ->
    _ = init_async_clients(),
    Counters = get_counters(?POOL_ID(C)),
    {ok, Connection1} = client_process_persistent(client1, fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {return, {ok, Connection}}
    end),
    {ok, Connection2} = client_process_persistent(client2, fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {return, {ok, Connection}}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
    ?assertNotEqual(Connection1, Connection2),
    ok = client_process_persistent(client1, fun() ->
        ?assertEqual(
            {error, invalid_connection_state},
            gunner_pool:free(?POOL_ID(C), Connection2, 1000)
        ),
        {return, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        ?assertEqual(
            {error, invalid_connection_state},
            gunner_pool:free(?POOL_ID(C), Connection1, 1000)
        ),
        {return, ok}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
    ok = client_process_persistent(client1, fun() ->
        ?assertEqual(ok, gunner_pool:free(?POOL_ID(C), Connection1, 1000)),
        {exit, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        ?assertEqual(ok, gunner_pool:free(?POOL_ID(C), Connection2, 1000)),
        {exit, ok}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, {free, 2}]).

-spec auto_free_on_client_death_test(config()) -> test_return().
auto_free_on_client_death_test(C) ->
    _ = init_async_clients(),
    Counters = get_counters(?POOL_ID(C)),
    ok = client_process_persistent(client1, fun() ->
        _ = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {return, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        _ = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        {return, ok}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}]),
    ok = client_process_persistent(client1, fun() ->
        {exit, ok}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, free]),
    ok = client_process_persistent(client2, fun() ->
        {exit, ok}
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, 2}, {free, 2}]).

-spec pool_limits_test(config()) -> test_return().
pool_limits_test(C) ->
    _ = init_async_clients(),
    Counters = get_counters(?POOL_ID(C)),
    RemainingCapacity = ?POOL_MAX_SIZE,
    {ok, Processes1} = spawn_connections(RemainingCapacity, ?POOL_ID(C)),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, RemainingCapacity}]),
    {error, pool_unavailable} = spawn_connections(1, ?POOL_ID(C)),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, RemainingCapacity + 1}]),
    ok = free_connections(Processes1, ?POOL_ID(C)),
    _ = wait_pool_cleanup(),
    ok = assert_counters(?POOL_ID(C), Counters, [{acquire, RemainingCapacity}, {free, RemainingCapacity}]).

spawn_connections(Amount, PoolID) ->
    spawn_connections(Amount, PoolID, []).

spawn_connections(0, _PoolID, Acc) ->
    {ok, Acc};
spawn_connections(Amount, PoolID, Acc) ->
    ClientID = {limit_test_client, Amount},
    Result = client_process_persistent(ClientID, fun() ->
        Result0 = gunner_pool:acquire(PoolID, {"localhost", 8080}, 1000),
        {return, Result0}
    end),
    case Result of
        {ok, Connection} ->
            spawn_connections(Amount - 1, PoolID, [{ClientID, Connection} | Acc]);
        {error, _} = Error ->
            Error
    end.

free_connections([], _PoolID) ->
    ok;
free_connections([{ClientID, Connection} | Rest], PoolID) ->
    Result = client_process_persistent(ClientID, fun() ->
        Result0 = gunner_pool:free(PoolID, Connection, 1000),
        {exit, Result0}
    end),
    case Result of
        ok ->
            free_connections(Rest, PoolID);
        {error, _} = Error ->
            Error
    end.

%%

-spec cancel_acquire_test(config()) -> test_return().
cancel_acquire_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    _ = client_process(fun() ->
        %% @TODO this is pretty dumb
        ?assertExit({timeout, _}, gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 0)),
        ok = assert_counters(?POOL_ID(C), Counters, [acquire])
    end),
    _ = wait_pool_cleanup(),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, free]).

-spec pool_group_isolation_test(config()) -> test_return().
pool_group_isolation_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        ok = gunner_pool:free(?POOL_ID(C), Connection, 1000)
    end),
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8088}, 1000),
        ok = gunner_pool:free(?POOL_ID(C), Connection, 1000)
    end),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, acquire, free, free]).

-spec pool_group_shared_free_limit_test(config()) -> test_return().
pool_group_shared_free_limit_test(C) ->
    Counters = get_counters(?POOL_ID(C)),
    %% This test relies on MIN_CONNECTIONS being 2
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8080}, 1000),
        ok = gunner_pool:free(?POOL_ID(C), Connection, 1000)
    end),
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8088}, 1000),
        ok = gunner_pool:free(?POOL_ID(C), Connection, 1000)
    end),
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_ID(C), {"localhost", 8089}, 1000),
        ok = gunner_pool:free(?POOL_ID(C), Connection, 1000)
    end),
    _ = wait_pool_cleanup(),
    ok = assert_counters(?POOL_ID(C), Counters, [acquire, acquire, acquire, free, free, free]),
    {2, _} = get_counters(?POOL_ID(C)).

%%

start_mock_server() ->
    start_mock_server(fun(_) ->
        {200, #{}, <<"ok">>}
    end).

start_mock_server(HandlerFun) ->
    _ = mock_http_server:start(default, 8080, HandlerFun),
    _ = mock_http_server:start(alternative_1, 8088, HandlerFun),
    _ = mock_http_server:start(alternative_2, 8089, HandlerFun),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative_1),
    ok = mock_http_server:stop(alternative_2).

%%

wait_pool_cleanup() ->
    _ = timer:sleep(?H_CLEANUP_TIMEOUT).

get_counters(PoolID) ->
    PoolStats = get_pool_stats(PoolID),
    InitialFree = get_pool_free_size(PoolStats),
    InitialTotal = get_pool_total_size(PoolStats),
    {InitialFree, InitialTotal}.

count_operations(Counters, {MaxFree, MaxTotal}, Operations) ->
    lists:foldl(
        fun
            (acquire, {Free, Total}) when Free =:= 0, MaxTotal > Total -> {Free, Total + 1};
            (acquire, {Free, Total}) when Free =:= 0, MaxTotal =< Total -> {Free, Total};
            (acquire, {Free, Total}) when Free > 0 -> {Free - 1, Total};
            (free, {Free, Total}) when MaxFree > Free -> {Free + 1, Total};
            (free, {Free, Total}) when MaxFree =< Free -> {Free, Total - 1};
            (busy_down, {Free, Total}) -> {Free, Total - 1};
            (free_down, {Free, Total}) -> {Free - 1, Total - 1}
        end,
        Counters,
        Operations
    ).

expand_operations(Operations) ->
    lists:flatmap(
        fun
            ({Operation, Amount}) -> lists:duplicate(Amount, Operation);
            (Operation) -> [Operation]
        end,
        Operations
    ).

assert_counters(PoolID, Counters, Operations) ->
    assert_counters(PoolID, Counters, Operations, {?POOL_MIN_SIZE, ?POOL_MAX_SIZE}).

assert_counters(PoolID, Counters, Operations, Limits) ->
    NewCounters = count_operations(Counters, Limits, expand_operations(Operations)),
    PoolStats = get_pool_stats(PoolID),
    case {get_pool_free_size(PoolStats), get_pool_total_size(PoolStats)} of
        NewCounters ->
            ok;
        InvalidCounters ->
            {error, {{original, Counters}, {calculated, NewCounters}, {actual, InvalidCounters}}}
    end.

%%

client_process_persistent(Name, Fun) ->
    client_process_persistent(Name, Fun, 1000).

client_process_persistent(Name, Fun, Timeout) ->
    Pid = find_or_create_process(Name),
    Pid ! {run, Fun},
    receive
        {result, Result} ->
            Result;
        {exit, Result} ->
            _ = delete_process(Name),
            Result;
        {caught, {Error, Reason, Stacktrace}} ->
            erlang:raise(Error, Reason, Stacktrace)
    after Timeout -> {error, timeout}
    end.

-define(WORKER_TAB, test_worker_pids).

init_async_clients() ->
    ets:new(?WORKER_TAB, [set, named_table]).

find_or_create_process(Name) ->
    case ets:lookup(?WORKER_TAB, Name) of
        [{_, Pid} | _] ->
            Pid;
        [] ->
            Pid = create_process(5000),
            _ = ets:insert(?WORKER_TAB, [{Name, Pid}]),
            Pid
    end.

create_process(Timeout) ->
    Self = self(),
    spawn_link(fun() -> async_client_process(Self, Timeout) end).

delete_process(Name) ->
    ets:delete(?WORKER_TAB, Name).

async_client_process(Parent, Timeout) ->
    receive
        {run, Fun} ->
            Result =
                try
                    Fun()
                catch
                    Error:Reason:Stacktrace ->
                        {caught, {Error, Reason, Stacktrace}}
                end,
            case Result of
                {return, Return} ->
                    Parent ! {result, Return},
                    async_client_process(Parent, Timeout);
                {exit, _} = Exit ->
                    Parent ! Exit;
                {caught, _} = Caught ->
                    Parent ! Caught,
                    async_client_process(Parent, Timeout)
            end
    after Timeout -> ok
    end.

client_process(Fun) ->
    client_process(Fun, 1000).

client_process(Fun, Timeout) ->
    Self = self(),
    _ = spawn_link(fun() ->
        Result =
            try
                {result, Fun()}
            catch
                Error:Reason:Stacktrace ->
                    {caught, {Error, Reason, Stacktrace}}
            end,
        Self ! Result
    end),
    receive
        {result, Result} ->
            Result;
        {caught, {Error, Reason, Stacktrace}} ->
            erlang:raise(Error, Reason, Stacktrace)
    after Timeout -> {error, timeout}
    end.

%%

get_pool_stats(PoolID) ->
    {ok, PoolStats} = gunner:pool_status(PoolID),
    PoolStats.

get_pool_total_size(#{total_connections := Size}) ->
    Size.

get_pool_free_size(#{available_connections := Size}) ->
    Size.
