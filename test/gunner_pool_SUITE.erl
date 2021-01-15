-module(gunner_pool_SUITE).

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

-export([pool_lifetime_test/1]).
-export([pool_already_exists/1]).
-export([pool_not_found/1]).

-export([acquire_free_ok_test/1]).
-export([mulitple_acquired_connections_for_process/1]).
-export([cant_free_multiple_times/1]).
-export([auto_free_on_client_death_test/1]).
-export([connection_allocation_test/1]).
-export([connection_reuse_test/1]).

-export([cancel_acquire_test/1]).

-export([strict_connection_ownership_test/1]).

-define(POOL_NAME_PROP, pool_name).
-define(POOL_NAME(C), proplists:get_value(?POOL_NAME_PROP, C)).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, pool_management},
        {group, multiple_pool_tests},
        {group, multiple_clients_tests},
        cancel_acquire_test
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {pool_management, [], [
            pool_lifetime_test,
            pool_already_exists,
            pool_not_found
        ]},
        {multiple_pool_tests, [parallel, {repeat, 2}], [
            {group, single_pool_tests},
            {group, single_pool_tests},
            {group, single_pool_tests}
        ]},
        {single_pool_tests, [sequence, shuffle], [
            acquire_free_ok_test,
            connection_allocation_test,
            connection_reuse_test,
            mulitple_acquired_connections_for_process,
            cant_free_multiple_times
        ]},
        {multiple_clients_tests, [], [
            strict_connection_ownership_test,
            auto_free_on_client_death_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(fun() ->
        {200, #{}, <<"ok">>}
    end),
    C ++ [{apps, [App || {ok, App} <- Apps]}].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, C),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(TestCaseName, C) when TestCaseName =:= single_pool_tests; TestCaseName =:= multiple_clients_tests ->
    PoolName = {pool, erlang:unique_integer()},
    ok = gunner:start_pool(PoolName, #{}),
    C ++ [{?POOL_NAME_PROP, PoolName}];
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(TestCaseName, C) when TestCaseName =:= single_pool_tests; TestCaseName =:= multiple_clients_tests ->
    ok = gunner:stop_pool(?POOL_NAME(C));
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

-spec pool_lifetime_test(config()) -> test_return().
pool_lifetime_test(_C) ->
    ?assertEqual(ok, gunner_pool:start_pool(default, #{})),
    ?assertMatch({ok, _}, gunner_pool:pool_status(default, 1000)),
    ?assertEqual(ok, gunner_pool:stop_pool(default)).

-spec pool_already_exists(config()) -> test_return().
pool_already_exists(_C) ->
    ?assertEqual(ok, gunner_pool:start_pool(default, #{})),
    ?assertEqual({error, already_exists}, gunner_pool:start_pool(default, #{})),
    ?assertEqual(ok, gunner_pool:stop_pool(default)).

-spec pool_not_found(config()) -> test_return().
pool_not_found(_C) ->
    ?assertEqual({error, pool_not_found}, gunner_pool:pool_status(default, 1000)),
    ?assertEqual({error, not_found}, gunner_pool:stop_pool(default)).

%%

-spec acquire_free_ok_test(config()) -> test_return().
acquire_free_ok_test(C) ->
    Ticket = make_ref(),
    Counters = init_counter_state(?POOL_NAME(C)),
    %% Initial connection creation
    ok = client_process(fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire]),
        ok = gunner_pool:free(?POOL_NAME(C), Connection, 1000)
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, free]).

-spec connection_allocation_test(config()) -> test_return().
connection_allocation_test(C) ->
    Ticket = make_ref(),
    Counters = init_counter_state(?POOL_NAME(C)),
    _ = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        {ok, Connection2} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ?assertNotEqual(Connection1, Connection2),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire])
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free]).

-spec connection_reuse_test(config()) -> test_return().
connection_reuse_test(C) ->
    Ticket = make_ref(),
    Counters = init_counter_state(?POOL_NAME(C)),
    _ = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        {ok, Connection2} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ?assertNotEqual(Connection1, Connection2),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
        ok = gunner_pool:free(?POOL_NAME(C), Connection2, 1000),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free]),
        {ok, Connection3} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ?assertEqual(Connection3, Connection2),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, acquire])
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, acquire, free, free]).

-spec mulitple_acquired_connections_for_process(config()) -> test_return().
mulitple_acquired_connections_for_process(C) ->
    Ticket = make_ref(),
    Counters = init_counter_state(?POOL_NAME(C)),
    ok = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        {ok, Connection2} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
        ok = gunner_pool:free(?POOL_NAME(C), Connection2, 1000),
        ok = gunner_pool:free(?POOL_NAME(C), Connection1, 1000),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free])
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free]).

-spec cant_free_multiple_times(config()) -> test_return().
cant_free_multiple_times(C) ->
    Ticket = make_ref(),
    Counters = init_counter_state(?POOL_NAME(C)),
    ok = client_process(fun() ->
        {ok, Connection1} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        {ok, _Connection2} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, Ticket, 1000),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
        ?assertEqual(ok, gunner_pool:free(?POOL_NAME(C), Connection1, 1000)),
        ?assertEqual(
            {error, {lease_return_failed, connection_not_found}},
            gunner_pool:free(?POOL_NAME(C), Connection1, 1000)
        ),
        ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free])
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free]).

-spec cancel_acquire_test(config()) -> test_return().
cancel_acquire_test(_C) ->
    PoolID = {pool, cancel_acquire_test},
    ok = gunner:start_pool(PoolID, #{}),
    Ticket = make_ref(),
    Counters = init_counter_state(PoolID),
    _ = client_process(fun() ->
        %% @TODO this is pretty dumb
        ?assertExit({timeout, _}, gunner_pool:acquire(PoolID, {"google.com", 80}, Ticket, 1)),
        ok = gunner_pool:cancel_acquire(PoolID, Ticket)
    end),
    ok = assert_counters(PoolID, Counters, []),
    _ = timer:sleep(1000),
    ok = assert_counters(PoolID, Counters, [acquire, free]),
    ok = gunner:stop_pool(PoolID).

-spec strict_connection_ownership_test(config()) -> test_return().
strict_connection_ownership_test(C) ->
    _ = start_worker_tab(),
    Counters = init_counter_state(?POOL_NAME(C)),
    {ok, Connection1} = client_process_persistent(client1, fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, make_ref(), 1000),
        {return, {ok, Connection}}
    end),
    {ok, Connection2} = client_process_persistent(client2, fun() ->
        {ok, Connection} = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, make_ref(), 1000),
        {return, {ok, Connection}}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
    ?assertNotEqual(Connection1, Connection2),
    ok = client_process_persistent(client1, fun() ->
        ?assertEqual(
            {error, {lease_return_failed, connection_not_found}},
            gunner_pool:free(?POOL_NAME(C), Connection2, 1000)
        ),
        {return, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        ?assertEqual(
            {error, {lease_return_failed, connection_not_found}},
            gunner_pool:free(?POOL_NAME(C), Connection1, 1000)
        ),
        {return, ok}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
    ok = client_process_persistent(client1, fun() ->
        ?assertEqual(ok, gunner_pool:free(?POOL_NAME(C), Connection1, 1000)),
        {exit, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        ?assertEqual(ok, gunner_pool:free(?POOL_NAME(C), Connection2, 1000)),
        {exit, ok}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free]).

-spec auto_free_on_client_death_test(config()) -> test_return().
auto_free_on_client_death_test(C) ->
    _ = start_worker_tab(),
    Counters = init_counter_state(?POOL_NAME(C)),
    ok = client_process_persistent(client1, fun() ->
        _ = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, make_ref(), 1000),
        {return, ok}
    end),
    ok = client_process_persistent(client2, fun() ->
        _ = gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8080}, make_ref(), 1000),
        {return, ok}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire]),
    ok = client_process_persistent(client1, fun() ->
        {exit, ok}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free]),
    ok = client_process_persistent(client2, fun() ->
        {exit, ok}
    end),
    ok = assert_counters(?POOL_NAME(C), Counters, [acquire, acquire, free, free]).

%%

start_mock_server(HandlerFun) ->
    mock_http_server:start(8080, HandlerFun).

stop_mock_server() ->
    mock_http_server:stop().

%%

init_counter_state(PoolID) ->
    InitialFree = get_pool_free_size(PoolID),
    InitialTotal = get_pool_total_size(PoolID),
    {InitialFree, InitialTotal}.

count_operations({Free, Total}, []) ->
    {Free, Total};
count_operations({Free, Total}, [acquire | Rest]) when Free =:= 0 ->
    count_operations({Free, Total + 1}, Rest);
count_operations({Free, Total}, [acquire | Rest]) when Free > 0 ->
    count_operations({Free - 1, Total}, Rest);
count_operations({Free, Total}, [free | Rest]) ->
    count_operations({Free + 1, Total}, Rest).

assert_counters(PoolID, Counters, Operations) ->
    NewCounters = count_operations(Counters, Operations),
    case {get_pool_free_size(PoolID), get_pool_total_size(PoolID)} of
        NewCounters ->
            ok;
        InvalidCounters ->
            {error, {NewCounters, InvalidCounters}}
    end.

%%

client_process_persistent(Name, Fun) ->
    client_process_persistent(Name, Fun, 1000).

client_process_persistent(Name, Fun, Timeout) ->
    Pid = find_or_create_process(Name),
    Pid ! Fun,
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

start_worker_tab() ->
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
    spawn_link(fun() -> persistent_process(Self, Timeout) end).

delete_process(Name) ->
    ets:delete(?WORKER_TAB, Name).

persistent_process(Parent, Timeout) ->
    receive
        Fun ->
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
                    persistent_process(Parent, Timeout);
                {exit, _} = Exit ->
                    Parent ! Exit;
                {caught, _} = Caught ->
                    Parent ! Caught,
                    persistent_process(Parent, Timeout)
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

get_pool_total_size(PoolID) ->
    {ok, #{total_count := Size}} = gunner:pool_status(PoolID),
    Size.

get_pool_free_size(PoolID) ->
    {ok, #{free_count := Size}} = gunner:pool_status(PoolID),
    Size.
