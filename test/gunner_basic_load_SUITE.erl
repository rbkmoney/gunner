-module(gunner_basic_load_SUITE).

-include("gunner_events.hrl").
-include("gunner_event_helpers.hrl").

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

%%

-export([
    basic_load/1
]).

%%

-define(POOL_PID_PROP, pool_pid).
-define(POOL_PID(C), proplists:get_value(?POOL_PID_PROP, C)).

-define(EH_ID, suite_pool).
-define(EH_STORAGE_PROP, event_handler_storage_pid).
-define(EH_STORAGE(C), proplists:get_value(?EH_STORAGE_PROP, C)).

-define(CNT_PROP, counter_ref).
-define(CNT(C), proplists:get_value(?CNT_PROP, C)).

-define(CNT_FAIL_PROP, counter_fail_idx).
-define(CNT_FAIL(C), proplists:get_value(?CNT_FAIL_PROP, C)).

-define(POOL_MIN_SIZE, 2).
-define(POOL_MAX_SIZE, 25).

%% Keep this more than the max age
-define(POOL_CLEANUP_INTERVAL, 200).
-define(POOL_MAX_AGE, 100).

%%

-spec all() -> [test_case_name() | {group, group_name()}].
all() -> [basic_load].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() -> [].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = [application:ensure_all_started(App) || App <- [ponos, cowboy, gunner]],
    {ok, EventStorage} = gunner_test_event_h:start_storage(),
    _ = start_mock_server(),
    CntRef = counters:new(1, [write_concurrency]),
    C ++
        [
            {?EH_STORAGE_PROP, EventStorage},
            {?CNT_PROP, CntRef},
            {?CNT_FAIL_PROP, 1},
            {apps, [App || {ok, App} <- Apps]}
        ].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    ok = gunner_test_event_h:stop_storage(?EH_STORAGE(C)),
    _ = stop_mock_server(),
    _ = lists:foreach(fun(App) -> application:stop(App) end, proplists:get_value(apps, C)),
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_Name, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_Name, _C) ->
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    {ok, PoolPid} = gunner:start_pool(#{
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_idle_age => ?POOL_MAX_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE,
        event_handler => gunner_test_event_h:make_event_h(?EH_ID, ?EH_STORAGE(C))
    }),
    C ++ [{?POOL_PID_PROP, PoolPid}].

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, C) ->
    ok = gunner:stop_pool(?POOL_PID(C)),
    ok.

%%

-spec basic_load(config()) -> test_return().
basic_load(C) ->
    Duration = 10000,
    Generator0 = make_generator(Duration),
    Generator1 = add_task(async_get, ponos_load_specs:make_constant(50.0), Generator0, task_async_get(C)),
    Generator2 = add_task(sync_post, ponos_load_specs:make_constant(50.0), Generator1, task_sync_post(C)),
    Generator3 = add_task(fail_connection, ponos_load_specs:make_constant(20.0), Generator2, task_fail_connection(C)),
    Generator4 = add_task(no_unlock, ponos_load_specs:make_constant(10.0), Generator3, task_no_unlock(C)),
    ok = run_generator(Generator4),
    ok = await_finish(Generator4, 1000, Duration * 2),
    ok = check_failures(C).

%%

task_async_get(C) ->
    make_task_fun(C, fun() ->
        Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
        Endpoint = valid_host(),
        case gunner:get(?POOL_PID(C), Endpoint, <<"/", Tag/binary>>) of
            {ok, 200, _, <<"ok/", Tag/binary>>} ->
                ok;
            {error, pool_unavailable} ->
                ok
        end
    end).

task_sync_post(C) ->
    make_task_fun(C, fun() ->
        Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
        Endpoint = valid_host(),
        case
            gunner:transaction(?POOL_PID(C), Endpoint, #{}, fun(ConnectionPid) ->
                StreamRef = gun:post(ConnectionPid, <<"/", Tag/binary>>, [], <<>>),
                case gun:await(ConnectionPid, StreamRef) of
                    {response, nofin, _, _} ->
                        {ok, Body} = gun:await_body(ConnectionPid, StreamRef),
                        Body
                end
            end)
        of
            {ok, <<"ok/", Tag/binary>>} ->
                ok;
            {error, pool_unavailable} ->
                ok
        end
    end).

task_fail_connection(C) ->
    make_task_fun(C, fun() ->
        Endpoint = {"nxdomain.land", 8080},
        case gunner:get(?POOL_PID(C), Endpoint, <<"/">>) of
            {error, {connection_failed, {shutdown, nxdomain}}} ->
                ok;
            {error, pool_unavailable} ->
                ok
        end
    end).

task_no_unlock(C) ->
    make_task_fun(C, fun() ->
        Endpoint = valid_host(),
        case gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000) of
            {ok, _ConnectionPid} ->
                ok;
            {error, pool_unavailable} ->
                ok
        end
    end).

%%

make_task_fun(C, TaskFun) ->
    fun() ->
        try
            TaskFun()
        catch
            Error:Reason:Stacktrace ->
                report_fail(C, {Error, Reason, Stacktrace})
        end
    end.

%%

report_fail(C, Error) ->
    %% Good enough for now
    _ = ct:pal("~p~n", [Error]),
    ok = counters:add(?CNT(C), ?CNT_FAIL(C), 1).

check_failures(C) ->
    case counters:get(?CNT(C), ?CNT_FAIL(C)) of
        0 ->
            ok;
        Failures ->
            _ = ct:pal("~p~n", [pop_events(C)]),
            {error, {failure_count, Failures}}
    end.

%%

make_generator(Duration) ->
    {[], [{duration, Duration}, {auto_start, false}]}.

add_task(Name, LoadSpec, {Tasks, Opts}, Fun) ->
    Args = [{name, Name}, {task, Fun}, {load_spec, LoadSpec}, {options, Opts}],
    [ok] = ponos:add_load_generators([Args]),
    {[Name | Tasks], Opts}.

run_generator({Tasks, _}) ->
    _ = ponos:init_load_generators(Tasks),
    ok.

await_finish({[], _}, _Tick, _Timeout) ->
    ok;
await_finish(_Generator, _Tick, Timeout) when Timeout =< 0 ->
    {error, timeout};
await_finish({[Name | Rest], Opts} = Generator, Tick, Timeout) ->
    case ponos:is_running(Name) of
        true ->
            _ = timer:sleep(Tick),
            await_finish(Generator, Tick, Timeout - Tick);
        false ->
            {error, {not_running, Name}};
        {error, {non_existing, Name}} ->
            await_finish({Rest, Opts}, Tick, Timeout)
    end.

%%

pop_events(C) ->
    pop_events(C, #{}).

pop_events(C, Opts) when is_map(Opts) ->
    pop_events(C, Opts, ?EH_ID);
pop_events(C, ID) when is_atom(ID) ->
    pop_events(C, #{}, ID).

pop_events(C, Opts, ID) ->
    {ok, Events} = gunner_test_event_h:pop_events(?EH_STORAGE(C), ID),
    filter_events(Events, Opts).

filter_events(Events, #{ignore_cleanups := true}) ->
    lists:filter(
        fun
            (?cleanup_started(_)) -> false;
            (?cleanup_finished(_)) -> false;
            (_) -> true
        end,
        Events
    );
filter_events(Events, _Opts) ->
    Events.

%%

valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).

start_mock_server() ->
    start_mock_server(fun(#{path := Path}) ->
        _ = timer:sleep(rand:uniform(500)),
        {200, #{}, <<"ok", Path/binary>>}
    end).

start_mock_server(HandlerFun) ->
    Opts = #{request_timeout => infinity},
    _ = mock_http_server:start(default, 8080, HandlerFun, Opts),
    _ = mock_http_server:start(alternative, 8086, HandlerFun, Opts),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative).
