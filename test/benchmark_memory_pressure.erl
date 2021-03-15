-module(benchmark_memory_pressure).

-export([run/0]).

-spec run() -> ok.
run() ->
    Opts = #{iterations => 100},
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    {ok, Pid} = gunner:start_pool(#{
        mode => locking
    }),
    _ = run(gunner, mk_gunner_runner(Pid), Opts#{target_process => Pid}),
    ok = gunner:stop_pool(Pid),
    _ = stop_mock_server(),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

start_mock_server() ->
    start_mock_server(fun(_) ->
        {200, #{}, <<"ok">>}
    end).

start_mock_server(HandlerFun) ->
    _ = mock_http_server:start(default, 8080, HandlerFun),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default).

-spec run(atom(), meter_memory_pressure:runner(), meter_memory_pressure:opts()) -> ok.
run(Name, Runner, Opts) ->
    _ = io:format("Benchmarking '~s' memory pressure...~n", [Name]),
    _ = io:format("====================================~n", []),
    Metrics = meter_memory_pressure:measure(Runner, Opts),
    lists:foreach(
        fun(Metric) ->
            io:format("~24s = ~-16b~n", [Metric, maps:get(Metric, Metrics)])
        end,
        [
            minor_gcs,
            minor_gcs_duration,
            major_gcs,
            major_gcs_duration,
            heap_reclaimed,
            offheap_bin_reclaimed,
            stack_min,
            stack_max
        ]
    ),
    _ = io:format("====================================~n~n", []),
    ok.

-spec mk_gunner_runner(pid()) -> meter_memory_pressure:runner().
mk_gunner_runner(PoolID) ->
    fun() ->
        case gunner_pool:acquire(PoolID, {"localhost", 8080}, 1000) of
            {ok, Connection} ->
                ok = gunner_pool:free(PoolID, Connection);
            {error, pool_unavailable} ->
                ok
        end
    end.
