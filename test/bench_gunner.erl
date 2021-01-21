-module(bench_gunner).

-export([
    gunner_pool_acquire/1,
    bench_gunner_pool_acquire/2,
    gunner_pool_free/1,
    bench_gunner_pool_free/2
]).

%%

-spec gunner_pool_acquire(_) -> _.
gunner_pool_acquire(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    ok = gunner:start_pool(default, #{
        free_connection_limit => 5,
        total_connection_limit => 100
    }),
    [{apps, [App || {ok, App} <- Apps]}];
gunner_pool_acquire({input, _State}) ->
    valid_host();
gunner_pool_acquire({stop, State}) ->
    ok = gunner:stop_pool(default),
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gunner_pool_acquire(_, _) -> _.
bench_gunner_pool_acquire(Destination, _) ->
    case gunner_pool:acquire(default, Destination, make_ref(), 1000) of
        {ok, _Connection} ->
            ok;
        {error, pool_unavailable} ->
            %% 100 connection pool fills up even at 10s run durarion. I guess its fine?
            ok
    end.

%%

-spec gunner_pool_free(_) -> _.
gunner_pool_free(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    ok = gunner:start_pool(default, #{
        free_connection_limit => 5,
        total_connection_limit => 100
    }),
    [{apps, [App || {ok, App} <- Apps]}];
gunner_pool_free({input, _State}) ->
    {ok, Connection} = gunner_pool:acquire(default, valid_host(), make_ref(), 1000),
    Connection;
gunner_pool_free({stop, State}) ->
    ok = gunner:stop_pool(default),
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gunner_pool_free(_, _) -> _.
bench_gunner_pool_free(Connection, _) ->
    case gunner_pool:free(default, Connection, 1000) of
        ok ->
            ok;
        {error, {lease_return_failed, connection_not_found}} ->
            %% Connection might have been killed because of lack of requests
            ok
    end.

%%

start_mock_server() ->
    start_mock_server(fun(_) ->
        {200, #{}, <<"ok">>}
    end).

start_mock_server(HandlerFun) ->
    Conf = #{request_timeout => 5000},
    _ = mock_http_server:start(default, 8080, HandlerFun, Conf),
    _ = mock_http_server:start(alternative_1, 8086, HandlerFun, Conf),
    _ = mock_http_server:start(alternative_2, 8087, HandlerFun, Conf),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative_1),
    ok = mock_http_server:stop(alternative_2).

valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086},
        {"localhost", 8087}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).
