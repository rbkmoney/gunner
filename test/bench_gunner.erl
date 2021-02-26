-module(bench_gunner).

-export([
    gunner_pool_loose/1,
    bench_gunner_pool_loose/2,
    gunner_pool_locking/1,
    bench_gunner_pool_locking/2
]).

-define(POOL_ID_PROP, pool_id).
-define(POOL_ID(St), proplists:get_value(?POOL_ID_PROP, St)).

%%

-spec gunner_pool_loose(_) -> _.
gunner_pool_loose(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    {ok, Pid} = gunner:start_pool(#{
        max_size => 1000
    }),
    [{?POOL_ID_PROP, Pid}, {apps, [App || {ok, App} <- Apps]}];
gunner_pool_loose({input, State}) ->
    ?POOL_ID(State);
gunner_pool_loose({stop, State}) ->
    ok = gunner:stop_pool(?POOL_ID(State)),
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gunner_pool_loose(_, _) -> _.
bench_gunner_pool_loose(PoolID, _) ->
    {ok, _} = gunner:get(PoolID, valid_host(), <<"/">>, 1000).

%%

-spec gunner_pool_locking(_) -> _.
gunner_pool_locking(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    {ok, Pid} = gunner:start_pool(#{
        mode => locking,
        max_size => 1000
    }),
    [{?POOL_ID_PROP, Pid}, {apps, [App || {ok, App} <- Apps]}];
gunner_pool_locking({input, State}) ->
    ?POOL_ID(State);
gunner_pool_locking({stop, State}) ->
    ok = gunner:stop_pool(?POOL_ID(State)),
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gunner_pool_locking(_, _) -> _.
bench_gunner_pool_locking(PoolID, _) ->
    {ok, StreamRef} = gunner:get(PoolID, valid_host(), <<"/">>, 1000),
    _ = gunner:free(PoolID, StreamRef, 1000).

%%

-spec start_mock_server() -> ok.
start_mock_server() ->
    start_mock_server(fun(_) ->
        {200, #{}, <<"ok">>}
    end).

-spec start_mock_server(fun()) -> ok.
start_mock_server(HandlerFun) ->
    Conf = #{request_timeout => infinity},
    _ = mock_http_server:start(default, 8080, HandlerFun, Conf),
    _ = mock_http_server:start(alternative_1, 8086, HandlerFun, Conf),
    _ = mock_http_server:start(alternative_2, 8087, HandlerFun, Conf),
    ok.

-spec stop_mock_server() -> ok.
stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative_1),
    ok = mock_http_server:stop(alternative_2).

-spec valid_host() -> {inet:hostname(), inet:port_number()}.
valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086},
        {"localhost", 8087}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).
