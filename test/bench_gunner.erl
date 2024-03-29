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
    {ok, _, _, _} = gunner:get(PoolID, valid_host(), <<"/">>).

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
    gunner:transaction(PoolID, valid_host(), #{}, fun(ConnectionPid) ->
        StreamRef = gun:request(ConnectionPid, <<"GET">>, <<"/">>, [], <<>>, #{}),
        {ok, <<"ok">>} = await(ConnectionPid, StreamRef, 1000),
        ok
    end).

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

-spec await(pid(), gun:stream_ref(), timeout()) -> {ok, Response :: _} | {error, Reason :: _}.
await(ConnectionPid, StreamRef, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case gun:await(ConnectionPid, StreamRef, Timeout) of
        {response, nofin, 200, _Headers} ->
            TimeoutLeft = Deadline - erlang:monotonic_time(millisecond),
            case gun:await_body(ConnectionPid, StreamRef, TimeoutLeft) of
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
