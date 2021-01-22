-module(bench_gunner).

-export([
    gunner_pool/1,
    bench_gunner_pool/2
]).

%%

-spec gunner_pool(_) -> _.
gunner_pool(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(),
    ok = gunner:start_pool(default, #{}),
    [{apps, [App || {ok, App} <- Apps]}];
gunner_pool({input, _State}) ->
    valid_host();
gunner_pool({stop, State}) ->
    ok = gunner:stop_pool(default),
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gunner_pool(_, _) -> _.
bench_gunner_pool(Destination, _) ->
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    {ok, <<"ok/", Tag/binary>>} = get(default, Destination, <<"/", Tag/binary>>, 1000).

%%

start_mock_server() ->
    start_mock_server(fun(#{path := Path}) ->
        {200, #{}, <<"ok", Path/binary>>}
    end).

start_mock_server(HandlerFun) ->
    Conf = #{request_timeout => infinity},
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

get(PoolID, ConnectionArgs, Path, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case gunner:get(PoolID, ConnectionArgs, Path, Timeout) of
        {ok, PoolRef} ->
            TimeoutLeft1 = Deadline - erlang:monotonic_time(millisecond),
            case gunner:await(PoolRef, TimeoutLeft1) of
                {response, nofin, 200, _Headers} ->
                    TimeoutLeft2 = Deadline - erlang:monotonic_time(millisecond),
                    case gunner:await_body(PoolRef, TimeoutLeft2) of
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
            end;
        {error, _Reason} = Error ->
            Error
    end.
