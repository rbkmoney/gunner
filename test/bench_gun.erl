%% Baseline test for raw gun. Should not be run by default or TIME_WAIT will destroy you
-module(bench_gun).

-export([
    gun/1,
    bench_gun/2
]).

%%

-spec gun(_) -> _.
gun(init) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gun]],
    _ = start_mock_server(),
    [{apps, [App || {ok, App} <- Apps]}];
gun({input, _State}) ->
    valid_host();
gun({stop, State}) ->
    _ = stop_mock_server(),
    Apps = proplists:get_value(apps, State),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

-spec bench_gun(_, _) -> _.
bench_gun({Host, Port}, _) ->
    {ok, Connection} = gun:open(Host, Port, #{retry => 0}),
    {ok, _} = gun:await_up(Connection, 1000),
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    {ok, <<"ok/", Tag/binary>>} = get(Connection, <<"/", Tag/binary>>, 1000),
    ok = gun:close(Connection).

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

get(Client, Path, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    StreamRef = gun:get(Client, Path),
    TimeoutLeft1 = Deadline - erlang:monotonic_time(millisecond),
    case gun:await(Client, StreamRef, TimeoutLeft1) of
        {response, nofin, 200, _Headers} ->
            TimeoutLeft2 = Deadline - erlang:monotonic_time(millisecond),
            case gun:await_body(Client, StreamRef, TimeoutLeft2) of
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
