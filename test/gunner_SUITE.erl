-module(gunner_SUITE).

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

-export([request_ok/1]).
-export([request_nxdomain/1]).
-export([pool_resizing_ok_test/1]).
-export([pool_unavalilable_test/1]).

-define(REQUEST_PROCESSING_SLEEP, 2500).
-define(GUNNER_POOL, default).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, default},
        {group, pool_tests}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {default, [], [
            request_ok,
            request_nxdomain
        ]},
        {pool_tests, [], [
            pool_resizing_ok_test,
            pool_unavalilable_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    C.

-spec end_per_suite(config()) -> _.
end_per_suite(_C) ->
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(default, C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(fun() ->
        {200, #{}, <<"ok">>}
    end),
    ok = gunner:start_pool(?GUNNER_POOL, #{}),
    C ++ [{apps, [App || {ok, App} <- Apps]}];
init_per_group(pool_tests, C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    _ = start_mock_server(fun() ->
        _ = timer:sleep(?REQUEST_PROCESSING_SLEEP),
        {200, #{}, <<"ok">>}
    end),
    ok = gunner:start_pool(?GUNNER_POOL, #{}),
    C ++ [{apps, [App || {ok, App} <- Apps]}].

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_, C) ->
    _ = stop_mock_server(),
    ok = gunner:stop_pool(?GUNNER_POOL),
    Apps = proplists:get_value(apps, C),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec request_ok(config()) -> test_return().
request_ok(_C) ->
    {ok, _Result} = get(?GUNNER_POOL, "localhost", 8080, <<"/">>).

-spec request_nxdomain(config()) -> test_return().
request_nxdomain(_C) ->
    {error, {connection_init_failed, {shutdown, nxdomain}}} = get(?GUNNER_POOL, "localghost", 8080, <<"/">>).

-spec pool_resizing_ok_test(config()) -> test_return().
pool_resizing_ok_test(_C) ->
    RequestsAmount = 25,
    Pool = ?GUNNER_POOL,
    Host = "localhost",
    Port = 8080,
    Path = <<"/">>,
    ok = spawn_requests(RequestsAmount, Pool, Host, Port, Path, 3000),
    _ = timer:sleep(1000),
    ?assertEqual(25, get_pool_size(Pool)),
    Responses = gather_responses(RequestsAmount, ?REQUEST_PROCESSING_SLEEP),
    ?assertEqual(5, get_pool_size(Pool)),
    ?assertEqual(ok, validate_no_errors(Responses, <<"ok">>)).

-spec pool_unavalilable_test(config()) -> test_return().
pool_unavalilable_test(_C) ->
    RequestsAmount = 26,
    Pool = ?GUNNER_POOL,
    Host = "localhost",
    Port = 8080,
    Path = <<"/">>,
    ok = spawn_requests(RequestsAmount, Pool, Host, Port, Path, 3000),
    _ = timer:sleep(1000),
    ?assertEqual(25, get_pool_size(Pool)),
    Responses = gather_responses(RequestsAmount, ?REQUEST_PROCESSING_SLEEP),
    ?assertEqual({error, pool_unavailable}, validate_no_errors(Responses, <<"ok">>)).

%%

get(Pool, Host, Port, Path) ->
    get(Pool, Host, Port, Path, 1000).

get(Pool, Host, Port, Path, Timeout) ->
    gunner:transaction(
        Pool,
        {Host, Port},
        fun(Client) ->
            StreamRef = gun:get(Client, Path),
            Deadline = erlang:monotonic_time(millisecond) + Timeout,
            case gun:await(Client, StreamRef, Timeout) of
                {response, nofin, 200, _Headers} ->
                    TimeoutLeft = Deadline - erlang:monotonic_time(millisecond),
                    case gun:await_body(Client, StreamRef, TimeoutLeft) of
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
            end
        end,
        Timeout
    ).

%%

start_mock_server(HandlerFun) ->
    mock_http_server:start(8080, HandlerFun).

stop_mock_server() ->
    mock_http_server:stop().

%%

spawn_requests(Amount, Pool, Host, Port, Path, Timeout) ->
    Parent = self(),
    _ = repeat(
        fun() ->
            spawn(fun() ->
                Parent ! {request_response, get(Pool, Host, Port, Path, Timeout)}
            end)
        end,
        Amount
    ),
    ok.

gather_responses(Amount, Timeout) ->
    repeat(
        fun() ->
            receive
                {request_response, Response} ->
                    Response
            after Timeout -> {error, timeout}
            end
        end,
        Amount
    ).

validate_no_errors([], _ResponseMatch) ->
    ok;
validate_no_errors([{ok, ResponseMatch} | Rest], ResponseMatch) ->
    validate_no_errors(Rest, ResponseMatch);
validate_no_errors([{ok, ResponseNotMatch} | _Rest], _ResponseMatch) ->
    {error, {response_does_not_match, ResponseNotMatch}};
validate_no_errors([{error, _} = Error | _Rest], _ResponseMatch) ->
    Error.

repeat(Fun, I) ->
    repeat(Fun, I, []).

repeat(_Fun, 0, Acc) ->
    Acc;
repeat(Fun, I, Acc0) ->
    Acc1 = Acc0 ++ [Fun()],
    repeat(Fun, I - 1, Acc1).

get_pool_size(PoolID) ->
    {ok, #{current_size := Size}} = gunner:pool_status(PoolID),
    Size.
