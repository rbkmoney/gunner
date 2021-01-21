-module(gunner_pool_survival_SUITE).

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

-export([normal_client/1]).
-export([misinformed_client/1]).
-export([confused_client/1]).
-export([impatient_client/1]).
-export([rude_client/1]).

-define(POOL_NAME_PROP, pool_name).
-define(POOL_NAME(C), proplists:get_value(?POOL_NAME_PROP, C)).

-define(POOL_FREE_CONNECTION_LIMIT, 5).
-define(POOL_TOTAL_CONNECTION_LIMIT, 50).

-define(COWBOY_HANDLER_MAX_SLEEP_DURATION, 2500).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, survival}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {survival, [parallel, shuffle], create_group(1000)}
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
init_per_group(TestGroupName, C) when TestGroupName =:= survival ->
    PoolName = {pool, erlang:unique_integer()},
    ok = gunner:start_pool(PoolName, #{
        free_connection_limit => ?POOL_FREE_CONNECTION_LIMIT,
        total_connection_limit => ?POOL_TOTAL_CONNECTION_LIMIT
    }),
    C ++ [{?POOL_NAME_PROP, PoolName}];
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(TestGroupName, C) when TestGroupName =:= survival ->
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

create_group(TotalTests) ->
    Spec = [
        {normal_client, 0.6},
        {misinformed_client, 0.1},
        {confused_client, 0.1},
        {impatient_client, 0.1},
        {rude_client, 0.1}
    ],
    make_testcase_list(Spec, TotalTests, []).

make_testcase_list([], _TotalTests, Acc) ->
    lists:flatten(Acc);
make_testcase_list([{CaseName, Percent} | Rest], TotalTests, Acc) ->
    Amount = round(TotalTests * Percent),
    make_testcase_list(Rest, TotalTests, [lists:duplicate(Amount, CaseName) | Acc]).

%%

-spec normal_client(config()) -> test_return().
normal_client(C) ->
    case gunner_pool:acquire(?POOL_NAME(C), valid_host(), make_ref(), 1000) of
        {ok, Connection} ->
            Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
            {ok, <<"ok/", Tag/binary>>} = get(Connection, <<"/", Tag/binary>>, ?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2),
            ok = gunner_pool:free(?POOL_NAME(C), Connection, 1000);
        {error, pool_unavailable} ->
            ok
    end.

-spec misinformed_client(config()) -> test_return().
misinformed_client(C) ->
    case gunner_pool:acquire(?POOL_NAME(C), {"localhost", 8081}, make_ref(), 1000) of
        {error, {connection_failed, _}} ->
            ok;
        {error, pool_unavailable} ->
            ok
    end.

-spec confused_client(config()) -> test_return().
confused_client(C) ->
    case gunner_pool:acquire(?POOL_NAME(C), {"localghost", 8080}, make_ref(), 1000) of
        {error, {connection_failed, _}} ->
            ok;
        {error, pool_unavailable} ->
            ok
    end.

-spec impatient_client(config()) -> test_return().
impatient_client(C) ->
    Ticket = make_ref(),
    try
        _ = gunner_pool:acquire(?POOL_NAME(C), valid_host(), Ticket, 1)
    catch
        _:_:_ -> ok
    after
        ok = gunner_pool:cancel_acquire(?POOL_NAME(C), Ticket)
    end.

-spec rude_client(config()) -> test_return().
rude_client(C) ->
    case gunner_pool:acquire(?POOL_NAME(C), valid_host(), make_ref(), 1000) of
        {ok, Connection} ->
            ok = gun:close(Connection),
            ok;
        {error, pool_unavailable} ->
            ok
    end.

%%

valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086},
        {"localhost", 8087}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).

%%

start_mock_server() ->
    start_mock_server(fun(#{path := Path}) ->
        _ = timer:sleep(rand:uniform(?COWBOY_HANDLER_MAX_SLEEP_DURATION)),
        {200, #{}, <<"ok", Path/binary>>}
    end).

start_mock_server(HandlerFun) ->
    _ = mock_http_server:start(default, 8080, HandlerFun),
    _ = mock_http_server:start(alternative_1, 8086, HandlerFun),
    _ = mock_http_server:start(alternative_2, 8087, HandlerFun),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative_1),
    ok = mock_http_server:stop(alternative_2).

%%

get(Client, Path, Timeout) ->
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
    end.
