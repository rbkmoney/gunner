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

-define(POOL_NAME_PROP, pool_name).
-define(POOL_NAME(C), proplists:get_value(?POOL_NAME_PROP, C)).

-define(POOL_CLEANUP_INTERVAL, 1000).
-define(POOL_MAX_CONNECTION_LOAD, 1).
-define(POOL_MAX_CONNECTION_IDLE_AGE, 5).
-define(POOL_MAX_SIZE, 25).
-define(POOL_MIN_SIZE, 5).

-define(COWBOY_HANDLER_MAX_SLEEP_DURATION, 2500).

-define(GUNNER_REF(ConnectionPid, StreamRef), {gunner_ref, ConnectionPid, StreamRef}).

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
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_load => ?POOL_MAX_CONNECTION_LOAD,
        max_connection_idle_age => ?POOL_MAX_CONNECTION_IDLE_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE
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
        {normal_client, 0.8},
        {misinformed_client, 0.1},
        {confused_client, 0.1}
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
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    case
        get(
            ?POOL_NAME(C),
            valid_host(),
            <<"/", Tag/binary>>,
            ?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2
        )
    of
        {ok, <<"ok/", Tag/binary>>} ->
            ok;
        {error, pool_unavailable} ->
            ok
    end.

-spec misinformed_client(config()) -> test_return().
misinformed_client(C) ->
    case gunner:get(?POOL_NAME(C), {"localhost", 8090}, <<"/">>, 1000) of
        {error, {connection_failed, _}} ->
            ok;
        {error, pool_unavailable} ->
            ok
    end.

-spec confused_client(config()) -> test_return().
confused_client(C) ->
    case gunner:get(?POOL_NAME(C), {"localghost", 8080}, <<"/">>, 1000) of
        {error, {connection_failed, _}} ->
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
