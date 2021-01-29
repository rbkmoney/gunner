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
-export([normal_locking_client/1]).
-export([misinformed_client/1]).
-export([confused_client/1]).

-define(POOL_ID_PROP, pool_id).
-define(POOL_ID(C), proplists:get_value(?POOL_ID_PROP, C)).

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
        {group, survival},
        {group, survival_locking}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {survival, [parallel, shuffle], create_group(survival, 500)},
        {survival_locking, [parallel, shuffle], create_group(survival_locking, 500)}
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
init_per_group(survival, C) ->
    {ok, Pid} = gunner:start_pool(#{
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_load => ?POOL_MAX_CONNECTION_LOAD,
        max_connection_idle_age => ?POOL_MAX_CONNECTION_IDLE_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE
    }),
    C ++ [{?POOL_ID_PROP, Pid}];
init_per_group(survival_locking, C) ->
    {ok, Pid} = gunner:start_pool(#{
        mode => locking,
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_load => ?POOL_MAX_CONNECTION_LOAD,
        max_connection_idle_age => ?POOL_MAX_CONNECTION_IDLE_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE
    }),
    C ++ [{?POOL_ID_PROP, Pid}];
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(TestGroupName, C) when TestGroupName =:= survival; TestGroupName =:= survival_locking ->
    ok = gunner:stop_pool(?POOL_ID(C));
end_per_group(_, _C) ->
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.

get_group_spec(survival) ->
    [
        {normal_client, 0.8},
        {misinformed_client, 0.1},
        {confused_client, 0.1}
    ];
get_group_spec(survival_locking) ->
    [
        {normal_locking_client, 0.8},
        {misinformed_client, 0.1},
        {confused_client, 0.1}
    ].

create_group(GroupName, TotalTests) ->
    Spec = get_group_spec(GroupName),
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
    case gunner:get(?POOL_ID(C), valid_host(), <<"/", Tag/binary>>, 1000) of
        {ok, PoolRef} ->
            {ok, <<"ok/", Tag/binary>>} = await(PoolRef, ?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2);
        {error, pool_unavailable} ->
            ok
    end.

-spec normal_locking_client(config()) -> test_return().
normal_locking_client(C) ->
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    case gunner:get(?POOL_ID(C), valid_host(), <<"/", Tag/binary>>, 1000) of
        {ok, PoolRef} ->
            {ok, <<"ok/", Tag/binary>>} = await(PoolRef, ?COWBOY_HANDLER_MAX_SLEEP_DURATION * 2),
            _ = gunner:free(?POOL_ID(C), PoolRef);
        {error, pool_unavailable} ->
            ok
    end.

-spec misinformed_client(config()) -> test_return().
misinformed_client(C) ->
    case gunner:get(?POOL_ID(C), {"localhost", 8090}, <<"/">>, 1000) of
        {error, {connection_failed, _}} ->
            ok;
        {error, pool_unavailable} ->
            ok
    end.

-spec confused_client(config()) -> test_return().
confused_client(C) ->
    case gunner:get(?POOL_ID(C), {"localghost", 8080}, <<"/">>, 1000) of
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

await(PoolRef, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case gunner:await(PoolRef, Timeout) of
        {response, nofin, 200, _Headers} ->
            TimeoutLeft = Deadline - erlang:monotonic_time(millisecond),
            case gunner:await_body(PoolRef, TimeoutLeft) of
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
