-module(gunner_pool_lifetime_SUITE).

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

-export([pool_lifetime_test/1]).
-export([pool_already_exists/1]).
-export([pool_not_found/1]).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, pool_management}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {pool_management, [], [
            pool_lifetime_test,
            pool_already_exists,
            pool_not_found
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = [application:ensure_all_started(App) || App <- [gunner]],
    C ++ [{apps, [App || {ok, App} <- Apps]}].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    Apps = proplists:get_value(apps, C),
    _ = lists:foreach(fun(App) -> application:stop(App) end, Apps),
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_, _C) ->
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec pool_lifetime_test(config()) -> test_return().
pool_lifetime_test(_C) ->
    {ok, Pid} = gunner_pool:start_pool(#{}),
    ?assertMatch({ok, _}, gunner_pool:pool_status(Pid, 1000)),
    ?assertEqual(ok, gunner_pool:stop_pool(Pid)).

-spec pool_already_exists(config()) -> test_return().
pool_already_exists(_C) ->
    PoolRef = {local, test_gunner_pool},
    {ok, Pid} = gunner_pool:start_pool(PoolRef, #{}),
    ?assertEqual({error, already_exists}, gunner_pool:start_pool(PoolRef, #{})),
    ?assertEqual(ok, gunner_pool:stop_pool(Pid)).

-spec pool_not_found(config()) -> test_return().
pool_not_found(_C) ->
    ?assertEqual({error, pool_not_found}, gunner_pool:pool_status(what, 1000)).
