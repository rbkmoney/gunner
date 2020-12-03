-module(gunner_pool_SUITE).

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

-define(GUNNER_POOL, default).

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, default}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {default, [], [
            pool_lifetime_test
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
init_per_group(_, C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    C ++ [{apps, [App || {ok, App} <- Apps]}].

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_, C) ->
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

-spec pool_lifetime_test(config()) -> test_return().
pool_lifetime_test(_C) ->
    ?assertEqual(ok, gunner:start_pool(?GUNNER_POOL, #{})),
    ?assertEqual({error, already_exists}, gunner:start_pool(?GUNNER_POOL, #{})),
    ?assertMatch({ok, _}, gunner_manager:get_pool(?GUNNER_POOL, 1000)),
    ?assertEqual(ok, gunner:stop_pool(?GUNNER_POOL)),
    ?assertEqual({error, not_found}, gunner:stop_pool(?GUNNER_POOL)).
