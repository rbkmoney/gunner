-module(gunner_SUITE).

-include("gunner_events.hrl").
-include("gunner_event_helpers.hrl").

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

%%

-export([
    pool_lifetime_test/1,
    pool_not_found_test/1,

    get_request_success_test/1,
    post_request_success_test/1,
    transaction_success_test/1,
    invalid_domain_test/1,
    connection_refused_test/1,

    pool_unavailable_test/1,
    pool_cleanup_test/1,
    pool_dead_connection_test/1,
    pool_dead_client_test/1,
    pool_group_separation_test/1,
    pool_group_reuse_test/1,
    pool_no_freeing_unlocked_test/1,
    pool_no_double_free_test/1
]).

%%

-define(POOL_PID_PROP, pool_pid).
-define(POOL_PID(C), proplists:get_value(?POOL_PID_PROP, C)).

-define(EH_ID, suite_pool).
-define(EH_STORAGE_PROP, event_handler_storage_pid).
-define(EH_STORAGE(C), proplists:get_value(?EH_STORAGE_PROP, C)).

-define(POOL_MIN_SIZE, 2).
-define(POOL_MAX_SIZE, 25).

%% Keep this more than the max age
-define(POOL_CLEANUP_INTERVAL, 200).
-define(POOL_MAX_AGE, 100).

%%

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, pool_lifetime},
        {group, basic_api},
        {group, pool_internal}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {pool_lifetime, [
            pool_lifetime_test,
            pool_not_found_test
        ]},
        {basic_api, [
            get_request_success_test,
            post_request_success_test,
            transaction_success_test,
            invalid_domain_test,
            connection_refused_test
        ]},
        {pool_internal, [
            pool_unavailable_test,
            pool_cleanup_test,
            pool_dead_connection_test,
            pool_dead_client_test,
            pool_group_separation_test,
            pool_group_reuse_test,
            pool_no_freeing_unlocked_test,
            pool_no_double_free_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = [application:ensure_all_started(App) || App <- [cowboy, gunner]],
    {ok, EventStorage} = gunner_test_event_h:start_storage(),
    _ = start_mock_server(),
    C ++ [{?EH_STORAGE_PROP, EventStorage}, {apps, [App || {ok, App} <- Apps]}].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    ok = gunner_test_event_h:stop_storage(?EH_STORAGE(C)),
    _ = stop_mock_server(),
    _ = lists:foreach(fun(App) -> application:stop(App) end, proplists:get_value(apps, C)),
    ok.

%%

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_Name, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_Name, _C) ->
    ok.

%%

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    {ok, PoolPid} = gunner:start_pool(#{
        cleanup_interval => ?POOL_CLEANUP_INTERVAL,
        max_connection_idle_age => ?POOL_MAX_AGE,
        max_size => ?POOL_MAX_SIZE,
        min_size => ?POOL_MIN_SIZE,
        event_handler => gunner_test_event_h:make_event_h(?EH_ID, ?EH_STORAGE(C))
    }),
    C ++ [{?POOL_PID_PROP, PoolPid}].

-spec end_per_testcase(test_case_name(), config()) -> _.
end_per_testcase(_Name, C) ->
    ok = gunner:stop_pool(?POOL_PID(C)),
    ok.

%%

-spec pool_lifetime_test(config()) -> test_return().
pool_lifetime_test(C) ->
    PoolRef = {local, test_gunner_pool},
    PoolOpts = #{event_handler => gunner_test_event_h:make_event_h(lifetime_test, ?EH_STORAGE(C))},
    {ok, Pid} = gunner:start_pool(PoolRef, PoolOpts),
    [?pool_init(PoolOpts)] = pop_events(C, lifetime_test),
    ?assertEqual({error, already_exists}, gunner:start_pool(PoolRef, PoolOpts)),
    [] = pop_events(C, lifetime_test),
    ?assertEqual(ok, gunner:stop_pool(Pid)),
    [?pool_terminate(_)] = pop_events(C, lifetime_test).

-spec pool_not_found_test(config()) -> test_return().
pool_not_found_test(_C) ->
    ?assertEqual({error, pool_not_found}, gunner:get(what, valid_host(), <<"/">>)).

%%

-spec get_request_success_test(config()) -> test_return().
get_request_success_test(C) ->
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    Endpoint = valid_host(),
    ClientPid = self(),
    ?assertMatch({ok, 200, _, <<"ok/", Tag/binary>>}, gunner:get(?POOL_PID(C), Endpoint, <<"/", Tag/binary>>)),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, _)),
            ?EV_MATCH(?connection_init_finished(Endpoint, _, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, _))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec post_request_success_test(config()) -> test_return().
post_request_success_test(C) ->
    Tag = list_to_binary(integer_to_list(erlang:unique_integer())),
    Endpoint = valid_host(),
    ClientPid = self(),
    ?assertMatch(
        {ok, 200, _, <<"ok/", Tag/binary>>},
        gunner:post(?POOL_PID(C), Endpoint, <<"/", Tag/binary>>, <<"TEST">>)
    ),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, _)),
            ?EV_MATCH(?connection_init_finished(Endpoint, _, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, _))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec transaction_success_test(config()) -> test_return().
transaction_success_test(C) ->
    Endpoint = valid_host(),
    ClientPid = self(),
    ?assertEqual({ok, ok}, gunner:transaction(?POOL_PID(C), Endpoint, #{}, fun(_ConnectionPid) -> ok end)),
    [
        ?acquire_started(Endpoint, ClientPid, true),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?connection_locked(Endpoint, ClientPid, ConnectionPid),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid),
        ?free_started(Endpoint, ClientPid, ConnectionPid),
        ?connection_unlocked(Endpoint, ClientPid, ConnectionPid),
        ?free_finished(Endpoint, ClientPid, ConnectionPid, ok)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, true)),
            ?EV_MATCH(?connection_init_started(Endpoint, _)),
            ?EV_MATCH(?connection_init_finished(Endpoint, _, ok)),
            ?EV_MATCH(?connection_locked(Endpoint, ClientPid, _)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, _)),
            ?EV_MATCH(?free_started(Endpoint, ClientPid, _)),
            ?EV_MATCH(?connection_unlocked(Endpoint, ClientPid, _)),
            ?EV_MATCH(?free_finished(Endpoint, ClientPid, _, ok))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec invalid_domain_test(config()) -> test_return().
invalid_domain_test(C) ->
    Endpoint = {"nxdomain.land", 8080},
    ClientPid = self(),
    Error = {connection_failed, {shutdown, nxdomain}},
    ?assertEqual({error, Error}, gunner:get(?POOL_PID(C), Endpoint, <<"/">>)),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, {error, Error}),
        ?acquire_finished_error(ClientPid, Error)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, _)),
            ?EV_MATCH(?connection_init_finished(Endpoint, _, {error, Error})),
            ?EV_MATCH(?acquire_finished_error(ClientPid, Error))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec connection_refused_test(config()) -> test_return().
connection_refused_test(C) ->
    Endpoint = {"localhost", 8090},
    ClientPid = self(),
    Error = {connection_failed, {shutdown, econnrefused}},
    ?assertEqual({error, Error}, gunner:get(?POOL_PID(C), Endpoint, <<"/">>)),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, {error, Error}),
        ?acquire_finished_error(ClientPid, Error)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, _)),
            ?EV_MATCH(?connection_init_finished(Endpoint, _, {error, Error})),
            ?EV_MATCH(?acquire_finished_error(ClientPid, Error))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_unavailable_test(config()) -> test_return().
pool_unavailable_test(C) ->
    ClientPid = self(),
    Endpoint = {"localhost", 8080},
    _Connections = [gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000) || _X <- lists:seq(1, ?POOL_MAX_SIZE)],
    {error, pool_unavailable} = gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000),
    [
        ?acquire_started(Endpoint, ClientPid, true),
        ?acquire_finished_error(ClientPid, pool_unavailable)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, true)),
            ?EV_MATCH(?acquire_finished_error(ClientPid, pool_unavailable))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_cleanup_test(config()) -> test_return().
pool_cleanup_test(C) ->
    Endpoint = {"localhost", 8080},
    Connections = [gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000) || _X <- lists:seq(1, ?POOL_MAX_SIZE)],
    [
        ?cleanup_started(?POOL_MAX_SIZE),
        ?cleanup_finished(?POOL_MAX_SIZE)
    ] = wait_events(
        [
            ?EV_MATCH(?cleanup_started(?POOL_MAX_SIZE)),
            ?EV_MATCH(?cleanup_finished(?POOL_MAX_SIZE))
        ],
        #{ignore_cleanups => false},
        C,
        ?POOL_CLEANUP_INTERVAL
    ),
    _ = [gunner_pool:free(?POOL_PID(C), ConnectionPid) || {ok, ConnectionPid} <- Connections],
    [
        ?cleanup_started(?POOL_MAX_SIZE),
        ?cleanup_finished(?POOL_MIN_SIZE)
    ] = wait_events(
        [
            ?EV_MATCH(?cleanup_started(?POOL_MAX_SIZE)),
            ?EV_MATCH(?cleanup_finished(?POOL_MIN_SIZE))
        ],
        #{ignore_cleanups => false},
        C,
        ?POOL_CLEANUP_INTERVAL
    ),
    ok.

-spec pool_dead_connection_test(config()) -> test_return().
pool_dead_connection_test(C) ->
    Endpoint = {"localhost", 8080},
    ClientPid = self(),
    {ok, ConnectionPid} = gunner_pool:acquire(?POOL_PID(C), Endpoint, false, 1000),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, ConnectionPid)),
            ?EV_MATCH(?connection_init_finished(Endpoint, ConnectionPid, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok = proc_lib:stop(ConnectionPid, normal, 1000),
    [
        ?connection_down(Endpoint, ConnectionPid, _)
    ] = wait_events(
        [
            ?EV_MATCH(?connection_down(Endpoint, ConnectionPid, _))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_dead_client_test(config()) -> test_return().
pool_dead_client_test(C) ->
    Endpoint = {"localhost", 8080},
    {ClientPid, ConnectionPid} = client_process(fun() ->
        {ok, ConnPid} = gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000),
        {self(), ConnPid}
    end),
    [
        ?acquire_started(Endpoint, ClientPid, true),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?connection_locked(Endpoint, ClientPid, ConnectionPid),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid),
        ?connection_unlocked(Endpoint, ClientPid, ConnectionPid),
        ?client_down(ClientPid, _)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, true)),
            ?EV_MATCH(?connection_init_started(Endpoint, ConnectionPid)),
            ?EV_MATCH(?connection_init_finished(Endpoint, ConnectionPid, ok)),
            ?EV_MATCH(?connection_locked(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?connection_unlocked(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?client_down(ClientPid, _))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_group_separation_test(config()) -> test_return().
pool_group_separation_test(C) ->
    ClientPid = self(),
    Endpoint1 = {"localhost", 8080},
    Endpoint2 = {"localhost", 8086},
    {ok, ConnectionPid1} = gunner_pool:acquire(?POOL_PID(C), Endpoint1, false, 1000),
    {ok, ConnectionPid2} = gunner_pool:acquire(?POOL_PID(C), Endpoint2, false, 1000),
    ?assertNotEqual(ConnectionPid1, ConnectionPid2),
    [
        ?acquire_started(Endpoint1, ClientPid, false),
        ?connection_init_started(Endpoint1, ConnectionPid1),
        ?connection_init_finished(Endpoint1, ConnectionPid1, ok),
        ?acquire_finished_ok(Endpoint1, ClientPid, ConnectionPid1),
        ?acquire_started(Endpoint2, ClientPid, false),
        ?connection_init_started(Endpoint2, ConnectionPid2),
        ?connection_init_finished(Endpoint2, ConnectionPid2, ok),
        ?acquire_finished_ok(Endpoint2, ClientPid, ConnectionPid2)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint1, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint1, ConnectionPid1)),
            ?EV_MATCH(?connection_init_finished(Endpoint1, ConnectionPid1, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint1, ClientPid, ConnectionPid1)),
            ?EV_MATCH(?acquire_started(Endpoint2, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint2, ConnectionPid2)),
            ?EV_MATCH(?connection_init_finished(Endpoint2, ConnectionPid2, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint2, ClientPid, ConnectionPid2))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_group_reuse_test(config()) -> test_return().
pool_group_reuse_test(C) ->
    ClientPid = self(),
    Endpoint = {"localhost", 8080},
    {ok, ConnectionPid1} = gunner_pool:acquire(?POOL_PID(C), Endpoint, false, 1000),
    {ok, ConnectionPid2} = gunner_pool:acquire(?POOL_PID(C), Endpoint, false, 1000),
    ?assertEqual(ConnectionPid1, ConnectionPid2),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid1),
        ?connection_init_finished(Endpoint, ConnectionPid1, ok),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid1),
        ?acquire_started(Endpoint, ClientPid, false),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid2)
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, ConnectionPid1)),
            ?EV_MATCH(?connection_init_finished(Endpoint, ConnectionPid1, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid1)),
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid2))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_no_freeing_unlocked_test(config()) -> test_return().
pool_no_freeing_unlocked_test(C) ->
    ClientPid = self(),
    Endpoint = {"localhost", 8080},
    {ok, ConnectionPid} = gunner_pool:acquire(?POOL_PID(C), Endpoint, false, 1000),
    ok = gunner_pool:free(?POOL_PID(C), ConnectionPid),
    [
        ?acquire_started(Endpoint, ClientPid, false),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid),
        ?free_started(Endpoint, ClientPid, ConnectionPid),
        ?free_finished(Endpoint, ClientPid, ConnectionPid, {error, not_locked})
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, false)),
            ?EV_MATCH(?connection_init_started(Endpoint, ConnectionPid)),
            ?EV_MATCH(?connection_init_finished(Endpoint, ConnectionPid, ok)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?free_started(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?free_finished(Endpoint, ClientPid, ConnectionPid, {error, not_locked}))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

-spec pool_no_double_free_test(config()) -> test_return().
pool_no_double_free_test(C) ->
    ClientPid = self(),
    Endpoint = {"localhost", 8080},
    {ok, ConnectionPid} = gunner_pool:acquire(?POOL_PID(C), Endpoint, true, 1000),
    ok = gunner_pool:free(?POOL_PID(C), ConnectionPid),
    ok = gunner_pool:free(?POOL_PID(C), ConnectionPid),
    [
        ?acquire_started(Endpoint, ClientPid, true),
        ?connection_init_started(Endpoint, ConnectionPid),
        ?connection_init_finished(Endpoint, ConnectionPid, ok),
        ?connection_locked(Endpoint, ClientPid, ConnectionPid),
        ?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid),
        ?free_started(Endpoint, ClientPid, ConnectionPid),
        ?connection_unlocked(Endpoint, ClientPid, ConnectionPid),
        ?free_finished(Endpoint, ClientPid, ConnectionPid, ok),
        ?free_started(Endpoint, ClientPid, ConnectionPid),
        ?free_finished(Endpoint, ClientPid, ConnectionPid, {error, not_locked})
    ] = wait_events(
        [
            ?EV_MATCH(?acquire_started(Endpoint, ClientPid, true)),
            ?EV_MATCH(?connection_init_started(Endpoint, ConnectionPid)),
            ?EV_MATCH(?connection_init_finished(Endpoint, ConnectionPid, ok)),
            ?EV_MATCH(?connection_locked(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?acquire_finished_ok(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?free_started(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?connection_unlocked(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?free_finished(Endpoint, ClientPid, ConnectionPid, ok)),
            ?EV_MATCH(?free_started(Endpoint, ClientPid, ConnectionPid)),
            ?EV_MATCH(?free_finished(Endpoint, ClientPid, ConnectionPid, {error, not_locked}))
        ],
        #{ignore_cleanups => false},
        C
    ),
    ok.

%%

wait_events(MatchFns, Opts, C) ->
    wait_events(MatchFns, Opts, C, 1000).

wait_events(MatchFns, Opts, C, Timeout) ->
    wait_events(?EH_ID, MatchFns, Opts, C, Timeout).

wait_events(ID, MatchFns, Opts, C, Timeout) ->
    wait_events(ID, MatchFns, [], [], [], Timeout, Opts, C).

wait_events(_ID, [], _UnmatchedEvents, MatchedEvents, _RcvEvents, _Timeout, _Opts, _C) ->
    lists:reverse(MatchedEvents);
wait_events(_ID, _MatchFns, _UnmatchedEvents, MatchedEvents, RcvEvents, Timeout, _Opts, _C) when Timeout =< 0 ->
    throw({timeout, {RcvEvents, MatchedEvents}});
wait_events(ID, MatchFns, [], MatchedEvents, RcvEvents, Timeout, Opts, C) ->
    Time0 = erlang:monotonic_time(millisecond),
    NewEvents = pop_events(C, Opts, ID),
    Time = erlang:monotonic_time(millisecond) - Time0,
    wait_events(ID, MatchFns, NewEvents, MatchedEvents, RcvEvents ++ NewEvents, Timeout - Time, Opts, C);
wait_events(ID, [MatchFn | FnRest] = MatchFns, [UnmatchedEv | EvTail], MatchedEvents, RcvEvents, Timeout, Opts, C) ->
    case MatchFn(UnmatchedEv) of
        true ->
            wait_events(ID, FnRest, EvTail, [UnmatchedEv | MatchedEvents], RcvEvents, Timeout, Opts, C);
        false ->
            wait_events(ID, MatchFns, EvTail, MatchedEvents, RcvEvents, Timeout, Opts, C)
    end.

pop_events(C, Opts) when is_map(Opts) ->
    pop_events(C, Opts, ?EH_ID);
pop_events(C, ID) when is_atom(ID) ->
    pop_events(C, #{}, ID).

pop_events(C, Opts, ID) ->
    {ok, Events} = gunner_test_event_h:pop_events(?EH_STORAGE(C), ID),
    filter_events(Events, Opts).

filter_events(Events, #{ignore_cleanups := true}) ->
    lists:filter(
        fun
            (?cleanup_started(_)) -> false;
            (?cleanup_finished(_)) -> false;
            (_) -> true
        end,
        Events
    );
filter_events(Events, _Opts) ->
    Events.

client_process(Fun) ->
    client_process(Fun, 1000).

client_process(Fun, Timeout) ->
    Self = self(),
    _ = spawn_link(fun() ->
        Result =
            try
                {result, Fun()}
            catch
                Error:Reason:Stacktrace ->
                    {caught, {Error, Reason, Stacktrace}}
            end,
        Self ! Result
    end),
    receive
        {result, Result} ->
            Result;
        {caught, {Error, Reason, Stacktrace}} ->
            erlang:raise(Error, Reason, Stacktrace)
    after Timeout -> {error, timeout}
    end.

valid_host() ->
    Hosts = [
        {"localhost", 8080},
        {"localhost", 8086}
    ],
    lists:nth(rand:uniform(length(Hosts)), Hosts).

start_mock_server() ->
    start_mock_server(fun(#{path := Path}) ->
        {200, #{}, <<"ok", Path/binary>>}
    end).

start_mock_server(HandlerFun) ->
    Opts = #{request_timeout => infinity},
    _ = mock_http_server:start(default, 8080, HandlerFun, Opts),
    _ = mock_http_server:start(alternative, 8086, HandlerFun, Opts),
    ok.

stop_mock_server() ->
    ok = mock_http_server:stop(default),
    ok = mock_http_server:stop(alternative).
