-module(gunner_pool).

%% API functions

-export([start_link/1]).
-export([acquire/3]).
-export([free/3]).
-export([pool_status/2]).

%% Gen Server callbacks

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

%% API Types

-type pool() :: pid().
-type pool_opts() :: #{
    worker_factory_handler => module()
}.

-type worker_group_id() :: term().

-type status_response() :: #{current_size := size()}.

-export_type([pool/0]).
-export_type([pool_opts/0]).
-export_type([worker_group_id/0]).
-export_type([status_response/0]).

%% Internal types

-type state() :: #{
    size := size(),
    worker_groups := worker_groups(),
    worker_leases := worker_leases(),
    worker_factory_handler := module()
}.

-type size() :: non_neg_integer().

-type worker_group() :: queue:queue().

-type worker_groups() :: #{worker_group_id() => worker_group()}.
-type worker_leases() :: #{worker() => ReturnTo :: worker_group_id()}.

-type worker() :: gunner_worker_factory:worker(_).
-type worker_args() :: gunner_worker_factory:worker_args(_).

%%

-define(DEFAULT_WORKER_FACTORY, gunner_gun_worker_factory).
-define(DEFAULT_MAX_POOL_SIZE, 25).
-define(DEFAULT_MAX_FREE_CONNECTIONS, 5).

%%
%% API functions
%%

-spec start_link(pool_opts()) -> genlib_gen:start_ret().
start_link(PoolOpts) ->
    gen_server:start_link(?MODULE, [PoolOpts], []).

-spec acquire(pool(), worker_args(), timeout()) -> {ok, worker()} | {error, pool_unavailable | {worker_init_failed, _}}.
acquire(Pool, WorkerArgs, Timeout) ->
    gen_server:call(Pool, {acquire, WorkerArgs}, Timeout).

-spec free(pool(), worker(), timeout()) -> ok | {error, invalid_worker}.
free(Pool, Worker, Timeout) ->
    gen_server:call(Pool, {free, Worker}, Timeout).

-spec pool_status(pool(), timeout()) -> status_response().
pool_status(Pool, Timeout) ->
    gen_server:call(Pool, status, Timeout).

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolOpts]) ->
    {ok, new_state(PoolOpts)}.

-spec handle_call
    ({acquire, worker_args()}, _From, state()) ->
        {reply, {ok, worker()} | {error, pool_unavailable | {worker_init_failed, _}}, state()};
    ({free, worker()}, _From, state()) -> {reply, ok | {error, invalid_worker}, state()}.
handle_call({acquire, WorkerArgs0}, _From, St0 = #{worker_factory_handler := FactoryHandler}) ->
    {GroupID, WorkerArgs1} = gunner_worker_factory:on_acquire(FactoryHandler, WorkerArgs0),
    St1 = ensure_group_exists(GroupID, St0),
    try maybe_expand_pool(GroupID, WorkerArgs1, St1) of
        {ok, St2} ->
            Group0 = get_state_worker_group(GroupID, St2),
            {Worker, Group1} = get_next_worker(Group0),
            St3 = update_state_worker_group(GroupID, Group1, St2),

            Leases0 = get_worker_leases(St3),
            Leases1 = add_new_lease(Worker, GroupID, Leases0),
            St4 = update_worker_leases(Leases1, St3),

            {reply, {ok, Worker}, St4}
    catch
        throw:pool_unavailable ->
            {reply, {error, pool_unavailable}, St0};
        throw:{worker_init_failed, Reason} ->
            {reply, {error, {worker_init_failed, Reason}}, St0}
    end;
handle_call({free, Worker}, _From, St0) ->
    Leases0 = get_worker_leases(St0),
    case get_existing_lease(Worker, Leases0) of
        {ok, ReturnTo} ->
            Group0 = get_state_worker_group(ReturnTo, St0),
            Group1 = add_worker_to_group(Worker, Group0),
            St1 = update_state_worker_group(ReturnTo, Group1, St0),

            Leases1 = remove_lease(Worker, Leases0),
            St2 = update_worker_leases(Leases1, St1),

            St3 = maybe_shrink_pool(ReturnTo, St2),
            {reply, ok, St3};
        {error, not_leased} ->
            {reply, {error, invalid_worker}, St0}
    end;
handle_call(status, _From, St0) ->
    {reply, get_pool_status(St0), St0};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({'DOWN', _Mref, process, Worker, _Reason}, St0) ->
    St1 = remove_worker_by_pid(Worker, St0),
    {noreply, St1};
handle_info(_, St0) ->
    {noreply, St0}.

%%
%% Internal functions
%%

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    #{
        size => 0,
        worker_groups => #{},
        worker_leases => #{},
        worker_factory_handler => maps:get(worker_factory_handler, Opts, ?DEFAULT_WORKER_FACTORY)
    }.

-spec get_pool_status(state()) -> status_response().
get_pool_status(#{size := Size}) ->
    #{current_size => Size}.

-spec ensure_group_exists(worker_group_id(), state()) -> state().
ensure_group_exists(GroupID, St0 = #{worker_groups := Groups0}) ->
    case check_group_exists(GroupID, Groups0) of
        true ->
            St0;
        false ->
            Groups1 = create_group(GroupID, Groups0),
            St0#{worker_groups := Groups1}
    end.

-spec maybe_expand_pool(worker_group_id(), worker_args(), state()) -> {ok, state()} | no_return().
maybe_expand_pool(GroupID, WorkerArgs, St0 = #{size := PoolSize, worker_factory_handler := FactoryHandler}) ->
    Group0 = get_state_worker_group(GroupID, St0),
    case group_needs_expansion(Group0) of
        true ->
            _ = assert_pool_available(PoolSize, St0),
            Group1 = try_expand_group(WorkerArgs, Group0, FactoryHandler),
            St1 = update_state_worker_group(GroupID, Group1, St0),
            {ok, St1#{size => PoolSize + 1}};
        false ->
            {ok, St0}
    end.

-spec assert_pool_available(size(), state()) -> ok | no_return().
assert_pool_available(PoolSize, _St0) ->
    case get_max_pool_size() of
        MaxPoolSize when PoolSize < MaxPoolSize ->
            ok;
        _ ->
            throw(pool_unavailable)
    end.

-spec try_expand_group(worker_args(), worker_group(), module()) -> worker_group() | no_return().
try_expand_group(WorkerArgs, Group0, FactoryHandler) ->
    case gunner_worker_factory:create_worker(FactoryHandler, WorkerArgs) of
        {ok, Worker} ->
            add_worker_to_group(Worker, Group0);
        {error, Reason} ->
            throw({worker_init_failed, Reason})
    end.

-spec group_needs_expansion(worker_group()) -> boolean().
group_needs_expansion(Group) ->
    is_worker_group_empty(Group).

-spec is_worker_group_empty(worker_group()) -> boolean().
is_worker_group_empty(Group) ->
    queue:is_empty(Group).

-spec check_group_exists(worker_group_id(), worker_groups()) -> boolean().
check_group_exists(GroupID, Groups) ->
    maps:is_key(GroupID, Groups).

-spec create_group(worker_group_id(), worker_groups()) -> worker_groups().
create_group(GroupID, Groups0) ->
    Groups0#{GroupID => queue:new()}.

-spec get_state_worker_group(worker_group_id(), state()) -> worker_group().
get_state_worker_group(GroupID, #{worker_groups := Groups0}) ->
    maps:get(GroupID, Groups0).

-spec get_next_worker(worker_group()) -> {worker(), worker_group()}.
get_next_worker(Group0) ->
    {{value, Worker}, Group1} = queue:out(Group0),
    {Worker, Group1}.

-spec add_worker_to_group(worker(), worker_group()) -> worker_group().
add_worker_to_group(Worker, Group0) ->
    queue:in(Worker, Group0).

-spec update_state_worker_group(worker_group_id(), worker_group(), state()) -> state().
update_state_worker_group(GroupID, Group, St0 = #{worker_groups := Groups0}) ->
    Groups1 = Groups0#{GroupID => Group},
    St0#{worker_groups => Groups1}.

-spec get_worker_leases(state()) -> worker_leases().
get_worker_leases(#{worker_leases := Leases}) ->
    Leases.

-spec get_existing_lease(worker(), worker_leases()) -> {ok, worker_group_id()} | {error, not_leased}.
get_existing_lease(Worker, Leases0) ->
    case maps:get(Worker, Leases0, undefined) of
        ReturnTo when ReturnTo =/= undefined ->
            {ok, ReturnTo};
        undefined ->
            {error, not_leased}
    end.

-spec add_new_lease(worker(), ReturnTo :: worker_group_id(), worker_leases()) -> worker_leases().
add_new_lease(Worker, ReturnTo, Leases0) ->
    Leases0#{Worker => ReturnTo}.

-spec remove_lease(worker(), worker_leases()) -> worker_leases().
remove_lease(Worker, Leases0) ->
    maps:without([Worker], Leases0).

-spec update_worker_leases(worker_leases(), state()) -> state().
update_worker_leases(Leases, St0) ->
    St0#{worker_leases => Leases}.

-spec maybe_shrink_pool(worker_group_id(), state()) -> state().
maybe_shrink_pool(GroupID, St0 = #{size := PoolSize, worker_factory_handler := FactoryHandler}) ->
    Group0 = get_state_worker_group(GroupID, St0),
    case group_needs_shrinking(Group0) of
        true ->
            Group1 = do_shrink_group(Group0, FactoryHandler),
            St1 = update_state_worker_group(GroupID, Group1, St0),
            St1#{size => PoolSize - 1};
        false ->
            St0
    end.

-spec group_needs_shrinking(worker_group()) -> boolean().
group_needs_shrinking(Group) ->
    MaxFreeGroupConnections = get_max_free_connections(),
    is_worker_group_full(Group, MaxFreeGroupConnections).

-spec is_worker_group_full(worker_group(), Max :: non_neg_integer()) -> boolean().
is_worker_group_full(Group, MaxFreeGroupConnections) ->
    queue:len(Group) > MaxFreeGroupConnections.

-spec do_shrink_group(worker_group(), module()) -> worker_group().
do_shrink_group(Group0, FactoryHandler) ->
    {Worker, Group1} = get_next_worker(Group0),
    ok = gunner_worker_factory:exit_worker(FactoryHandler, Worker),
    Group1.

get_max_pool_size() ->
    genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE).

get_max_free_connections() ->
    genlib_app:env(gunner, group_max_free_connections, ?DEFAULT_MAX_FREE_CONNECTIONS).

%%

-spec remove_worker_by_pid(worker(), state()) -> state().
remove_worker_by_pid(WorkerPID, St0 = #{worker_groups := Groups0, worker_leases := Leases0}) ->
    Leases1 = remove_lease(WorkerPID, Leases0),
    Groups1 = maps:map(
        fun(_K, V) ->
            queue:filter(fun(W) -> WorkerPID =/= W end, V)
        end,
        Groups0
    ),
    St0#{worker_groups => Groups1, worker_leases => Leases1}.
