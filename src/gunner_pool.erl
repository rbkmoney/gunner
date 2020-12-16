-module(gunner_pool).

%% API functions

-export([start_link/1]).

-export([acquire/3]).
-export([cancel_acquire/1]).

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
    clients := clients(),
    worker_factory_handler := module()
}.

-type worker_groups() :: #{
    worker_group_id() => worker_group()
}.

-type worker_group() :: gunner_pool_worker_group:state().

-type clients() :: #{
    client_pid() => client_state()
}.

-type client_pid() :: pid().
-type client_state() :: gunner_pool_client_state:state().

-type size() :: non_neg_integer().

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

-spec cancel_acquire(pool()) -> ok.
cancel_acquire(Pool) ->
    gen_server:cast(Pool, {cancel_acquire, self()}).

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
handle_call({acquire, WorkerArgs0}, {ClientPid, _}, St0 = #{worker_factory_handler := FactoryHandler}) ->
    {GroupID, WorkerArgs1} = gunner_worker_factory:on_acquire(FactoryHandler, WorkerArgs0),
    St1 = ensure_group_exists(GroupID, St0),
    try
        St2 = maybe_expand_pool(GroupID, WorkerArgs1, St1),
        {Worker, St3} = handle_acquire(GroupID, ClientPid, St2),
        {reply, {ok, Worker}, St3}
    catch
        throw:pool_unavailable ->
            {reply, {error, pool_unavailable}, St0};
        throw:{worker_init_failed, Reason} ->
            {reply, {error, {worker_init_failed, Reason}}, St0}
    end;
handle_call({free, Worker}, {ClientPid, _}, St0) ->
    try handle_free(Worker, ClientPid, St0) of
        St1 ->
            {reply, ok, St1}
    catch
        throw:no_client_state ->
            {reply, {error, {client, not_found}}, St0};
        throw:not_leased ->
            {reply, {error, {worker, not_leased}}, St0}
    end;
handle_call(status, _From, St0) ->
    {reply, get_pool_status(St0), St0};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast({cancel_acquire, client_pid()}, state()) -> {noreply, state()}.
handle_cast({cancel_acquire, ClientPid}, St0) ->
    {noreply, handle_cancel(ClientPid, St0)};
handle_cast(_Cast, _St) ->
    erlang:error(unexpected_cast).

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({'DOWN', _Mref, process, Pid, _Reason}, St0) ->
    St1 = handle_process_down(Pid, St0),
    {noreply, St1};
handle_info(_, St0) ->
    {noreply, St0}.

%%
%% Internal functions
%%

-spec handle_acquire(worker_group_id(), pid(), state()) -> {worker(), state()} | no_return().
handle_acquire(GroupID, ClientPid, St0 = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    {Worker, WorkerGroups1} = fetch_next_worker(GroupID, WorkerGroups0),
    Clients1 = register_client_lease(ClientPid, Worker, GroupID, Clients0),
    {Worker, St0#{worker_groups => WorkerGroups1, clients => Clients1}}.

-spec fetch_next_worker(worker_group_id(), worker_groups()) -> {worker(), worker_groups()}.
fetch_next_worker(GroupID, WorkerGroups) ->
    Group0 = get_worker_group(GroupID, WorkerGroups),
    {Worker, Group1} = get_next_worker(Group0),
    {Worker, update_worker_group(GroupID, Group1, WorkerGroups)}.

-spec get_worker_group(worker_group_id(), worker_groups()) -> worker_group().
get_worker_group(GroupID, WorkerGroups) ->
    maps:get(GroupID, WorkerGroups).

-spec update_worker_group(worker_group_id(), worker_group(), worker_groups()) -> worker_groups().
update_worker_group(GroupID, Group, WorkerGroups) ->
    WorkerGroups#{GroupID => Group}.

-spec register_client_lease(client_pid(), worker(), worker_group_id(), clients()) -> clients().
register_client_lease(ClientPid, Worker, GroupID, Clients0) ->
    Clients1 = ensure_client_state_exists(ClientPid, Clients0),
    ClientSt0 = get_client_state(ClientPid, Clients1),
    ClientSt1 = register_new_lease(Worker, GroupID, ClientSt0),
    update_client_state(ClientPid, ClientSt1, Clients1).

-spec ensure_client_state_exists(client_pid(), clients()) -> clients().
ensure_client_state_exists(ClientPid, Clients) ->
    case check_client_state_exists(ClientPid, Clients) of
        true ->
            Clients;
        false ->
            add_new_client_state(ClientPid, Clients)
    end.

-spec check_client_state_exists(client_pid(), clients()) -> boolean().
check_client_state_exists(ClientPid, Clients) ->
    maps:is_key(ClientPid, Clients).

-spec add_new_client_state(client_pid(), clients()) -> clients().
add_new_client_state(ClientPid, Clients0) ->
    _ = erlang:monitor(process, ClientPid),
    Clients0#{ClientPid => gunner_pool_client_state:new()}.

-spec get_client_state(client_pid(), clients()) -> client_state() | undefined.
get_client_state(ClientPid, Clients) ->
    maps:get(ClientPid, Clients, undefined).

-spec remove_client_state(client_pid(), clients()) -> clients().
remove_client_state(ClientPid, Clients) ->
    maps:without([ClientPid], Clients).

-spec register_new_lease(worker(), worker_group_id(), client_state()) -> client_state().
register_new_lease(Worker, GroupID, ClientSt) ->
    gunner_pool_client_state:register_lease(Worker, GroupID, ClientSt).

-spec update_client_state(client_pid(), client_state(), clients()) -> clients().
update_client_state(ClientPid, ClientState, Clients) ->
    Clients#{ClientPid => ClientState}.

%%

-spec handle_free(worker(), client_pid(), state()) -> state().
handle_free(Worker, ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    {GroupID, Clients1} = return_lease(Worker, ClientPid, Clients0),
    WorkerGroups1 = return_worker(Worker, GroupID, WorkerGroups0),
    maybe_shrink_pool(GroupID, St#{worker_groups => WorkerGroups1, clients => Clients1}).

-spec try_get_client_state(client_pid(), clients()) -> client_state() | no_return().
try_get_client_state(ClientPid, Clients) ->
    case get_client_state(ClientPid, Clients) of
        ClientState when ClientState =/= undefined ->
            ClientState;
        undefined ->
            throw(no_client_state)
    end.

-spec return_lease(worker(), client_pid(), clients()) -> {worker_group_id(), clients()} | no_return().
return_lease(Worker, ClientPid, Clients) ->
    ClientState0 = try_get_client_state(ClientPid, Clients),
    {GroupID, ClientState1} = do_return_lease(Worker, ClientState0),
    {GroupID, update_client_state(ClientPid, ClientState1, Clients)}.

-spec do_return_lease(worker(), client_state()) -> {worker_group_id(), client_state()} | no_return().
do_return_lease(Worker, ClientState) ->
    case gunner_pool_client_state:return_lease(Worker, ClientState) of
        {ok, WorkerGroupId, NewClientState} ->
            {WorkerGroupId, NewClientState};
        {error, _} ->
            throw(not_leased)
    end.

-spec return_worker(worker(), worker_group_id(), worker_groups()) -> worker_groups().
return_worker(Worker, GroupID, WorkerGroups) ->
    Group0 = get_worker_group(GroupID, WorkerGroups),
    Group1 = add_worker_to_group(Worker, Group0),
    update_worker_group(GroupID, Group1, WorkerGroups).

%%

handle_cancel(ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    {Worker, GroupID, Clients1} = cancel_lease(ClientPid, Clients0),
    WorkerGroups1 = return_worker(Worker, GroupID, WorkerGroups0),
    maybe_shrink_pool(GroupID, St#{worker_groups => WorkerGroups1, clients => Clients1}).

-spec cancel_lease(client_pid(), clients()) -> {worker(), worker_group_id(), clients()} | no_return().
cancel_lease(ClientPid, Clients) ->
    ClientState0 = try_get_client_state(ClientPid, Clients),
    {Worker, GroupID, ClientState1} = do_cancel_lease(ClientState0),
    {Worker, GroupID, update_client_state(ClientPid, ClientState1, Clients)}.

-spec do_cancel_lease(client_state()) -> {worker(), worker_group_id(), client_state()} | no_return().
do_cancel_lease(ClientState) ->
    case gunner_pool_client_state:cancel_lease(ClientState) of
        {ok, Worker, WorkerGroupId, NewClientState} ->
            {Worker, WorkerGroupId, NewClientState};
        {error, _} ->
            throw(not_leased)
    end.

%%

-spec handle_process_down(worker() | client_pid(), state()) -> state().
handle_process_down(Pid, St) ->
    handle_process_down(determine_process_type(Pid, St), Pid, St).

-spec determine_process_type(worker() | client_pid(), state()) -> worker | client.
determine_process_type(Pid, #{clients := Clients}) ->
    case check_client_state_exists(Pid, Clients) of
        true ->
            client;
        false ->
            worker
    end.

-spec handle_process_down
    (client, client_pid(), state()) -> state();
    (worker, worker(), state()) -> state().
handle_process_down(client, ClientPid, St = #{clients := Clients0}) ->
    ClientSt0 = get_client_state(ClientPid, Clients0),
    {Leases, _ClientSt1} = gunner_pool_client_state:purge_leases(ClientSt0),
    return_all_leases(Leases, St#{clients => remove_client_state(ClientPid, Clients0)});
handle_process_down(worker, Worker, St = #{worker_groups := Groups0}) ->
    Groups1 = maps:map(
        fun(_K, Group) ->
            gunner_pool_worker_group:delete_worker(Worker, Group)
        end,
        Groups0
    ),
    St#{worker_groups => Groups1}.

return_all_leases([], St) ->
    St;
return_all_leases([{Worker, WorkerGroupID} | Rest], St = #{worker_groups := WorkerGroups0}) ->
    WorkerGroups1 = return_worker(Worker, WorkerGroupID, WorkerGroups0),
    St1 = maybe_shrink_pool(WorkerGroupID, St#{worker_groups => WorkerGroups1}),
    return_all_leases(Rest, St1).

%%

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    #{
        size => 0,
        worker_groups => #{},
        clients => #{},
        worker_factory_handler => maps:get(worker_factory_handler, Opts, ?DEFAULT_WORKER_FACTORY)
    }.

%%

-spec create_worker_group(worker_group_id(), worker_groups()) -> worker_groups().
create_worker_group(GroupID, Groups) ->
    Groups#{GroupID => gunner_pool_worker_group:new()}.

-spec is_worker_group_empty(worker_group()) -> boolean().
is_worker_group_empty(Group) ->
    gunner_pool_worker_group:is_empty(Group).

-spec get_next_worker(worker_group()) -> {worker(), worker_group()}.
get_next_worker(Group) ->
    gunner_pool_worker_group:next_worker(Group).

-spec add_worker_to_group(worker(), worker_group()) -> worker_group().
add_worker_to_group(Worker, Group) ->
    gunner_pool_worker_group:add_worker(Worker, Group).

%%

-spec get_pool_status(state()) -> status_response().
get_pool_status(#{size := Size}) ->
    #{current_size => Size}.

-spec ensure_group_exists(worker_group_id(), state()) -> state().
ensure_group_exists(GroupID, St0 = #{worker_groups := Groups0}) ->
    case check_group_exists(GroupID, Groups0) of
        true ->
            St0;
        false ->
            Groups1 = create_worker_group(GroupID, Groups0),
            St0#{worker_groups := Groups1}
    end.

-spec maybe_expand_pool(worker_group_id(), worker_args(), state()) -> state() | no_return().
maybe_expand_pool(GroupID, WorkerArgs, St0 = #{size := PoolSize, worker_factory_handler := FactoryHandler}) ->
    Group0 = get_state_worker_group(GroupID, St0),
    case group_needs_expansion(Group0) of
        true ->
            _ = assert_pool_available(PoolSize, St0),
            Group1 = try_expand_group(WorkerArgs, Group0, FactoryHandler),
            St1 = update_state_worker_group(GroupID, Group1, St0),
            St1#{size => PoolSize + 1};
        false ->
            St0
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
            _ = erlang:monitor(process, Worker),
            add_worker_to_group(Worker, Group0);
        {error, Reason} ->
            throw({worker_init_failed, Reason})
    end.

-spec group_needs_expansion(worker_group()) -> boolean().
group_needs_expansion(Group) ->
    is_worker_group_empty(Group).

-spec check_group_exists(worker_group_id(), worker_groups()) -> boolean().
check_group_exists(GroupID, Groups) ->
    maps:is_key(GroupID, Groups).

-spec get_state_worker_group(worker_group_id(), state()) -> worker_group().
get_state_worker_group(GroupID, #{worker_groups := Groups0}) ->
    maps:get(GroupID, Groups0).

-spec update_state_worker_group(worker_group_id(), worker_group(), state()) -> state().
update_state_worker_group(GroupID, Group, St0 = #{worker_groups := Groups0}) ->
    Groups1 = Groups0#{GroupID => Group},
    St0#{worker_groups => Groups1}.

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
    gunner_pool_worker_group:size(Group) > MaxFreeGroupConnections.

-spec do_shrink_group(worker_group(), module()) -> worker_group().
do_shrink_group(Group0, FactoryHandler) ->
    {Worker, Group1} = get_next_worker(Group0),
    ok = gunner_worker_factory:exit_worker(FactoryHandler, Worker),
    Group1.

get_max_pool_size() ->
    genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE).

get_max_free_connections() ->
    genlib_app:env(gunner, group_max_free_connections, ?DEFAULT_MAX_FREE_CONNECTIONS).
