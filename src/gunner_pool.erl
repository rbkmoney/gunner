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
    worker_requests := worker_requests(),
    %@TODO remove
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

-type worker_requests() :: #{
    worker() => worker_request()
}.

-type worker_request() :: {worker_group_id(), TargetClient :: from()}.

-type size() :: non_neg_integer().

-type worker() :: gunner_worker_factory:worker(_).
-type worker_args() :: gunner_worker_factory:worker_args(_).

-type gun_client_opts() :: gun:opts().

-type from() :: {pid(), Tag :: _}.

%%

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).
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
        {reply, {ok, worker()} | {error, pool_unavailable | {worker_init_failed, _}}, state()} |
        {noreply, state()};
    ({free, worker()}, _From, state()) -> {reply, ok | {error, invalid_worker}, state()}.
handle_call({acquire, WorkerArgs}, {ClientPid, _} = From, St0) ->
    GroupID = create_group_id(WorkerArgs),
    St1 = ensure_group_exists(GroupID, St0),
    case handle_acquire(GroupID, ClientPid, St1) of
        {ok, {worker, WorkerPid}, St2} ->
            {reply, {ok, WorkerPid}, St2};
        {ok, no_worker} ->
            case handle_worker_start(GroupID, WorkerArgs, From, St1) of
                {ok, St2} ->
                    {noreply, St2};
                {error, _Reason} = Error ->
                    {reply, Error, St0}
            end;
        {error, pool_unavailable} ->
            {reply, {error, pool_unavailable}, St0}
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
handle_info({gun_up, Pid, _Protocol}, St0) ->
    St1 = handle_worker_started(Pid, St0),
    {noreply, St1};
handle_info({'DOWN', _Mref, process, Pid, Reason}, St0) ->
    St1 = handle_process_down(Pid, Reason, St0),
    {noreply, St1};
handle_info(_, St0) ->
    {noreply, St0}.

%%
%% Internal functions
%%

-spec create_group_id(worker_args()) -> worker_group_id().
create_group_id(WorkerArgs) ->
    WorkerArgs.

-spec handle_acquire(worker_group_id(), pid(), state()) ->
    {ok, {worker, worker()}, state()} | {ok, no_worker} | {error, pool_unavailable}.
handle_acquire(GroupID, ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    case fetch_next_worker(GroupID, WorkerGroups0) of
        {ok, Worker, WorkerGroups1} ->
            Clients1 = register_client_lease(ClientPid, Worker, GroupID, Clients0),
            {ok, {worker, Worker}, St#{worker_groups => WorkerGroups1, clients => Clients1}};
        {error, no_available_workers} ->
            case assert_pool_available(St) of
                ok -> {ok, no_worker};
                error -> {error, pool_unavailable}
            end
    end.

-spec fetch_next_worker(worker_group_id(), worker_groups()) ->
    {ok, worker(), worker_groups()} | {error, no_available_workers}.
fetch_next_worker(GroupID, WorkerGroups) ->
    Group0 = get_worker_group_by_id(GroupID, WorkerGroups),
    case is_worker_group_empty(Group0) of
        false ->
            {Worker, Group1} = get_next_worker(Group0),
            {ok, Worker, update_worker_group(GroupID, Group1, WorkerGroups)};
        true ->
            {error, no_available_workers}
    end.

-spec register_client_lease(client_pid(), worker(), worker_group_id(), clients()) -> clients().
register_client_lease(ClientPid, Worker, GroupID, Clients0) ->
    Clients1 = ensure_client_state_exists(ClientPid, Clients0),
    ClientSt0 = get_client_state_by_pid(ClientPid, Clients1),
    ClientSt1 = register_client_lease(Worker, GroupID, ClientSt0),
    update_client_state(ClientPid, ClientSt1, Clients1).

-spec ensure_client_state_exists(client_pid(), clients()) -> clients().
ensure_client_state_exists(ClientPid, Clients) ->
    case client_state_exists(ClientPid, Clients) of
        true ->
            Clients;
        false ->
            add_new_client_state(ClientPid, Clients)
    end.

-spec add_new_client_state(client_pid(), clients()) -> clients().
add_new_client_state(ClientPid, Clients) ->
    update_client_state(ClientPid, create_client_state(ClientPid), Clients).

-spec assert_pool_available(state()) -> ok | error.
assert_pool_available(#{size := PoolSize}) ->
    case get_max_pool_size() of
        MaxPoolSize when PoolSize < MaxPoolSize ->
            ok;
        _ ->
            error
    end.

-spec handle_worker_start(worker_group_id(), worker_args(), From :: from(), state()) ->
    {ok, state()} | {error, {worker_init_failed, Reason :: term()}}.
handle_worker_start(GroupID, WorkerArgs, From, St = #{size := PoolSize, worker_requests := WorkerRequests0}) ->
    case create_new_worker(WorkerArgs) of
        {ok, WorkerPid} ->
            _ = erlang:monitor(process, WorkerPid),
            WorkerRequests1 = add_worker_request(WorkerPid, GroupID, From, WorkerRequests0),
            {ok, St#{size => PoolSize + 1, worker_requests => WorkerRequests1}};
        {error, Reason} ->
            {error, {worker_init_failed, Reason}}
    end.

-spec create_new_worker(worker_args()) -> {ok, worker()} | {error, Reason :: any()}.
create_new_worker({Host, Port}) ->
    ClientOpts = get_gun_client_opts(),
    gun:open(Host, Port, ClientOpts#{retry => 0}).

-spec handle_worker_started(worker(), state()) -> state().
handle_worker_started(WorkerPid, St = #{worker_requests := WorkerRequests0, clients := Clients0}) ->
    case get_worker_request_by_pid(WorkerPid, WorkerRequests0) of
        {GroupID, {ClientPid, _} = From} ->
            WorkerRequests1 = remove_worker_request(WorkerPid, WorkerRequests0),
            Clients1 = register_client_lease(ClientPid, WorkerPid, GroupID, Clients0),
            ok = gen_server:reply(From, {ok, WorkerPid}),
            St#{worker_requests := WorkerRequests1, clients := Clients1};
        undefined ->
            St
    end.

-spec handle_worker_start_failed(worker(), Reason :: term(), state()) -> state().
handle_worker_start_failed(WorkerPid, Reason, St = #{size := PoolSize, worker_requests := WorkerRequests0}) ->
    case get_worker_request_by_pid(WorkerPid, WorkerRequests0) of
        {_GroupID, From} ->
            WorkerRequests1 = remove_worker_request(WorkerPid, WorkerRequests0),
            ok = gen_server:reply(From, {error, {worker_init_failed, Reason}}),
            St#{size => PoolSize - 1, worker_requests := WorkerRequests1};
        undefined ->
            St
    end.

%%

-spec handle_free(worker(), client_pid(), state()) -> state().
handle_free(Worker, ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    {GroupID, Clients1} = return_lease(Worker, ClientPid, Clients0),
    WorkerGroups1 = return_worker(Worker, GroupID, WorkerGroups0),
    maybe_shrink_pool(GroupID, St#{worker_groups => WorkerGroups1, clients => Clients1}).

-spec try_get_client_state(client_pid(), clients()) -> client_state() | no_return().
try_get_client_state(ClientPid, Clients) ->
    case get_client_state_by_pid(ClientPid, Clients) of
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
    case return_client_lease(Worker, ClientState) of
        {ok, WorkerGroupId, NewClientState} ->
            {WorkerGroupId, NewClientState};
        {error, _} ->
            throw(not_leased)
    end.

-spec return_worker(worker(), worker_group_id(), worker_groups()) -> worker_groups().
return_worker(Worker, GroupID, WorkerGroups) ->
    Group0 = get_worker_group_by_id(GroupID, WorkerGroups),
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
    case cancel_client_lease(ClientState) of
        {ok, Worker, WorkerGroupId, NewClientState} ->
            {Worker, WorkerGroupId, NewClientState};
        {error, _} ->
            throw(not_leased)
    end.

%%

-spec handle_process_down(worker() | client_pid(), Reason :: term(), state()) -> state().
handle_process_down(Pid, Reason, St) ->
    handle_process_down(determine_process_type(Pid, St), Pid, Reason, St).

-spec determine_process_type(worker() | client_pid(), state()) -> worker | client.
determine_process_type(Pid, #{clients := Clients}) ->
    case client_state_exists(Pid, Clients) of
        true ->
            client;
        false ->
            worker
    end.

-spec handle_process_down
    (client, client_pid(), Reason :: term(), state()) -> state();
    (worker, worker(), Reason :: term(), state()) -> state().
handle_process_down(client, ClientPid, _Reason, St = #{clients := Clients0}) ->
    ClientSt0 = get_client_state_by_pid(ClientPid, Clients0),
    {Leases, ClientSt1} = purge_client_leases(ClientSt0),
    ok = destroy_client_state(ClientSt1),
    return_all_leases(Leases, St#{clients => remove_client_state(ClientPid, Clients0)});
handle_process_down(worker, Worker, Reason, St = #{worker_requests := WorkerRequests0}) ->
    case worker_request_exists(Worker, WorkerRequests0) of
        true ->
            handle_worker_start_failed(Worker, Reason, St);
        false ->
            handle_worker_exit(Worker, St)
    end.

-spec handle_worker_exit(worker(), state()) -> state().
handle_worker_exit(Worker, St = #{worker_groups := Groups0}) ->
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
        worker_factory_handler => maps:get(worker_factory_handler, Opts, ?DEFAULT_WORKER_FACTORY),
        worker_requests => #{}
    }.

%% Client states

-spec get_client_state_by_pid(client_pid(), clients()) -> client_state() | undefined.
get_client_state_by_pid(ClientPid, Clients) ->
    maps:get(ClientPid, Clients, undefined).

-spec update_client_state(client_pid(), client_state(), clients()) -> clients().
update_client_state(ClientPid, ClientState, Clients) ->
    Clients#{ClientPid => ClientState}.

-spec remove_client_state(client_pid(), clients()) -> clients().
remove_client_state(ClientPid, Clients) ->
    maps:without([ClientPid], Clients).

-spec client_state_exists(client_pid(), clients()) -> boolean().
client_state_exists(ClientPid, Clients) ->
    maps:is_key(ClientPid, Clients).

-spec create_client_state(client_pid()) -> client_state().
create_client_state(ClientPid) ->
    gunner_pool_client_state:new(ClientPid).

-spec register_client_lease(worker(), worker_group_id(), client_state()) -> client_state().
register_client_lease(Worker, GroupID, ClientSt) ->
    gunner_pool_client_state:register_lease(Worker, GroupID, ClientSt).

-spec return_client_lease(worker(), client_state()) ->
    {ok, worker_group_id(), client_state()} | {error, no_leases | worker_not_found}.
return_client_lease(Worker, ClientSt) ->
    gunner_pool_client_state:return_lease(Worker, ClientSt).

-spec cancel_client_lease(client_state()) ->
    {ok, worker(), worker_group_id(), client_state()} | {error, no_leases | worker_not_found}.
cancel_client_lease(ClientSt) ->
    gunner_pool_client_state:cancel_lease(ClientSt).

-spec purge_client_leases(client_state()) -> {[{worker(), worker_group_id()}], client_state()}.
purge_client_leases(ClientSt) ->
    gunner_pool_client_state:purge_leases(ClientSt).

-spec destroy_client_state(client_state()) -> ok | {error, active_leases_present}.
destroy_client_state(ClientSt) ->
    gunner_pool_client_state:destroy(ClientSt).

%% Worker groups

-spec get_worker_group_by_id(worker_group_id(), worker_groups()) -> worker_group() | undefined.
get_worker_group_by_id(WorkerGroupID, WorkerGroups) ->
    maps:get(WorkerGroupID, WorkerGroups, undefined).

-spec update_worker_group(worker_group_id(), worker_group(), worker_groups()) -> worker_groups().
update_worker_group(GroupID, Group, WorkerGroups) ->
    WorkerGroups#{GroupID => Group}.

-spec worker_group_exists(worker_group_id(), worker_groups()) -> boolean().
worker_group_exists(WorkerGroupID, WorkerGroups) ->
    maps:is_key(WorkerGroupID, WorkerGroups).

-spec create_worker_group() -> worker_group().
create_worker_group() ->
    gunner_pool_worker_group:new().

-spec is_worker_group_empty(worker_group()) -> boolean().
is_worker_group_empty(Group) ->
    gunner_pool_worker_group:is_empty(Group).

-spec get_next_worker(worker_group()) -> {worker(), worker_group()}.
get_next_worker(Group) ->
    gunner_pool_worker_group:next_worker(Group).

-spec add_worker_to_group(worker(), worker_group()) -> worker_group().
add_worker_to_group(Worker, Group) ->
    gunner_pool_worker_group:add_worker(Worker, Group).

-spec get_worker_group_size(worker_group()) -> non_neg_integer().
get_worker_group_size(Group) ->
    gunner_pool_worker_group:size(Group).

%% Worker requests

-spec add_worker_request(worker(), worker_group_id(), From :: _, worker_requests()) -> worker_requests().
add_worker_request(WorkerPid, GroupID, From, WorkerRequests0) ->
    WorkerRequests0#{WorkerPid => {GroupID, From}}.

-spec get_worker_request_by_pid(worker(), worker_requests()) -> worker_request() | undefined.
get_worker_request_by_pid(WorkerPid, WorkerRequests) ->
    maps:get(WorkerPid, WorkerRequests, undefined).

-spec remove_worker_request(worker(), worker_requests()) -> worker_requests().
remove_worker_request(WorkerPid, WorkerRequests) ->
    maps:without([WorkerPid], WorkerRequests).

-spec worker_request_exists(worker(), worker_requests()) -> boolean().
worker_request_exists(WorkerPid, WorkerRequests) ->
    maps:is_key(WorkerPid, WorkerRequests).

%%

-spec get_pool_status(state()) -> status_response().
get_pool_status(#{size := Size}) ->
    #{current_size => Size}.

-spec ensure_group_exists(worker_group_id(), state()) -> state().
ensure_group_exists(GroupID, St0 = #{worker_groups := Groups0}) ->
    case worker_group_exists(GroupID, Groups0) of
        true ->
            St0;
        false ->
            Groups1 = update_worker_group(GroupID, create_worker_group(), Groups0),
            St0#{worker_groups := Groups1}
    end.

-spec get_state_worker_group(worker_group_id(), state()) -> worker_group().
get_state_worker_group(GroupID, #{worker_groups := Groups}) ->
    get_worker_group_by_id(GroupID, Groups).

-spec update_state_worker_group(worker_group_id(), worker_group(), state()) -> state().
update_state_worker_group(GroupID, Group, St = #{worker_groups := Groups}) ->
    St#{worker_groups => update_worker_group(GroupID, Group, Groups)}.

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
    get_worker_group_size(Group) > MaxFreeGroupConnections.

-spec do_shrink_group(worker_group(), module()) -> worker_group().
do_shrink_group(Group0, FactoryHandler) ->
    {Worker, Group1} = get_next_worker(Group0),
    ok = gunner_worker_factory:exit_worker(FactoryHandler, Worker),
    Group1.

get_max_pool_size() ->
    genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE).

get_max_free_connections() ->
    genlib_app:env(gunner, group_max_free_connections, ?DEFAULT_MAX_FREE_CONNECTIONS).

-spec get_gun_client_opts() -> gun_client_opts().
get_gun_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).
