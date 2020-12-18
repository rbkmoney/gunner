-module(gunner_pool).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

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
-type pool_id() :: atom().
-type pool_opts() :: #{}.

-type worker_group_id() :: term().

-type status_response() :: #{current_size := size()}.

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([worker_group_id/0]).
-export_type([status_response/0]).

%% Internal types

-type state() :: #{
    size := size(),
    worker_groups := worker_groups(),
    clients := clients(),
    worker_requests := worker_requests()
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

-type worker() :: gunner:worker().
-type worker_args() :: gunner:worker_args().

-type gun_client_opts() :: gun:opts().

-type from() :: {pid(), Tag :: _}.
-type supported_registries() :: gproc | global.

%%

-define(DEFAULT_PROCESS_REGISTRY, global).

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).
-define(DEFAULT_MAX_POOL_SIZE, 25).
-define(DEFAULT_MAX_FREE_CONNECTIONS, 5).

%%
%% API functions
%%

-spec start_pool(pool_id(), pool_opts()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts) ->
    case gunner_pool_sup:start_pool(PoolID, PoolOpts) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _}} ->
            {error, already_exists}
    end.

-spec stop_pool(pool_id()) -> ok | {error, not_found}.
stop_pool(PoolID) ->
    case get_pool_pid(PoolID) of
        Pid when is_pid(Pid) ->
            gunner_pool_sup:stop_pool(Pid);
        undefined ->
            {error, not_found}
    end.

-spec start_link(pool_id(), pool_opts()) -> genlib_gen:start_ret().
start_link(PoolID, PoolOpts) ->
    gen_server:start_link(via_tuple(PoolID), ?MODULE, [PoolOpts], []).

%%

-spec acquire(pool_id(), worker_args(), timeout()) ->
    {ok, worker()} | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}}.
acquire(PoolID, WorkerArgs, Timeout) ->
    call_pool(PoolID, {acquire, WorkerArgs}, Timeout).

-spec cancel_acquire(pool_id()) -> ok.
cancel_acquire(PoolID) ->
    gen_server:cast(via_tuple(PoolID), {cancel_acquire, self()}).

-spec free(pool_id(), worker(), timeout()) ->
    ok | {error, pool_not_found | {client, not_found} | {worker, {lease_return_failed, _Reason}}}.
free(PoolID, Worker, Timeout) ->
    call_pool(PoolID, {free, Worker}, Timeout).

-spec pool_status(pool_id(), timeout()) -> {ok, status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    call_pool(PoolID, status, Timeout).

%% API helpers

-spec via_tuple(pool_id()) -> {via, module(), Name :: _}.
via_tuple(PoolID) ->
    Via = get_via(),
    {via, Via, name_tuple(Via, PoolID)}.

% @TODO more universal registry support?
-spec name_tuple(supported_registries(), pool_id()) -> Name | {n, l, Name} when Name :: {?MODULE, pool_id()}.
name_tuple(global, PoolID) ->
    {?MODULE, PoolID};
name_tuple(gproc, PoolID) ->
    {n, l, name_tuple(global, PoolID)}.

-spec get_pool_pid(pool_id()) -> pid() | undefined.
get_pool_pid(PoolID) ->
    Via = get_via(),
    erlang:apply(Via, whereis_name, [name_tuple(Via, PoolID)]).

-spec get_via() -> supported_registries().
get_via() ->
    % I kinda miss compile-time env
    genlib_app:env(gunner, process_registry, ?DEFAULT_PROCESS_REGISTRY).

-spec call_pool(pool_id(), Args :: _, timeout()) -> Response :: _ | no_return().
call_pool(PoolID, Args, Timeout) ->
    try
        gen_server:call(via_tuple(PoolID), Args, Timeout)
    catch
        exit:{noproc, _} ->
            {error, pool_not_found}
    end.

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolOpts]) ->
    {ok, new_state(PoolOpts)}.

-spec handle_call
    ({acquire, worker_args()}, from(), state()) ->
        {reply, {ok, worker()} | {error, pool_unavailable | {worker_init_failed, _}}, state()} |
        {noreply, state()};
    ({free, worker()}, from(), state()) ->
        {reply, ok | {error, {client, not_found} | {worker, {lease_return_failed, _Reason}}}, state()}.
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
    case handle_free(Worker, ClientPid, St0) of
        {ok, St1} ->
            {reply, ok, St1};
        {error, client_state_not_found} ->
            {reply, {error, {client, not_found}}, St0};
        {error, {lease_return_failed, _} = Reason} ->
            {reply, {error, {worker, Reason}}, St0}
    end;
handle_call(status, _From, St0) ->
    {reply, {ok, get_pool_status(St0)}, St0};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast({cancel_acquire, client_pid()}, state()) -> {noreply, state()}.
handle_cast({cancel_acquire, ClientPid}, St0) ->
    % @TODO to combat the ability to call this function arbitrarily we need to introduce some sort of a token here
    case handle_cancel(ClientPid, St0) of
        {ok, St1} ->
            {noreply, St1};
        {error, _} ->
            {noreply, St0}
    end;
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
    case create_worker(WorkerArgs) of
        {ok, WorkerPid} ->
            _ = erlang:monitor(process, WorkerPid),
            WorkerRequests1 = add_worker_request(WorkerPid, GroupID, From, WorkerRequests0),
            {ok, St#{size => PoolSize + 1, worker_requests => WorkerRequests1}};
        {error, Reason} ->
            {error, {worker_init_failed, Reason}}
    end.

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

-spec handle_free(worker(), client_pid(), state()) ->
    {ok, state()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
handle_free(Worker, ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    case return_lease(Worker, ClientPid, Clients0) of
        {ok, GroupID, Clients1} ->
            WorkerGroups1 = return_worker(Worker, GroupID, WorkerGroups0),
            St1 = maybe_shrink_pool(GroupID, St#{worker_groups => WorkerGroups1, clients => Clients1}),
            {ok, St1};
        {error, _Reason} = Error ->
            Error
    end.

-spec return_lease(worker(), client_pid(), clients()) ->
    {ok, worker_group_id(), clients()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
return_lease(Worker, ClientPid, Clients) ->
    case get_client_state_by_pid(ClientPid, Clients) of
        ClientState0 when ClientState0 =/= undefined ->
            case do_return_lease(Worker, ClientState0) of
                {ok, GroupID, ClientState1} ->
                    {ok, GroupID, update_client_state(ClientPid, ClientState1, Clients)};
                {error, Reason} ->
                    {error, {lease_return_failed, Reason}}
            end;
        undefined ->
            {error, client_state_not_found}
    end.

-spec do_return_lease(worker(), client_state()) ->
    {ok, worker_group_id(), client_state()} | {error, no_leases | worker_not_found}.
do_return_lease(Worker, ClientState) ->
    case return_client_lease(Worker, ClientState) of
        {ok, WorkerGroupId, NewClientState} ->
            {ok, WorkerGroupId, NewClientState};
        {error, _Reason} = Error ->
            Error
    end.

-spec return_worker(worker(), worker_group_id(), worker_groups()) -> worker_groups().
return_worker(Worker, GroupID, WorkerGroups) ->
    Group0 = get_worker_group_by_id(GroupID, WorkerGroups),
    Group1 = add_worker_to_group(Worker, Group0),
    update_worker_group(GroupID, Group1, WorkerGroups).

%%

-spec handle_cancel(client_pid(), state()) ->
    {ok, state()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
handle_cancel(ClientPid, St = #{worker_groups := WorkerGroups0, clients := Clients0}) ->
    case cancel_lease(ClientPid, Clients0) of
        {ok, Worker, GroupID, Clients1} ->
            WorkerGroups1 = return_worker(Worker, GroupID, WorkerGroups0),
            St1 = maybe_shrink_pool(GroupID, St#{worker_groups => WorkerGroups1, clients => Clients1}),
            {ok, St1};
        {error, _Reason} = Error ->
            Error
    end.

-spec cancel_lease(client_pid(), clients()) ->
    {ok, worker(), worker_group_id(), clients()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
cancel_lease(ClientPid, Clients) ->
    case get_client_state_by_pid(ClientPid, Clients) of
        ClientState0 when ClientState0 =/= undefined ->
            case cancel_client_lease(ClientState0) of
                {ok, Worker, GroupID, ClientState1} ->
                    {ok, Worker, GroupID, update_client_state(ClientPid, ClientState1, Clients)};
                {error, Reason} ->
                    {error, {lease_return_failed, Reason}}
            end;
        undefined ->
            {error, client_state_not_found}
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
            delete_worker_from_group(Worker, Group)
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
new_state(_Opts) ->
    #{
        size => 0,
        worker_groups => #{},
        clients => #{},
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

-spec delete_worker_from_group(worker(), worker_group()) -> worker_group().
delete_worker_from_group(Worker, Group) ->
    gunner_pool_worker_group:delete_worker(Worker, Group).

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

%% Workers

-spec create_worker(worker_args()) -> {ok, worker()} | {error, Reason :: any()}.
create_worker({Host, Port}) ->
    ClientOpts = get_gun_client_opts(),
    gun:open(Host, Port, ClientOpts#{retry => 0}).

-spec exit_worker(worker()) -> ok.
exit_worker(WorkerPid) ->
    ok = gun:shutdown(WorkerPid).

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

-spec maybe_shrink_pool(worker_group_id(), state()) -> state().
maybe_shrink_pool(GroupID, St0 = #{size := PoolSize, worker_groups := Groups0}) ->
    Group0 = get_worker_group_by_id(GroupID, Groups0),
    case group_needs_shrinking(Group0) of
        true ->
            Group1 = do_shrink_group(Group0),
            St0#{size => PoolSize - 1, worker_groups => update_worker_group(GroupID, Group1, Groups0)};
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

-spec do_shrink_group(worker_group()) -> worker_group().
do_shrink_group(Group0) ->
    {Worker, Group1} = get_next_worker(Group0),
    ok = exit_worker(Worker),
    Group1.

get_max_pool_size() ->
    genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE).

get_max_free_connections() ->
    genlib_app:env(gunner, group_max_free_connections, ?DEFAULT_MAX_FREE_CONNECTIONS).

-spec get_gun_client_opts() -> gun_client_opts().
get_gun_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).
