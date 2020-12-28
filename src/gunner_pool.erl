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
-type pool_opts() :: #{
    connection_limit => size(),
    max_free_connections => size()
}.

-type connection_group_id() :: term().

-type status_response() :: #{current_size := size()}.

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([connection_group_id/0]).
-export_type([status_response/0]).

%% Internal types

-type state() :: #{
    size := size(),
    connection_limit := size(),
    connection_groups := connection_groups(),
    max_free_connections := size(),
    clients := clients(),
    connection_requests := connection_requests()
}.

-type connection_groups() :: #{
    connection_group_id() => connection_group()
}.

-type connection_group() :: gunner_pool_connection_group:state().

-type clients() :: #{
    client_pid() => client_state()
}.

-type client_pid() :: pid().
-type client_state() :: gunner_pool_client_state:state().

-type connection_requests() :: #{
    connection() => connection_request()
}.

-type connection_request() :: {connection_group_id(), TargetClient :: from()}.

-type size() :: non_neg_integer().

-type connection() :: gunner:connection().
-type connection_args() :: gunner:connection_args().

-type gun_client_opts() :: gun:opts().

-type from() :: {pid(), Tag :: _}.
-type supported_registries() :: gproc | global.

%%

-define(DEFAULT_PROCESS_REGISTRY, global).

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).
-define(DEFAULT_MAX_FREE_CONNECTIONS, 5).
-define(DEFAULT_CONNECTION_LIMIT, 25).

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

-spec acquire(pool_id(), connection_args(), timeout()) ->
    {ok, connection()} | {error, pool_not_found | pool_unavailable | {connection_init_failed, _}}.
acquire(PoolID, ConnectionArgs, Timeout) ->
    call_pool(PoolID, {acquire, ConnectionArgs}, Timeout).

-spec cancel_acquire(pool_id()) -> ok.
cancel_acquire(PoolID) ->
    gen_server:cast(via_tuple(PoolID), {cancel_acquire, self()}).

-spec free(pool_id(), connection(), timeout()) ->
    ok | {error, pool_not_found | {client, not_found} | {connection, {lease_return_failed, _Reason}}}.
free(PoolID, Connection, Timeout) ->
    call_pool(PoolID, {free, Connection}, Timeout).

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
    ({acquire, connection_args()}, from(), state()) ->
        {reply, {ok, connection()} | {error, pool_unavailable | {connection_init_failed, _}}, state()} |
        {noreply, state()};
    ({free, connection()}, from(), state()) ->
        {reply, ok | {error, {client, not_found} | {connection, {lease_return_failed, _Reason}}}, state()}.
handle_call({acquire, ConnectionArgs}, {ClientPid, _} = From, St0) ->
    GroupID = create_group_id(ConnectionArgs),
    St1 = ensure_group_exists(GroupID, St0),
    case handle_acquire(GroupID, ClientPid, St1) of
        {ok, {connection, ConnectionPid}, St2} ->
            {reply, {ok, ConnectionPid}, St2};
        {ok, no_connection} ->
            case handle_connection_start(GroupID, ConnectionArgs, From, St1) of
                {ok, St2} ->
                    {noreply, St2};
                {error, _Reason} = Error ->
                    {reply, Error, St0}
            end;
        {error, pool_unavailable} ->
            {reply, {error, pool_unavailable}, St0}
    end;
handle_call({free, Connection}, {ClientPid, _}, St0) ->
    case handle_free(Connection, ClientPid, St0) of
        {ok, St1} ->
            {reply, ok, St1};
        {error, client_state_not_found} ->
            {reply, {error, {client, not_found}}, St0};
        {error, {lease_return_failed, _} = Reason} ->
            {reply, {error, {connection, Reason}}, St0}
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
    St1 = handle_connection_start_success(Pid, St0),
    {noreply, St1};
handle_info({'DOWN', _Mref, process, Pid, Reason}, St0) ->
    St1 = handle_process_down(Pid, Reason, St0),
    {noreply, St1};
handle_info(_, St0) ->
    {noreply, St0}.

%%
%% Internal functions
%%

-spec create_group_id(connection_args()) -> connection_group_id().
create_group_id(ConnectionArgs) ->
    ConnectionArgs.

-spec handle_acquire(connection_group_id(), pid(), state()) ->
    {ok, {connection, connection()}, state()} | {ok, no_connection} | {error, pool_unavailable}.
handle_acquire(GroupID, ClientPid, St = #{connection_groups := ConnectionGroups0, clients := Clients0}) ->
    case fetch_next_connection(GroupID, ConnectionGroups0) of
        {ok, Connection, ConnectionGroups1} ->
            Clients1 = register_client_lease(ClientPid, Connection, GroupID, Clients0),
            {ok, {connection, Connection}, St#{connection_groups => ConnectionGroups1, clients => Clients1}};
        {error, no_available_connections} ->
            case assert_pool_available(St) of
                ok -> {ok, no_connection};
                error -> {error, pool_unavailable}
            end
    end.

-spec fetch_next_connection(connection_group_id(), connection_groups()) ->
    {ok, connection(), connection_groups()} | {error, no_available_connections}.
fetch_next_connection(GroupID, ConnectionGroups) ->
    Group0 = get_connection_group_by_id(GroupID, ConnectionGroups),
    case is_connection_group_empty(Group0) of
        false ->
            {Connection, Group1} = get_next_connection(Group0),
            {ok, Connection, update_connection_group(GroupID, Group1, ConnectionGroups)};
        true ->
            {error, no_available_connections}
    end.

-spec register_client_lease(client_pid(), connection(), connection_group_id(), clients()) -> clients().
register_client_lease(ClientPid, Connection, GroupID, Clients0) ->
    Clients1 = ensure_client_state_exists(ClientPid, Clients0),
    ClientSt0 = get_client_state_by_pid(ClientPid, Clients1),
    ClientSt1 = register_client_lease(Connection, GroupID, ClientSt0),
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
assert_pool_available(#{connection_limit := Limit, size := PoolSize}) when PoolSize < Limit ->
    ok;
assert_pool_available(_) ->
    error.

-spec handle_connection_start(connection_group_id(), connection_args(), From :: from(), state()) ->
    {ok, state()} | {error, {connection_init_failed, Reason :: term()}}.
handle_connection_start(
    GroupID,
    ConnectionArgs,
    From,
    St = #{size := PoolSize, connection_requests := ConnectionRequests0}
) ->
    case create_connection(ConnectionArgs) of
        {ok, ConnectionPid} ->
            _ = erlang:monitor(process, ConnectionPid),
            ConnectionRequests1 = add_connection_request(ConnectionPid, GroupID, From, ConnectionRequests0),
            {ok, St#{size => PoolSize + 1, connection_requests => ConnectionRequests1}};
        {error, Reason} ->
            {error, {connection_init_failed, Reason}}
    end.

-spec handle_connection_start_success(connection(), state()) -> state().
handle_connection_start_success(ConnectionPid, St = #{connection_requests := ConnectionRequests0, clients := Clients0}) ->
    {GroupID, {ClientPid, _} = From} = get_connection_request_by_pid(ConnectionPid, ConnectionRequests0),
    ConnectionRequests1 = remove_connection_request(ConnectionPid, ConnectionRequests0),
    Clients1 = register_client_lease(ClientPid, ConnectionPid, GroupID, Clients0),
    ok = gen_server:reply(From, {ok, ConnectionPid}),
    St#{connection_requests := ConnectionRequests1, clients := Clients1}.

-spec handle_connection_start_fail(connection(), Reason :: term(), state()) -> state().
handle_connection_start_fail(
    ConnectionPid,
    Reason,
    St = #{size := PoolSize, connection_requests := ConnectionRequests0}
) ->
    {_GroupID, From} = get_connection_request_by_pid(ConnectionPid, ConnectionRequests0),
    ConnectionRequests1 = remove_connection_request(ConnectionPid, ConnectionRequests0),
    ok = gen_server:reply(From, {error, {connection_init_failed, Reason}}),
    St#{size => PoolSize - 1, connection_requests := ConnectionRequests1}.

%%

-spec handle_free(connection(), client_pid(), state()) ->
    {ok, state()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
handle_free(Connection, ClientPid, St = #{connection_groups := ConnectionGroups0, clients := Clients0}) ->
    case return_lease(Connection, ClientPid, Clients0) of
        {ok, GroupID, Clients1} ->
            ConnectionGroups1 = return_connection(Connection, GroupID, ConnectionGroups0),
            St1 = maybe_shrink_pool(GroupID, St#{connection_groups => ConnectionGroups1, clients => Clients1}),
            {ok, St1};
        {error, _Reason} = Error ->
            Error
    end.

-spec return_lease(connection(), client_pid(), clients()) ->
    {ok, connection_group_id(), clients()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
return_lease(Connection, ClientPid, Clients) ->
    case get_client_state_by_pid(ClientPid, Clients) of
        ClientState0 when ClientState0 =/= undefined ->
            case do_return_lease(Connection, ClientState0) of
                {ok, GroupID, ClientState1} ->
                    {ok, GroupID, update_client_state(ClientPid, ClientState1, Clients)};
                {error, Reason} ->
                    {error, {lease_return_failed, Reason}}
            end;
        undefined ->
            {error, client_state_not_found}
    end.

-spec do_return_lease(connection(), client_state()) ->
    {ok, connection_group_id(), client_state()} | {error, no_leases | connection_not_found}.
do_return_lease(Connection, ClientState) ->
    case return_client_lease(Connection, ClientState) of
        {ok, ConnectionGroupId, NewClientState} ->
            {ok, ConnectionGroupId, NewClientState};
        {error, _Reason} = Error ->
            Error
    end.

-spec return_connection(connection(), connection_group_id(), connection_groups()) -> connection_groups().
return_connection(Connection, GroupID, ConnectionGroups) ->
    Group0 = get_connection_group_by_id(GroupID, ConnectionGroups),
    Group1 = add_connection_to_group(Connection, Group0),
    update_connection_group(GroupID, Group1, ConnectionGroups).

%%

-spec handle_cancel(client_pid(), state()) ->
    {ok, state()} | {error, client_state_not_found | {lease_return_failed, _Reason}}.
handle_cancel(ClientPid, St = #{connection_groups := ConnectionGroups0, clients := Clients0}) ->
    case cancel_lease(ClientPid, Clients0) of
        {ok, Connection, GroupID, Clients1} ->
            ConnectionGroups1 = return_connection(Connection, GroupID, ConnectionGroups0),
            St1 = maybe_shrink_pool(GroupID, St#{connection_groups => ConnectionGroups1, clients => Clients1}),
            {ok, St1};
        {error, _Reason} = Error ->
            Error
    end.

-spec cancel_lease(client_pid(), clients()) ->
    {ok, connection(), connection_group_id(), clients()} |
    {error, client_state_not_found | {lease_return_failed, _Reason}}.
cancel_lease(ClientPid, Clients) ->
    case get_client_state_by_pid(ClientPid, Clients) of
        ClientState0 when ClientState0 =/= undefined ->
            case cancel_client_lease(ClientState0) of
                {ok, Connection, GroupID, ClientState1} ->
                    {ok, Connection, GroupID, update_client_state(ClientPid, ClientState1, Clients)};
                {error, Reason} ->
                    {error, {lease_return_failed, Reason}}
            end;
        undefined ->
            {error, client_state_not_found}
    end.

%%

-spec handle_process_down(connection() | client_pid(), Reason :: term(), state()) -> state().
handle_process_down(Pid, Reason, St) ->
    handle_process_down(determine_process_type(Pid, St), Pid, Reason, St).

-spec determine_process_type(connection() | client_pid(), state()) -> connection | client.
determine_process_type(Pid, #{clients := Clients}) ->
    case client_state_exists(Pid, Clients) of
        true ->
            client;
        false ->
            connection
    end.

-spec handle_process_down
    (client, client_pid(), Reason :: term(), state()) -> state();
    (connection, connection(), Reason :: term(), state()) -> state().
handle_process_down(client, ClientPid, _Reason, St = #{clients := Clients0}) ->
    ClientSt0 = get_client_state_by_pid(ClientPid, Clients0),
    {Leases, ClientSt1} = purge_client_leases(ClientSt0),
    ok = destroy_client_state(ClientSt1),
    return_all_leases(Leases, St#{clients => remove_client_state(ClientPid, Clients0)});
handle_process_down(connection, Connection, Reason, St = #{connection_requests := ConnectionRequests0}) ->
    case connection_request_exists(Connection, ConnectionRequests0) of
        true ->
            handle_connection_start_fail(Connection, Reason, St);
        false ->
            handle_connection_exit(Connection, St)
    end.

-spec handle_connection_exit(connection(), state()) -> state().
handle_connection_exit(Connection, St = #{connection_groups := Groups0}) ->
    Groups1 = maps:map(
        fun(_K, Group) ->
            delete_connection_from_group(Connection, Group)
        end,
        Groups0
    ),
    St#{connection_groups => Groups1}.

return_all_leases([], St) ->
    St;
return_all_leases([{Connection, ConnectionGroupID} | Rest], St = #{connection_groups := ConnectionGroups0}) ->
    ConnectionGroups1 = return_connection(Connection, ConnectionGroupID, ConnectionGroups0),
    St1 = maybe_shrink_pool(ConnectionGroupID, St#{connection_groups => ConnectionGroups1}),
    return_all_leases(Rest, St1).

%%

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    #{
        size => 0,
        connection_limit => maps:get(connection_limit, Opts, ?DEFAULT_CONNECTION_LIMIT),
        connection_groups => #{},
        max_free_connections => maps:get(max_free_connections, Opts, ?DEFAULT_MAX_FREE_CONNECTIONS),
        clients => #{},
        connection_requests => #{}
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

-spec register_client_lease(connection(), connection_group_id(), client_state()) -> client_state().
register_client_lease(Connection, GroupID, ClientSt) ->
    gunner_pool_client_state:register_lease(Connection, GroupID, ClientSt).

-spec return_client_lease(connection(), client_state()) ->
    {ok, connection_group_id(), client_state()} | {error, no_leases | connection_not_found}.
return_client_lease(Connection, ClientSt) ->
    gunner_pool_client_state:return_lease(Connection, ClientSt).

-spec cancel_client_lease(client_state()) ->
    {ok, connection(), connection_group_id(), client_state()} | {error, no_leases | connection_not_found}.
cancel_client_lease(ClientSt) ->
    gunner_pool_client_state:cancel_lease(ClientSt).

-spec purge_client_leases(client_state()) -> {[{connection(), connection_group_id()}], client_state()}.
purge_client_leases(ClientSt) ->
    gunner_pool_client_state:purge_leases(ClientSt).

-spec destroy_client_state(client_state()) -> ok | {error, active_leases_present}.
destroy_client_state(ClientSt) ->
    gunner_pool_client_state:destroy(ClientSt).

%% Connection groups

-spec get_connection_group_by_id(connection_group_id(), connection_groups()) -> connection_group() | undefined.
get_connection_group_by_id(ConnectionGroupID, ConnectionGroups) ->
    maps:get(ConnectionGroupID, ConnectionGroups, undefined).

-spec update_connection_group(connection_group_id(), connection_group(), connection_groups()) -> connection_groups().
update_connection_group(GroupID, Group, ConnectionGroups) ->
    ConnectionGroups#{GroupID => Group}.

-spec connection_group_exists(connection_group_id(), connection_groups()) -> boolean().
connection_group_exists(ConnectionGroupID, ConnectionGroups) ->
    maps:is_key(ConnectionGroupID, ConnectionGroups).

-spec create_connection_group() -> connection_group().
create_connection_group() ->
    gunner_pool_connection_group:new().

-spec is_connection_group_empty(connection_group()) -> boolean().
is_connection_group_empty(Group) ->
    gunner_pool_connection_group:is_empty(Group).

-spec get_next_connection(connection_group()) -> {connection(), connection_group()}.
get_next_connection(Group) ->
    gunner_pool_connection_group:next_connection(Group).

-spec add_connection_to_group(connection(), connection_group()) -> connection_group().
add_connection_to_group(Connection, Group) ->
    gunner_pool_connection_group:add_connection(Connection, Group).

-spec get_connection_group_size(connection_group()) -> non_neg_integer().
get_connection_group_size(Group) ->
    gunner_pool_connection_group:size(Group).

-spec delete_connection_from_group(connection(), connection_group()) -> connection_group().
delete_connection_from_group(Connection, Group) ->
    gunner_pool_connection_group:delete_connection(Connection, Group).

%% Connection requests

-spec add_connection_request(connection(), connection_group_id(), From :: _, connection_requests()) ->
    connection_requests().
add_connection_request(ConnectionPid, GroupID, From, ConnectionRequests0) ->
    ConnectionRequests0#{ConnectionPid => {GroupID, From}}.

-spec get_connection_request_by_pid(connection(), connection_requests()) -> connection_request() | undefined.
get_connection_request_by_pid(ConnectionPid, ConnectionRequests) ->
    maps:get(ConnectionPid, ConnectionRequests, undefined).

-spec remove_connection_request(connection(), connection_requests()) -> connection_requests().
remove_connection_request(ConnectionPid, ConnectionRequests) ->
    maps:without([ConnectionPid], ConnectionRequests).

-spec connection_request_exists(connection(), connection_requests()) -> boolean().
connection_request_exists(ConnectionPid, ConnectionRequests) ->
    maps:is_key(ConnectionPid, ConnectionRequests).

%% Connections

-spec create_connection(connection_args()) -> {ok, connection()} | {error, Reason :: any()}.
create_connection({Host, Port}) ->
    ClientOpts = get_gun_client_opts(),
    gun:open(Host, Port, ClientOpts#{retry => 0}).

-spec exit_connection(connection()) -> ok.
exit_connection(ConnectionPid) ->
    ok = gun:shutdown(ConnectionPid).

%%

-spec get_pool_status(state()) -> status_response().
get_pool_status(#{size := Size}) ->
    #{current_size => Size}.

-spec ensure_group_exists(connection_group_id(), state()) -> state().
ensure_group_exists(GroupID, St0 = #{connection_groups := Groups0}) ->
    case connection_group_exists(GroupID, Groups0) of
        true ->
            St0;
        false ->
            Groups1 = update_connection_group(GroupID, create_connection_group(), Groups0),
            St0#{connection_groups := Groups1}
    end.

-spec maybe_shrink_pool(connection_group_id(), state()) -> state().
maybe_shrink_pool(GroupID, St0 = #{max_free_connections := Limit, size := PoolSize, connection_groups := Groups0}) ->
    Group0 = get_connection_group_by_id(GroupID, Groups0),
    case group_needs_shrinking(Group0, Limit) of
        true ->
            Group1 = do_shrink_group(Group0),
            St0#{size => PoolSize - 1, connection_groups => update_connection_group(GroupID, Group1, Groups0)};
        false ->
            St0
    end.

-spec group_needs_shrinking(connection_group(), Limit :: size()) -> boolean().
group_needs_shrinking(Group, Limit) ->
    is_connection_group_full(Group, Limit).

-spec is_connection_group_full(connection_group(), Max :: non_neg_integer()) -> boolean().
is_connection_group_full(Group, MaxFreeGroupConnections) ->
    get_connection_group_size(Group) > MaxFreeGroupConnections.

-spec do_shrink_group(connection_group()) -> connection_group().
do_shrink_group(Group0) ->
    {Connection, Group1} = get_next_connection(Group0),
    ok = exit_connection(Connection),
    Group1.

-spec get_gun_client_opts() -> gun_client_opts().
get_gun_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).
