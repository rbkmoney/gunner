-module(gunner_pool).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

-export([acquire/4]).
-export([cancel_acquire/2]).

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
    total_connection_limit => size(),
    free_connection_limit => size()
}.

-type connection_group_id() :: term().

-type status_response() :: #{total_count := size(), free_count := size()}.

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([connection_group_id/0]).
-export_type([status_response/0]).

%% Internal types

-type state() :: #{
    total_connection_limit := size(),
    free_connection_limit := size(),
    pool := connection_pool(),
    clients := clients(),
    connection_requests := connection_requests(),
    connection_monitors := connection_monitors(),
    client_assignments := client_assignments()
}.

-type connection_pool() :: gunner_connection_pool:state().

-type clients() :: #{
    client_pid() => client_state()
}.

-type client_pid() :: pid().
-type client_state() :: gunner_pool_client_state:state().

-type connection_requests() :: #{
    connection() => connection_request()
}.

-type connection_request() :: #{
    requester := from() | undefined,
    group_id := connection_group_id(),
    ticket := ticket()
}.

-type size() :: non_neg_integer().

-type connection() :: gunner:connection().
-type connection_args() :: gunner:connection_args().

-type gun_client_opts() :: gun:opts().

-type from() :: {pid(), Tag :: _}.
-type ticket() :: reference().
-type monitor_ref() :: reference().

-type connection_monitors() :: #{connection() => monitor_ref()}.
-type client_assignments() :: #{connection() => client_pid()}.

%%

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).
-define(DEFAULT_FREE_CONNECTION_LIMIT, 5).
-define(DEFAULT_TOTAL_CONNECTION_LIMIT, 25).

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

-spec acquire(pool_id(), connection_args(), ticket(), timeout()) ->
    {ok, connection()} | {error, pool_not_found | pool_unavailable | {connection_failed, _}}.
acquire(PoolID, ConnectionArgs, Ticket, Timeout) ->
    call_pool(PoolID, {acquire, ConnectionArgs, Ticket}, Timeout).

-spec cancel_acquire(pool_id(), ticket()) -> ok.
cancel_acquire(PoolID, Ticket) ->
    gen_server:cast(via_tuple(PoolID), {cancel_acquire, self(), Ticket}).

-spec free(pool_id(), connection(), timeout()) ->
    ok | {error, pool_not_found | {lease_return_failed, connection_not_found | client_state_not_found}}.
free(PoolID, Connection, Timeout) ->
    call_pool(PoolID, {free, Connection}, Timeout).

-spec pool_status(pool_id(), timeout()) -> {ok, status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    call_pool(PoolID, status, Timeout).

%% API helpers

-spec call_pool(pool_id(), Args :: _, timeout()) -> Response :: _ | no_return().
call_pool(PoolID, Args, Timeout) ->
    try
        gen_server:call(via_tuple(PoolID), Args, Timeout)
    catch
        exit:{noproc, _} ->
            {error, pool_not_found}
    end.

-spec via_tuple(pool_id()) -> gunner_pool_registry:via_tuple().
via_tuple(PoolID) ->
    gunner_pool_registry:via_tuple(PoolID).

-spec get_pool_pid(pool_id()) -> pid() | undefined.
get_pool_pid(PoolID) ->
    gunner_pool_registry:whereis(PoolID).

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolOpts]) ->
    {ok, new_state(PoolOpts)}.

%% @WARN Types here are not actually checked (as of Erlang/OTP 22 [erts-10.7.2.4]). Thanks, dialyzer.
-spec handle_call
    ({acquire, connection_args(), ticket()}, from(), state()) ->
        {noreply, state()} |
        {reply, {error, pool_unavailable | {connection_failed, Reason :: _}}, state()};
    ({free, connection()}, from(), state()) -> {reply, ok | {error, Reason}, state()} when
        Reason :: {lease_return_failed, connection_not_found | client_state_not_found};
    (status, from(), state()) -> {reply, {ok, status_response()}, state()}.
%%(Any :: _, from(), state()) -> no_return().
handle_call({acquire, ConnectionArgs, Ticket}, From, St0) ->
    GroupID = create_group_id(ConnectionArgs),
    Result =
        case acquire_connection_from_pool(GroupID, St0) of
            {{ok, Connection}, St1} ->
                handle_connection_acquired(Connection, GroupID, From, Ticket, St1);
            {no_connection_available, St0} ->
                handle_connection_creation(ConnectionArgs, GroupID, From, Ticket, St0)
        end,
    case Result of
        {ok, St2} ->
            {noreply, St2};
        {error, _Reason} = Error ->
            %% don't alter state on error
            {reply, Error, St0}
    end;
handle_call({free, Connection}, {ClientPid, _}, St0) ->
    Result =
        case do_free_lease(ClientPid, Connection, St0) of
            {{ok, GroupID}, St1} ->
                handle_connection_free(Connection, GroupID, St1);
            {{error, Reason}, _} ->
                {error, {lease_return_failed, Reason}}
        end,
    case Result of
        {ok, St2} ->
            {reply, ok, St2};
        {error, _Reason} = Error ->
            {reply, Error, St0}
    end;
handle_call(status, _From, St0) ->
    {reply, {ok, get_pool_status(St0)}, St0};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast({cancel_acquire, client_pid(), ticket()}, state()) -> {noreply, state()}.
handle_cast({cancel_acquire, ClientPid, Ticket}, St0) ->
    Result =
        case do_cancel_connection_request(Ticket, ClientPid, St0) of
            {ok, St1} ->
                {ok, St1};
            {error, no_connection_request} ->
                case do_cancel_lease(ClientPid, Ticket, St0) of
                    {{ok, Connection, GroupID}, St1} ->
                        handle_connection_free(Connection, GroupID, St1);
                    {{error, Reason}, _} ->
                        {error, {lease_cancel_failed, Reason}}
                end
        end,
    case Result of
        {ok, NewSt} ->
            {noreply, NewSt};
        {error, _Reason} ->
            %@TODO Log this?
            {noreply, St0}
    end;
handle_cast(_Cast, _St) ->
    erlang:error(unexpected_cast).

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({gun_up, ConnPid, _Protocol}, St0) ->
    {ok, St1} = handle_connection_started(ConnPid, St0),
    {noreply, St1};
handle_info({gun_down, _ConnPid, _Protocol, _Reason, _KilledStreams}, St0) ->
    %% Handled via 'DOWN'
    {noreply, St0};
handle_info({'DOWN', _Mref, process, ProcessPid, Reason}, St0) ->
    {ok, St1} = handle_process_down(ProcessPid, Reason, St0),
    {noreply, St1};
handle_info(_, _St0) ->
    erlang:error(unexpected_info).

%%
%% Internal functions
%%

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    #{
        total_connection_limit => maps:get(total_connection_limit, Opts, ?DEFAULT_TOTAL_CONNECTION_LIMIT),
        free_connection_limit => maps:get(free_connection_limit, Opts, ?DEFAULT_FREE_CONNECTION_LIMIT),
        pool => gunner_connection_pool:new(),
        clients => #{},
        connection_requests => #{},
        connection_monitors => #{},
        client_assignments => #{}
    }.

-spec create_group_id(connection_args()) -> connection_group_id().
create_group_id(ConnectionArgs) ->
    ConnectionArgs.

%%

-spec handle_connection_acquired(connection(), connection_group_id(), from(), ticket(), state()) ->
    {ok, state()} | {error, Reason :: _}.
handle_connection_acquired(Connection, GroupID, Requester, Ticket, St0) ->
    %% assign connection to client state and reply
    St1 = return_connection_to_client(Connection, Requester, GroupID, Ticket, St0),
    {ok, St1}.

-spec handle_connection_creation(connection_args(), connection_group_id(), from(), ticket(), state()) ->
    {ok, state()} | {error, pool_unavailable | {connection_failed, Reason :: _}}.
handle_connection_creation(ConnectionArgs, GroupID, Requester, Ticket, St0) ->
    case process_create_connection(ConnectionArgs, St0) of
        {ok, ConnectionPid, St1} ->
            ConnRequestSt = create_connection_request(Requester, GroupID, Ticket),
            {ok, save_connection_request(ConnectionPid, ConnRequestSt, St1)};
        {error, _Reason} = Error ->
            Error
    end.

%%

-spec handle_connection_started(connection(), state()) -> {ok, state()} | {error, connection_not_requested}.
handle_connection_started(Connection, St0) ->
    case get_connection_request(Connection, St0) of
        {ok, #{group_id := GroupID, requester := undefined}} ->
            %% original requester has since then cancelled the request
            %% assign connection to the pool and get out
            {ok, St1} = assign_connection_to_pool(Connection, GroupID, St0),
            St2 = remove_connection_request(Connection, St1),
            {ok, St2};
        {ok, #{group_id := GroupID, requester := Requester, ticket := Ticket}} ->
            {ok, St1} = assign_connection_to_pool(Connection, GroupID, St0),
            {{ok, Connection}, St2} = acquire_connection_from_pool(GroupID, St1),
            St3 = return_connection_to_client(Connection, Requester, GroupID, Ticket, St2),
            St4 = remove_connection_request(Connection, St3),
            {ok, St4};
        not_found ->
            {error, connection_not_requested}
    end.

%%

-spec handle_connection_closed(connection(), state()) -> {ok, state()} | {error, Reason :: _}.
handle_connection_closed(Connection, St0 = #{pool := PoolSt0}) ->
    case gunner_connection_pool:is_busy(Connection, PoolSt0) of
        true ->
            {{ok, GroupID}, St1} = do_free_lease(Connection, St0),
            with_pool(St1, fun(PoolSt) ->
                {ok, PoolSt1} = gunner_connection_pool:free(Connection, GroupID, PoolSt),
                gunner_connection_pool:remove(Connection, GroupID, PoolSt1)
            end);
        false ->
            with_pool(St0, fun(PoolSt) ->
                {ok, GroupID} = gunner_connection_pool:get_assignment(Connection, PoolSt),
                gunner_connection_pool:remove(Connection, GroupID, PoolSt)
            end)
    end.

%%

-spec handle_connection_free(connection(), connection_group_id(), state()) -> {ok, state()}.
handle_connection_free(Connection, GroupID, St0 = #{free_connection_limit := FreeLimit}) ->
    {Result, St1} = with_pool(St0, fun(PoolSt) ->
        %% sanity check
        {ok, GroupID} = gunner_connection_pool:get_assignment(Connection, PoolSt),
        {ok, PoolSt1} = gunner_connection_pool:free(Connection, GroupID, PoolSt),
        case is_pool_oversized(PoolSt1, FreeLimit) of
            true ->
                {ok, PoolSt2} = gunner_connection_pool:remove(Connection, GroupID, PoolSt1),
                {{ok, pool_shrinked}, PoolSt2};
            false ->
                {ok, PoolSt1}
        end
    end),
    case Result of
        {ok, pool_shrinked} ->
            process_kill_connection(Connection, St1);
        ok ->
            {ok, St1}
    end.

%%

-spec handle_process_down(pid(), Reason :: any(), state()) -> {ok, state()}.
handle_process_down(ProcessPid, Reason, St0) ->
    %%@TODO All of this really needs to be refactored
    case is_client_process(ProcessPid, St0) of
        true ->
            handle_client_death(ProcessPid, St0);
        false ->
            handle_connection_death(ProcessPid, Reason, St0)
    end.

-spec is_client_process(pid(), state()) -> boolean().
is_client_process(ProcessPid, #{clients := Clients}) ->
    client_state_exists(ProcessPid, Clients).

%%

-spec handle_connection_death(connection(), Reason :: any(), state()) -> {ok, state()}.
handle_connection_death(Connection, Reason, St0) ->
    case connection_request_exists(Connection, St0) of
        true ->
            handle_connection_start_failed(Connection, Reason, St0);
        false ->
            handle_connection_closed(Connection, St0)
    end.

%%

-spec handle_client_death(client_pid(), state()) -> {ok, state()}.
handle_client_death(ClientPid, St0) ->
    {{ok, Leases}, St1} = with_client_state(ClientPid, St0, fun(ClientSt0) ->
        {Leases, ClientSt1} = gunner_pool_client_state:purge_leases(ClientSt0),
        ok = gunner_pool_client_state:destroy(ClientSt1),
        {{ok, Leases}, ClientSt1}
    end),
    St2 = remove_client_state(ClientPid, St1),
    return_all_leases(Leases, St2).

return_all_leases([], St) ->
    {ok, St};
return_all_leases([{Connection, _Ticket, GroupID} | Rest], St0) ->
    {ok, St1} = handle_connection_free(Connection, GroupID, St0),
    return_all_leases(Rest, St1).

%%

-spec handle_connection_start_failed(connection(), Reason :: any(), state()) ->
    {ok, state()} | {error, connection_not_requested}.
handle_connection_start_failed(Connection, Reason, St0) ->
    case get_connection_request(Connection, St0) of
        {ok, #{requester := undefined}} ->
            {ok, remove_connection_request(Connection, St0)};
        {ok, #{requester := Requester}} ->
            _ = gen_server:reply(Requester, {error, {connection_failed, Reason}}),
            {ok, remove_connection_request(Connection, St0)};
        not_found ->
            {error, connection_not_requested}
    end.

%%

-spec do_cancel_connection_request(ticket(), client_pid(), state()) -> {ok, state()} | {error, no_connection_request}.
do_cancel_connection_request(Ticket, ClientPid, St) ->
    case find_connection_request(Ticket, ClientPid, St) of
        {ok, ConnectionPid, ConnRequestSt} ->
            NewConnRequestSt = ConnRequestSt#{requester => undefined},
            {ok, save_connection_request(ConnectionPid, NewConnRequestSt, St)};
        {error, not_found} ->
            {error, no_connection_request}
    end.

-spec find_connection_request(ticket(), client_pid(), state()) ->
    {ok, connection(), connection_request()} | {error, not_found}.
find_connection_request(Ticket, ClientPid, #{connection_requests := ConnectionRequests}) ->
    do_find_connection_request(Ticket, ClientPid, maps:to_list(ConnectionRequests)).

-spec do_find_connection_request(ticket(), client_pid(), list()) ->
    {ok, connection(), connection_request()} | {error, not_found}.
do_find_connection_request(_Ticket, _ClientPid, []) ->
    {error, not_found};
do_find_connection_request(Ticket, ClientPid, [{ClientPid, Request = #{ticket := Ticket}} | _Rest]) ->
    {ok, ClientPid, Request};
do_find_connection_request(Ticket, ClientPid, [_ | Rest]) ->
    do_find_connection_request(Ticket, ClientPid, Rest).

%%

-spec assign_connection_to_pool(connection(), connection_group_id(), state()) ->
    {ok | {error, connection_exists}, state()}.
assign_connection_to_pool(Connection, GroupID, St0) ->
    with_pool(St0, fun(PoolSt) ->
        case gunner_connection_pool:assign(Connection, GroupID, PoolSt) of
            {ok, _PoolSt1} = Ok ->
                Ok;
            {error, _Reason} = Error ->
                {Error, PoolSt}
        end
    end).

-spec acquire_connection_from_pool(connection_group_id(), state()) ->
    {AcquireResult :: {ok, connection()} | no_connection_available, state()}.
acquire_connection_from_pool(GroupID, St0) ->
    with_pool(St0, fun(PoolSt) ->
        case gunner_connection_pool:acquire(GroupID, PoolSt) of
            {ok, Connection, PoolSt1} ->
                {{ok, Connection}, PoolSt1};
            {ok, no_connection} ->
                {no_connection_available, PoolSt};
            {error, group_not_found} ->
                {no_connection_available, PoolSt}
        end
    end).

-spec return_connection_to_client(connection(), from(), connection_group_id(), ticket(), state()) -> state().
return_connection_to_client(Connection, {ClientPid, _} = From, GroupID, Ticket, St0) ->
    {ok, St1} = do_save_lease(Connection, ClientPid, Ticket, GroupID, St0),
    _ = gen_server:reply(From, {ok, Connection}),
    St1.

%%

-spec do_save_lease(connection(), client_pid(), ticket(), connection_group_id(), state()) -> {ok, state()}.
do_save_lease(Connection, ClientPid, Ticket, GroupID, St0) ->
    St1 = ensure_client_state_exists(ClientPid, St0),
    {Result, St2} = with_client_state(ClientPid, St1, fun(ClientSt0) ->
        ClientSt1 = gunner_pool_client_state:register_lease(Connection, Ticket, GroupID, ClientSt0),
        {ok, ClientSt1}
    end),
    {Result, save_client_assignment(Connection, ClientPid, St2)}.

-spec do_free_lease(connection(), state()) -> {FreeLeaseResult, state()} when
    FreeLeaseResult :: {ok, connection_group_id()} | {error, connection_not_found | client_state_not_found}.
do_free_lease(Connection, St0) ->
    ClientPid = get_client_assignment(Connection, St0),
    do_free_lease(ClientPid, Connection, St0).

-spec do_free_lease(client_pid(), connection(), state()) -> {FreeLeaseResult, state()} when
    FreeLeaseResult :: {ok, connection_group_id()} | {error, connection_not_found | client_state_not_found}.
do_free_lease(ClientPid, Connection, St0) ->
    {Result, St1} = with_client_state(ClientPid, St0, fun(ClientSt0) ->
        %% @TODO it is now possible to get rid of ReturnTo in client leases, consider
        case gunner_pool_client_state:free_lease(Connection, ClientSt0) of
            {ok, ConnectionGroupId, ClientSt1} ->
                {{ok, ConnectionGroupId}, ClientSt1};
            {error, _Reason} = Error ->
                {Error, ClientSt0}
        end
    end),
    {Result, remove_client_assignment(Connection, St1)}.

-spec do_cancel_lease(client_pid(), ticket(), state()) -> {CancelLeaseResult, state()} when
    CancelLeaseResult ::
        {ok, connection(), connection_group_id()} | {error, connection_not_found | client_state_not_found}.
do_cancel_lease(ClientPid, Ticket, St0) ->
    {Result, St1} = with_client_state(ClientPid, St0, fun(ClientSt0) ->
        case gunner_pool_client_state:cancel_lease(Ticket, ClientSt0) of
            {ok, Connection, ConnectionGroupId, ClientSt1} ->
                {{ok, Connection, ConnectionGroupId}, ClientSt1};
            {error, _Reason} = Error ->
                {Error, ClientSt0}
        end
    end),
    case Result of
        {ok, Connection, _} ->
            {Result, remove_client_assignment(Connection, St1)};
        {error, _} = Error ->
            {Error, St1}
    end.

%%

-spec get_client_assignment(connection(), state()) -> client_pid().
get_client_assignment(Connection, #{client_assignments := Assignments}) ->
    maps:get(Connection, Assignments).

-spec save_client_assignment(connection(), client_pid(), state()) -> state().
save_client_assignment(Connection, ClientPid, St = #{client_assignments := Assignments}) ->
    St#{client_assignments => Assignments#{Connection => ClientPid}}.

-spec remove_client_assignment(connection(), state()) -> state().
remove_client_assignment(Connection, St = #{client_assignments := Assignments}) ->
    St#{client_assignments => maps:without([Connection], Assignments)}.

%%

-spec process_create_connection(connection_args(), state()) ->
    {ok, connection(), state()} |
    {error, pool_unavailable} |
    {error, {connection_failed, Reason :: _}}.
process_create_connection(ConnectionArgs, St0 = #{pool := PoolSt}) ->
    TotalLimit = get_total_limit(St0),
    %% not sure if bug or feature
    PendingRequestCount = get_pending_request_count(St0),
    case is_pool_available(PoolSt, TotalLimit - PendingRequestCount) of
        true ->
            case start_connection_process(ConnectionArgs) of
                {ok, Connection, MRef} ->
                    {ok, Connection, save_connection_monitor(Connection, MRef, St0)};
                {error, Reason} ->
                    {error, {connection_failed, Reason}}
            end;
        false ->
            {error, pool_unavailable}
    end.

-spec process_kill_connection(connection(), state()) -> {ok, state()}.
process_kill_connection(Connection, St0) ->
    MRef = get_connection_monitor(Connection, St0),
    ok = kill_connection_process(Connection, MRef),
    {ok, remove_connection_monitor(Connection, St0)}.

-spec is_pool_available(connection_pool(), Limit :: size()) -> boolean().
is_pool_available(PoolSt, Limit) ->
    case gunner_connection_pool:total_count(PoolSt) of
        Total when Total < Limit ->
            true;
        Total when Total >= Limit ->
            false
    end.

-spec is_pool_oversized(connection_pool(), Limit :: size()) -> boolean().
is_pool_oversized(PoolSt, Limit) ->
    case gunner_connection_pool:free_count(PoolSt) of
        Free when Free > Limit ->
            true;
        Free when Free =< Limit ->
            false
    end.

%%

-spec create_connection_request(from(), connection_group_id(), ticket()) -> connection_request().
create_connection_request(Requester, GroupID, Ticket) ->
    #{
        requester => Requester,
        group_id => GroupID,
        ticket => Ticket
    }.

-spec connection_request_exists(connection(), state()) -> boolean().
connection_request_exists(ConnectionPid, #{connection_requests := Requests}) ->
    maps:is_key(ConnectionPid, Requests).

-spec save_connection_request(connection(), connection_request(), state()) -> state().
save_connection_request(ConnectionPid, ConnRequestSt, St = #{connection_requests := Requests0}) ->
    Requests1 = Requests0#{ConnectionPid => ConnRequestSt},
    St#{connection_requests => Requests1}.

-spec remove_connection_request(connection(), state()) -> state().
remove_connection_request(ConnectionPid, St = #{connection_requests := Requests}) ->
    St#{connection_requests => maps:without([ConnectionPid], Requests)}.

-spec get_connection_request(connection(), state()) -> {ok, connection_request()} | not_found.
get_connection_request(ConnectionPid, #{connection_requests := Requests0}) ->
    case maps:get(ConnectionPid, Requests0, undefined) of
        ConnRequestSt when ConnRequestSt =/= undefined ->
            {ok, ConnRequestSt};
        undefined ->
            not_found
    end.

%%

-spec start_connection_process(connection_args()) -> {ok, connection(), monitor_ref()} | {error, Reason :: any()}.
start_connection_process({Host, Port}) ->
    ClientOpts = get_gun_client_opts(),
    case gun:open(Host, Port, ClientOpts#{retry => 0}) of
        {ok, ConnPid} ->
            MRef = monitor(process, ConnPid),
            {ok, ConnPid, MRef};
        {error, _Reason} = Error ->
            Error
    end.

-spec kill_connection_process(connection(), monitor_ref()) -> ok.
kill_connection_process(ConnectionPid, MRef) ->
    true = demonitor(MRef, [flush]),
    gun:shutdown(ConnectionPid).

-spec get_gun_client_opts() -> gun_client_opts().
get_gun_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).

%% Helpers

-spec with_pool(state(), fun((connection_pool()) -> {Result, connection_pool()})) -> {Result, state()} when
    Result :: any().
with_pool(St = #{pool := PoolSt}, Fun) ->
    case Fun(PoolSt) of
        {Result, PoolSt} ->
            {Result, St};
        {Result, NewPoolSt} ->
            {Result, St#{pool => NewPoolSt}}
    end.

-spec with_client_state(client_pid(), state(), fun((client_state()) -> {Result, client_state()})) ->
    {Result, state()}
when
    Result :: {error, client_state_not_found} | any().
with_client_state(ClientPid, St = #{clients := Clients0}, Fun) ->
    case get_client_state(ClientPid, Clients0) of
        ClientSt when ClientSt =/= undefined ->
            case Fun(ClientSt) of
                {Result, ClientSt} ->
                    {Result, St};
                {Result, NewClientSt} ->
                    {Result, St#{clients => save_client_state(ClientPid, NewClientSt, Clients0)}}
            end;
        undefined ->
            {{error, client_state_not_found}, St}
    end.

-spec ensure_client_state_exists(client_pid(), state()) -> state().
ensure_client_state_exists(ClientPid, St0 = #{clients := Clients0}) ->
    case client_state_exists(ClientPid, Clients0) of
        true ->
            St0;
        false ->
            ClientSt0 = gunner_pool_client_state:new(ClientPid),
            St0#{clients => save_client_state(ClientPid, ClientSt0, Clients0)}
    end.

-spec client_state_exists(client_pid(), clients()) -> boolean().
client_state_exists(ClientPid, Clients) ->
    maps:is_key(ClientPid, Clients).

-spec get_client_state(client_pid(), clients()) -> client_state() | undefined.
get_client_state(ClientPid, Clients) ->
    maps:get(ClientPid, Clients, undefined).

-spec save_client_state(client_pid(), client_state(), clients()) -> clients().
save_client_state(ClientPid, ClientSt, Clients) ->
    Clients#{ClientPid => ClientSt}.

-spec remove_client_state(client_pid(), state()) -> state().
remove_client_state(ClientPid, St = #{clients := Clients}) ->
    St#{clients => maps:without([ClientPid], Clients)}.

%%

-spec save_connection_monitor(connection(), monitor_ref(), state()) -> state().
save_connection_monitor(Connection, MRef, St = #{connection_monitors := Monitors}) ->
    St#{connection_monitors => Monitors#{Connection => MRef}}.

-spec get_connection_monitor(connection(), state()) -> monitor_ref().
get_connection_monitor(Connection, #{connection_monitors := Monitors}) ->
    maps:get(Connection, Monitors).

-spec remove_connection_monitor(connection(), state()) -> state().
remove_connection_monitor(Connection, St = #{connection_monitors := Monitors}) ->
    St#{connection_monitors => maps:without([Connection], Monitors)}.

%%

-spec get_pool_status(state()) -> status_response().
get_pool_status(#{pool := PoolSt}) ->
    #{
        total_count => gunner_connection_pool:total_count(PoolSt),
        free_count => gunner_connection_pool:free_count(PoolSt)
    }.

get_total_limit(#{total_connection_limit := TotalLimit}) ->
    TotalLimit.

get_pending_request_count(#{connection_requests := ConnectionRequests}) ->
    maps:size(ConnectionRequests).
