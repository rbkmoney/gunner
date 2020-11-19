-module(gunner_pool).

%% API functions
-export([start_link/1]).
-export([acquire_connection/4]).
-export([free_connection/4]).

%% Gen Server callbacks
-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

%% API Types
-type pool() :: pid().
-type pool_id() :: term().
-type pool_size() :: integer().

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_size/0]).

%% Internal types
-type conn_host() :: gunner:conn_host().
-type conn_port() :: gunner:conn_port().
-type client_opts() :: gunner_connection:client_opts().

-type connection() :: gunner_connection:connection().

-type connection_endpoint() :: {conn_host(), conn_port()}.
-type connection_group() :: gunner_connection_group:state().
-type connection_type() :: active | inactive.

-type connections() :: #{
    connection_endpoint() => connection_group()
}.

-type state() :: #{
    id := pool_id(),
    size := pool_size(),
    active_connections := connections(),
    inactive_connections := connection_group()
}.

%%

-define(DEFAULT_MAX_POOL_SIZE, 5).
-define(DEFAULT_MAX_LOAD_PER_CONNECTION, 5).
-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).

%%
%% API functions
%%

-spec start_link(pool_id()) -> genlib_gen:start_ret().
start_link(PoolID) ->
    gen_server:start_link(?MODULE, [PoolID], []).

-spec acquire_connection(pool(), conn_host(), conn_port(), timeout()) -> {ok, connection()} | {error, Reason :: term()}.
acquire_connection(Pool, Host, Port, Timeout) ->
    gen_server:call(Pool, {acquire_connection, Host, Port}, Timeout).

-spec free_connection(pool(), conn_host(), conn_port(), connection()) -> ok.
free_connection(Pool, Host, Port, Connection) ->
    gen_server:call(Pool, {free_connection, Host, Port, Connection}).

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolID]) ->
    {ok, new_state(PoolID)}.

-spec handle_call(any(), _, state()) -> {reply, {ok, connection()} | {error, Reason :: term()}, state()}.
handle_call({acquire_connection, Host, Port}, _From, St0) ->
    Endpoint = endpoint_from_host_port(Host, Port),
    PoolSize = get_pool_size(St0),
    ActiveGroup0 =
        case get_active_connection_group(Endpoint, St0) of
            Group when Group =/= undefined ->
                Group;
            undefined ->
                gunner_connection_group:new()
        end,
    case handle_acquire_connection(Endpoint, ActiveGroup0, PoolSize) of
        {ok, Connection, ActiveGroup1, NewPoolSize} ->
            St1 = set_active_connection_group(Endpoint, ActiveGroup1, St0),
            St2 = set_pool_size(NewPoolSize, St1),
            {reply, {ok, Connection}, St2};
        {error, pool_unavailable} = Error ->
            {reply, Error, St0};
        {error, {connection_error, _}} = Error ->
            {reply, Error, St0}
    end;
handle_call({free_connection, Host, Port, Connection}, _From, St0) ->
    Endpoint = endpoint_from_host_port(Host, Port),
    ConnectionType = determine_connection_type(Connection, Endpoint, St0),
    case handle_free_connection(ConnectionType, Connection, Endpoint, St0) of
        {ok, St1} ->
            {reply, ok, St1};
        {error, connection_not_found} = Error ->
            {reply, Error, St0}
    end;
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({'DOWN', _Mref, process, ConnectionPid, _Reason}, St0) ->
    St1 = remove_connection_by_pid(ConnectionPid, St0),
    {noreply, St1}.

%%
%% Internal functions
%%

-spec handle_acquire_connection(connection_endpoint(), connection_group(), pool_size()) ->
    {ok, connection(), connection_group(), pool_size()} | {error, pool_unavailable | {connection_error, _}}.
handle_acquire_connection(Endpoint, ActiveGroup0, PoolSize) ->
    case try_aquire_connection(ActiveGroup0) of
        {ok, Connection, ActiveGroup1} ->
            {ok, Connection, ActiveGroup1, PoolSize};
        {error, no_free_connections} ->
            try_expand_connection_group(Endpoint, ActiveGroup0, PoolSize)
    end.

-spec try_aquire_connection(connection_group()) ->
    {ok, connection(), connection_group()} | {error, no_free_connections}.
try_aquire_connection(ActiveGroup0) ->
    case check_max_load_reached(ActiveGroup0) of
        true ->
            {error, no_free_connections};
        false ->
            do_aquire_connection(ActiveGroup0)
    end.

-spec check_max_load_reached(connection_group()) -> boolean().
check_max_load_reached(ConnectionGroup) ->
    GroupSize = gunner_connection_group:size(ConnectionGroup),
    GroupLoad = gunner_connection_group:load(ConnectionGroup),
    MaxConnectionLoad = get_max_connection_load(),
    %% We calculate if we need to expand before the load actually increases, so thats why we need a +1
    %% @TODO Maybe make this more obvious later
    is_max_load_reached(GroupSize, GroupLoad + 1, MaxConnectionLoad).

-spec is_max_load_reached(Size :: integer(), Load :: integer(), MaxLoad :: integer()) -> boolean().
is_max_load_reached(Size, Load, MaxLoad) ->
    case target_connections_for_group_load(Load, MaxLoad) of
        TargetSize when TargetSize > Size ->
            true;
        _ ->
            false
    end.

-spec check_min_load_reached(connection_group()) -> boolean().
check_min_load_reached(ConnectionGroup) ->
    GroupSize = gunner_connection_group:size(ConnectionGroup),
    GroupLoad = gunner_connection_group:load(ConnectionGroup),
    MaxConnectionLoad = get_max_connection_load(),
    is_min_load_reached(GroupSize, GroupLoad, MaxConnectionLoad).

-spec is_min_load_reached(Size :: integer(), Load :: integer(), MaxLoad :: integer()) -> boolean().
is_min_load_reached(Size, Load, MaxLoad) ->
    case target_connections_for_group_load(Load, MaxLoad) of
        TargetSize when TargetSize < Size ->
            true;
        _ ->
            false
    end.

-spec target_connections_for_group_load(integer(), integer()) -> integer().
target_connections_for_group_load(CurrentGroupLoad, MaxConnectionLoad) ->
    round(math:ceil(CurrentGroupLoad / MaxConnectionLoad)).

-spec do_aquire_connection(connection_group()) -> {ok, connection(), connection_group()} | {error, no_free_connections}.
do_aquire_connection(ActiveGroup0) ->
    case gunner_connection_group:acquire_connection(ActiveGroup0) of
        {ok, _Connection, _ActiveGroup1} = Ok ->
            Ok;
        {error, no_connections} ->
            {error, no_free_connections}
    end.

-spec try_expand_connection_group(connection_endpoint(), connection_group(), pool_size()) ->
    {ok, connection(), connection_group(), pool_size()} | {error, pool_unavailable | {connection_error, _}}.
try_expand_connection_group({Host, Port}, ActiveGroup0, PoolSize) ->
    case can_pool_expand(PoolSize) of
        true ->
            ClientOpts = get_connection_client_opts(),
            try_create_new_connection(Host, Port, ClientOpts, PoolSize, ActiveGroup0);
        false ->
            {error, pool_unavailable}
    end.

-spec try_create_new_connection(conn_host(), conn_port(), client_opts(), pool_size(), connection_group()) ->
    {ok, connection(), connection_group(), pool_size()} | {error, {connection_error, _}}.
try_create_new_connection(Host, Port, ClientOpts, CurrentPoolSize, ActiveGroup0) ->
    case create_connection(Host, Port, ClientOpts) of
        {ok, Connection} ->
            ActiveGroup1 = gunner_connection_group:add_connection(Connection, 1, ActiveGroup0),
            {ok, Connection, ActiveGroup1, CurrentPoolSize + 1};
        {error, Error} ->
            {error, {connection_error, Error}}
    end.

%%

-spec determine_connection_type(connection(), connection_endpoint(), state()) -> connection_type() | undefined.
determine_connection_type(Connection, Endpoint, State) ->
    case is_connection_in_group(Connection, get_inactive_connection_group(State)) of
        true ->
            inactive;
        false ->
            case is_connection_in_active_group(Connection, Endpoint, State) of
                true ->
                    active;
                false ->
                    undefined
            end
    end.

-spec is_connection_in_active_group(connection(), connection_endpoint(), state()) -> boolean().
is_connection_in_active_group(Connection, Endpoint, State) ->
    case get_active_connection_group(Endpoint, State) of
        ActiveGroup when ActiveGroup =/= undefined ->
            is_connection_in_group(Connection, ActiveGroup);
        undefined ->
            false
    end.

-spec is_connection_in_group(connection(), connection_group()) -> boolean().
is_connection_in_group(Connection, ConnectionGroup) ->
    gunner_connection_group:is_member(Connection, ConnectionGroup).

-spec handle_free_connection(connection_type(), connection(), connection_endpoint(), state()) ->
    {ok, state()} | {error, connection_not_found}.
handle_free_connection(active, Connection, Endpoint, St0) ->
    ActiveGroup0 = get_active_connection_group(Endpoint, St0),
    {ok, ActiveGroup1} = gunner_connection_group:free_connection(Connection, ActiveGroup0),
    case check_min_load_reached(ActiveGroup1) of
        true ->
            handle_shrink_pool(Connection, Endpoint, ActiveGroup1, St0);
        false ->
            {ok, set_active_connection_group(Endpoint, ActiveGroup1, St0)}
    end;
handle_free_connection(inactive, Connection, _Endpoint, St0) ->
    InactiveGroup0 = get_inactive_connection_group(St0),
    {ok, InactiveGroup1} = gunner_connection_group:free_connection(Connection, InactiveGroup0),
    {ok, set_inactive_connection_group(InactiveGroup1, St0)};
handle_free_connection(undefined, _Connection, _Endpoint, _St0) ->
    {error, connection_not_found}.

-spec handle_shrink_pool(connection(), connection_endpoint(), connection_group(), state()) -> {ok, state()}.
handle_shrink_pool(Connection, Endpoint, ActiveGroup0, St0) ->
    {ok, ActiveGroup1} = gunner_connection_group:delete_connection(Connection, ActiveGroup0),
    St1 = set_pool_size(get_pool_size(St0) - 1, St0),
    St2 = set_active_connection_group(Endpoint, ActiveGroup1, St1),
    case gunner_connection_group:load(Connection, ActiveGroup0) of
        0 ->
            ok = exit_connection(Connection),
            {ok, St2};
        ConnLoad when ConnLoad > 0 ->
            InactiveGroup0 = get_inactive_connection_group(St0),
            InactiveGroup1 = gunner_connection_group:add_connection(Connection, ConnLoad, InactiveGroup0),
            {ok, set_inactive_connection_group(InactiveGroup1, St2)}
    end.

%%

-spec new_state(pool_id()) -> state().
new_state(PoolID) ->
    #{
        id => PoolID,
        size => 0,
        active_connections => #{},
        inactive_connections => gunner_connection_group:new()
    }.

-spec get_pool_size(state()) -> pool_size().
get_pool_size(#{size := Size}) ->
    Size.

-spec set_pool_size(pool_size(), state()) -> state().
set_pool_size(Size, St0) ->
    St0#{size => Size}.

-spec can_pool_expand(pool_size()) -> boolean().
can_pool_expand(CurrentPoolSize) ->
    MaxPoolSize = genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE),
    CurrentPoolSize < MaxPoolSize.

%%

-spec endpoint_from_host_port(conn_host(), conn_port()) -> connection_endpoint().
endpoint_from_host_port(Host, Port) ->
    {Host, Port}.

-spec get_active_connection_group(connection_endpoint(), state()) -> connection_group() | undefined.
get_active_connection_group(Endpoint, #{active_connections := Connections}) ->
    maps:get(Endpoint, Connections, undefined).

-spec set_active_connection_group(connection_endpoint(), connection_group(), state()) -> state().
set_active_connection_group(Endpoint, Group, St = #{active_connections := Connections0}) ->
    Connections1 = maps:put(Endpoint, Group, Connections0),
    St#{active_connections => Connections1}.

-spec get_inactive_connection_group(state()) -> connection_group().
get_inactive_connection_group(#{inactive_connections := InactiveGroup}) ->
    InactiveGroup.

-spec set_inactive_connection_group(connection_group(), state()) -> state().
set_inactive_connection_group(InactiveGroup, St0) ->
    St0#{inactive_connections => InactiveGroup}.

%%

-spec create_connection(conn_host(), conn_port(), client_opts()) -> {ok, connection()} | {error, Reason :: _}.
create_connection(Host, Port, Opts) ->
    case gunner_connection_sup:start_connection(Host, Port, Opts) of
        {ok, Connection} ->
            _ = erlang:monitor(process, Connection),
            {ok, Connection};
        {error, _} = Error ->
            Error
    end.

-spec exit_connection(connection()) -> ok.
exit_connection(Connection) ->
    ok = gunner_connection_sup:stop_connection(Connection).

-spec get_connection_client_opts() -> gunner_connection:client_opts().
get_connection_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).

-spec get_max_connection_load() -> integer().
get_max_connection_load() ->
    genlib_app:env(gunner, max_connection_load, ?DEFAULT_MAX_LOAD_PER_CONNECTION).

%%

-spec remove_connection_by_pid(connection(), state()) -> state().
remove_connection_by_pid(Connection, #{active_connections := Connections0} = St0) ->
    case remove_active_connection_by_pid(Connection, Connections0) of
        {Connections1, true} ->
            St1 = set_pool_size(get_pool_size(St0) - 1, St0),
            St1#{active_connections => Connections1};
        {_, false} ->
            InactiveGroup0 = get_inactive_connection_group(St0),
            case remove_inactive_connection_by_pid(Connection, InactiveGroup0) of
                {InactiveGroup1, true} ->
                    St0#{inactive_connections => InactiveGroup1};
                {_, false} ->
                    St0
            end
    end.

-spec remove_active_connection_by_pid(connection(), connections()) -> {connections(), Found :: boolean()}.
remove_active_connection_by_pid(Connection, ActiveConnections) ->
    maps:fold(
        fun(Key, ActiveGroup0, {Acc0, Found}) ->
            case gunner_connection_group:delete_connection(Connection, ActiveGroup0) of
                {ok, ActiveGroup1} ->
                    {Acc0#{Key => ActiveGroup1}, true};
                {error, connection_not_found} ->
                    {Acc0#{Key => ActiveGroup0}, Found}
            end
        end,
        {#{}, false},
        ActiveConnections
    ).

-spec remove_inactive_connection_by_pid(connection(), connection_group()) -> {connection_group(), Found :: boolean()}.
remove_inactive_connection_by_pid(Connection, InactiveGroup0) ->
    case gunner_connection_group:delete_connection(Connection, InactiveGroup0) of
        {ok, InactiveGroup1} ->
            {InactiveGroup1, true};
        {error, connection_not_found} ->
            {InactiveGroup0, false}
    end.
