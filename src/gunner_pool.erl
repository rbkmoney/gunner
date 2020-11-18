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

-export_type([pool/0]).
-export_type([pool_id/0]).

%% Internal types
-type conn_host() :: gunner:conn_host().
-type conn_port() :: gunner:conn_port().
-type client_opts() :: gunner_connection:client_opts().

-type connection() :: gunner_connection:connection().

-type connection_endpoint() :: {conn_host(), conn_port()}.
-type connection_group() :: gunner_connection_group:state().

-type connections() :: #{
    connection_endpoint() => connection_group()
}.

-type state() :: #{
    id := pool_id(),
    size := integer(),
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
    Group0 =
        case get_active_group_for_endpoint(Endpoint, St0) of
            Group when Group =/= undefined ->
                Group;
            undefined ->
                gunner_connection_group:new()
        end,
    case maybe_expand_pool(Host, Port, Group0, St0) of
        {ok, Group1, St1} ->
            case gunner_connection_group:acquire_connection(Group1) of
                {ok, Connection, Group2} ->
                    {reply, {ok, Connection}, set_active_group_for_endpoint(Endpoint, Group2, St1)};
                {error, _} = Error ->
                    {reply, Error, St0}
            end;
        {error, _} = Error ->
            {reply, Error, St0}
    end;
handle_call({free_connection, Host, Port, Connection}, _From, St0) ->
    Endpoint = endpoint_from_host_port(Host, Port),
    case get_active_group_for_endpoint(Endpoint, St0) of
        ActiveGroup0 when ActiveGroup0 =/= undefined ->
            case handle_free_connection(Connection, ActiveGroup0, St0) of
                {active, ActiveGroup1, St1} ->
                    {reply, ok, set_active_group_for_endpoint(Endpoint, ActiveGroup1, St1)};
                {inactive, InactiveGroup0, St1} ->
                    {reply, ok, set_inactive_connections(InactiveGroup0, St1)};
                {error, _} = Error ->
                    {reply, Error, St0}
            end;
        undefined ->
            {reply, {error, group_not_found}, St0}
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

-spec new_state(pool_id()) -> state().
new_state(PoolID) ->
    #{
        id => PoolID,
        size => 0,
        active_connections => #{},
        inactive_connections => gunner_connection_group:new()
    }.

%%

-spec endpoint_from_host_port(conn_host(), conn_port()) -> connection_endpoint().
endpoint_from_host_port(Host, Port) ->
    {Host, Port}.

-spec get_active_group_for_endpoint(connection_endpoint(), state()) -> connection_group() | undefined.
get_active_group_for_endpoint(Endpoint, #{active_connections := Connections}) ->
    maps:get(Endpoint, Connections, undefined).

-spec set_active_group_for_endpoint(connection_endpoint(), connection_group(), state()) -> state().
set_active_group_for_endpoint(Endpoint, Group, St = #{active_connections := Connections0}) ->
    Connections1 = maps:put(Endpoint, Group, Connections0),
    St#{active_connections => Connections1}.

%%

-spec get_inactive_connections(state()) -> connection_group().
get_inactive_connections(#{inactive_connections := InactiveGroup}) ->
    InactiveGroup.

-spec set_inactive_connections(connection_group(), state()) -> state().
set_inactive_connections(InactiveGroup, St0) ->
    St0#{inactive_connections => InactiveGroup}.

%%

-spec handle_free_connection(connection(), connection_group(), state()) ->
    {active | inactive, connection_group(), state()} | {error, Reason :: term()}.
handle_free_connection(Connection, ActiveGroup0, St0) ->
    case handle_free_active_connection(Connection, ActiveGroup0, St0) of
        {ok, ActiveGroup1, St1} ->
            {active, ActiveGroup1, St1};
        {error, _} ->
            case handle_free_inactive_connection(Connection, get_inactive_connections(St0), St0) of
                {ok, InactiveGroup0, St1} ->
                    {inactive, InactiveGroup0, St1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec handle_free_active_connection(connection(), connection_group(), state()) ->
    {ok, connection_group(), state()} | {error, Reason :: term()}.
handle_free_active_connection(Connection, ActiveGroup0, St0) ->
    case gunner_connection_group:free_connection(Connection, ActiveGroup0) of
        {ok, ActiveGroup1} ->
            case pool_should_shrink(ActiveGroup1, St0) of
                true ->
                    handle_pool_shrink(Connection, ActiveGroup1, St0);
                false ->
                    {ok, ActiveGroup1, St0}
            end;
        {error, _} = Error ->
            Error
    end.

-spec handle_free_inactive_connection(connection(), connection_group(), state()) ->
    {ok, connection_group(), state()} | {error, Reason :: term()}.
handle_free_inactive_connection(Connection, InactiveGroup0, St0) ->
    case gunner_connection_group:free_connection(Connection, InactiveGroup0) of
        {ok, InactiveGroup1} ->
            case gunner_connection_group:load(Connection, InactiveGroup1) of
                0 ->
                    ok = exit_connection(Connection),
                    {ok, InactiveGroup2} = gunner_connection_group:delete_connection(Connection, InactiveGroup1),
                    {ok, InactiveGroup2, St0};
                _ ->
                    {ok, InactiveGroup1, St0}
            end;
        {error, _} = Error ->
            Error
    end.

%%

-spec handle_pool_shrink(connection(), connection_group(), state()) ->
    {ok, connection_group(), state()} | {error, Reason :: term()}.
handle_pool_shrink(Connection, ActiveGroup0, St0) ->
    case gunner_connection_group:delete_connection(Connection, ActiveGroup0) of
        {ok, ActiveGroup1} ->
            case gunner_connection_group:load(Connection, ActiveGroup0) of
                0 ->
                    ok = exit_connection(Connection),
                    {ok, ActiveGroup1, decrement_pool_size(St0)};
                ConnLoad when ConnLoad > 0 ->
                    InactiveGroup0 = get_inactive_connections(St0),
                    InactiveGroup1 = gunner_connection_group:add_connection(Connection, ConnLoad, InactiveGroup0),
                    {ok, ActiveGroup1, decrement_pool_size(set_inactive_connections(InactiveGroup1, St0))}
            end;
        {error, _} = Error ->
            Error
    end.

%%

-spec maybe_expand_pool(conn_host(), conn_port(), connection_group(), state()) ->
    {ok, connection_group(), state()} | {error, Reason :: term()}.
maybe_expand_pool(Host, Port, Group0, St0) ->
    case pool_needs_expansion(Group0, St0) of
        {true, below_limit} ->
            case create_connection(Host, Port, get_client_opts(St0)) of
                {ok, Connection0} ->
                    Group1 = gunner_connection_group:add_connection(Connection0, 0, Group0),
                    {ok, Group1, increment_pool_size(St0)};
                {error, _} = Error ->
                    Error
            end;
        {true, limit_reached} ->
            {error, pool_unavailable};
        false ->
            {ok, Group0, St0}
    end.

%%

-spec pool_needs_expansion(connection_group(), state()) -> {true, below_limit | limit_reached} | false.
pool_needs_expansion(Group, St) ->
    case pool_should_expand(Group, St) of
        true ->
            case pool_below_limit(St) of
                true ->
                    {true, below_limit};
                false ->
                    {true, limit_reached}
            end;
        false ->
            false
    end.

-spec pool_should_expand(connection_group(), state()) -> boolean().
pool_should_expand(_Group, #{size := PoolSize}) when PoolSize =:= 0 ->
    true;
pool_should_expand(Group, #{size := PoolSize}) when PoolSize > 0 ->
    CurrentGroupSize = gunner_connection_group:size(Group),
    %% We calculate if we need to expand before the load actually increases, so thats why we need a +1
    %% @TODO Maybe make this more obvious later
    CurrentGroupLoad = gunner_connection_group:load(Group) + 1,
    MaxConnectionLoad = genlib_app:env(gunner, max_connection_load, ?DEFAULT_MAX_LOAD_PER_CONNECTION),
    case target_connections_for_group_load(CurrentGroupLoad, MaxConnectionLoad) of
        Size when Size > CurrentGroupSize ->
            true;
        _ ->
            false
    end.

-spec pool_below_limit(state()) -> boolean().
pool_below_limit(#{size := PoolSize}) ->
    MaxPoolSize = genlib_app:env(gunner, max_pool_size, ?DEFAULT_MAX_POOL_SIZE),
    PoolSize < MaxPoolSize.

-spec pool_should_shrink(connection_group(), state()) -> boolean().
pool_should_shrink(_Group, #{size := PoolSize}) when PoolSize =:= 0 ->
    false;
pool_should_shrink(Group, #{size := PoolSize}) when PoolSize > 0 ->
    CurrentGroupSize = gunner_connection_group:size(Group),
    CurrentGroupLoad = gunner_connection_group:load(Group),
    MaxConnectionLoad = genlib_app:env(gunner, max_connection_load, ?DEFAULT_MAX_LOAD_PER_CONNECTION),
    case target_connections_for_group_load(CurrentGroupLoad, MaxConnectionLoad) of
        Size when Size < CurrentGroupSize ->
            true;
        _ ->
            false
    end.

-spec target_connections_for_group_load(integer(), integer()) -> integer().
target_connections_for_group_load(CurrentGroupLoad, MaxConnectionLoad) ->
    round(math:ceil(CurrentGroupLoad / MaxConnectionLoad)).

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

%%

-spec remove_connection_by_pid(connection(), state()) -> state().
remove_connection_by_pid(Connection, #{active_connections := Connections0} = St0) ->
    case remove_active_connection_by_pid(Connection, Connections0) of
        {Connections1, true} ->
            decrement_pool_size(St0#{active_connections => Connections1});
        {_, false} ->
            InactiveGroup0 = get_inactive_connections(St0),
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

-spec get_client_opts(state()) -> gunner_connection:client_opts().
get_client_opts(_St) ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).

increment_pool_size(St0 = #{size := PoolSize}) ->
    St0#{size => PoolSize + 1}.

decrement_pool_size(St0 = #{size := PoolSize}) ->
    St0#{size => PoolSize - 1}.

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec target_connections_for_group_load_test() -> _.

target_connections_for_group_load_test() ->
    MaxLoadPerConnection = 5,
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(pid_1, 0, GroupSt0),
    {ok, pid_1, GroupSt2} = gunner_connection_group:acquire_connection(GroupSt1),
    {ok, pid_1, GroupSt3} = gunner_connection_group:acquire_connection(GroupSt2),
    {ok, pid_1, GroupSt4} = gunner_connection_group:acquire_connection(GroupSt3),
    {ok, pid_1, GroupSt5} = gunner_connection_group:acquire_connection(GroupSt4),
    {ok, pid_1, GroupSt6} = gunner_connection_group:acquire_connection(GroupSt5),
    {ok, pid_1, GroupSt7} = gunner_connection_group:acquire_connection(GroupSt6),
    CurrentGroupLoad0 = gunner_connection_group:load(GroupSt7),
    ?assertEqual(2, target_connections_for_group_load(CurrentGroupLoad0, MaxLoadPerConnection)),
    {ok, GroupSt8} = gunner_connection_group:free_connection(pid_1, GroupSt7),
    CurrentGroupLoad1 = gunner_connection_group:load(GroupSt8),
    ?assertEqual(1, target_connections_for_group_load(CurrentGroupLoad1, MaxLoadPerConnection)).

-endif.
