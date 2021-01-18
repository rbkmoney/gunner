% @TODO unsure how to name this. NOT a client interface TO the pool, but rather a client's state in the pool.
% This whole module is kinda wonky, now that i'm thinking about it, but I wanted a nice way to handle data related to
% library's clients, like list of leases and monitors
-module(gunner_pool_client_state).

%% API functions

-export([new/1]).

-export([register_lease/4]).
-export([cancel_lease/2]).
-export([free_lease/2]).

-export([purge_leases/1]).

-export([destroy/1]).

%% API Types

-type state() :: #{
    connection_leases := connection_leases(),
    monitor := reference()
}.

-export_type([state/0]).

%% Internal Types

-type connection() :: gunner:connection().
-type connection_group_id() :: gunner_pool:connection_group_id().
-type ticket() :: reference().

-type connection_leases() :: [connection_lease()].
-type connection_lease() :: {connection(), ticket(), ReturnTo :: connection_group_id()}.

%%
%% API functions
%%

-spec new(pid()) -> state().
new(ClientPid) ->
    #{
        connection_leases => [],
        monitor => erlang:monitor(process, ClientPid)
    }.

%% @doc Registers a connection lease for this client
-spec register_lease(connection(), ticket(), connection_group_id(), state()) -> state().
register_lease(Connection, Ticket, ReturnTo, St = #{connection_leases := Leases}) ->
    St#{connection_leases => [new_lease(Connection, Ticket, ReturnTo) | Leases]}.

%% @doc Cancels lease using its ticket
-spec cancel_lease(ticket(), state()) ->
    {ok, connection(), connection_group_id(), state()} | {error, connection_not_found}.
cancel_lease(Ticket, St = #{connection_leases := Leases}) ->
    case lists:keytake(Ticket, 2, Leases) of
        {value, {Connection, _Ticket, ReturnTo}, NewLeases} ->
            {ok, Connection, ReturnTo, St#{connection_leases => NewLeases}};
        false ->
            {error, connection_not_found}
    end.

%% @doc Returns the leased connection
-spec free_lease(connection(), state()) -> {ok, connection_group_id(), state()} | {error, connection_not_found}.
free_lease(Connection, St = #{connection_leases := Leases}) ->
    case lists:keytake(Connection, 1, Leases) of
        {value, {_Connection, _Ticket, ReturnTo}, NewLeases} ->
            {ok, ReturnTo, St#{connection_leases => NewLeases}};
        false ->
            {error, connection_not_found}
    end.

%% @doc Purges all the leases
-spec purge_leases(state()) -> {connection_leases(), state()}.
purge_leases(St = #{connection_leases := Leases}) ->
    {Leases, St#{connection_leases => []}}.

-spec destroy(state()) -> ok | {error, active_leases_present}.
destroy(#{monitor := MRef, connection_leases := []}) ->
    _ = erlang:demonitor(MRef),
    ok;
destroy(_) ->
    {error, active_leases_present}.

%%
%% Internal functions
%%

-spec new_lease(connection(), ticket(), connection_group_id()) -> connection_lease().
new_lease(Connection, Ticket, ReturnTo) ->
    {Connection, Ticket, ReturnTo}.
