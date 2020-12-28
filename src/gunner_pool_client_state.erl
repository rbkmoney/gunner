% @TODO unsure how to name this. NOT a client interface TO the pool, but rather a client's state in the pool.
% This whole module is kinda wonky, now that i'm thinking about it, but I wanted a nice way to handle data related to
% library's clients, like list of leases and monitors
-module(gunner_pool_client_state).

%% API functions

-export([new/1]).

-export([register_lease/3]).
-export([cancel_lease/1]).
-export([return_lease/2]).

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

-type connection_leases() :: [connection_lease()].
-type connection_lease() :: {connection(), ReturnTo :: connection_group_id()}.

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
-spec register_lease(connection(), connection_group_id(), state()) -> state().
register_lease(Connection, ReturnTo, St = #{connection_leases := Leases}) ->
    St#{connection_leases => [new_lease(Connection, ReturnTo) | Leases]}.

%% @doc Cancels last lease for this client
-spec cancel_lease(state()) ->
    {ok, connection(), connection_group_id(), state()} | {error, no_leases | connection_not_found}.
cancel_lease(#{connection_leases := []}) ->
    {error, no_leases};
cancel_lease(St0 = #{connection_leases := [{LastConnection, _} | _]}) ->
    case return_lease(LastConnection, St0) of
        {ok, ConnectionGroupId, St1} ->
            {ok, LastConnection, ConnectionGroupId, St1};
        {error, _} = Error ->
            Error
    end.

%% @doc Returns the leased connection
-spec return_lease(connection(), state()) ->
    {ok, connection_group_id(), state()} | {error, no_leases | connection_not_found}.
return_lease(_Connection, #{connection_leases := []}) ->
    {error, no_leases};
return_lease(Connection, St = #{connection_leases := Leases}) ->
    case find_and_remove_lease(Connection, Leases) of
        {ok, ConnectionGroupId, NewLeases} ->
            {ok, ConnectionGroupId, St#{connection_leases => NewLeases}};
        {error, _} = Error ->
            Error
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

-spec new_lease(connection(), connection_group_id()) -> connection_lease().
new_lease(Connection, ReturnTo) ->
    {Connection, ReturnTo}.

-spec find_and_remove_lease(connection(), connection_leases()) ->
    {ok, connection_group_id(), connection_leases()} | {error, connection_not_found}.
find_and_remove_lease(Connection, Leases) ->
    case lists:keytake(Connection, 1, Leases) of
        {value, {_, ReturnTo}, NewLeases} ->
            {ok, ReturnTo, NewLeases};
        false ->
            {error, connection_not_found}
    end.
