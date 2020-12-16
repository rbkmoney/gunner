% @TODO unsure how to name this. NOT a client interface TO the pool, but rather a client's state in the pool.
% This whole module is kinda wonky, now that i'm thinking about it, but I wanted a nice way to handle data related to
% library's clients, like list of leases and monitors
-module(gunner_pool_client_state).

%% API functions

-export([new/0]).

-export([register_lease/3]).
-export([cancel_lease/1]).
-export([return_lease/2]).

-export([purge_leases/1]).

%% API Types

-type state() :: #{
    worker_leases := worker_leases()
}.

-export_type([state/0]).

%% Internal Types

-type worker() :: gunner_worker_factory:worker(_).
-type worker_group_id() :: gunner_pool:worker_group_id().

-type worker_leases() :: [worker_lease()].
-type worker_lease() :: {worker(), ReturnTo :: worker_group_id()}.

%%
%% API functions
%%

-spec new() -> state().
new() ->
    #{
        worker_leases => []
    }.

%% @doc Registers a worker lease for this client
-spec register_lease(worker(), worker_group_id(), state()) -> state().
register_lease(Worker, ReturnTo, St = #{worker_leases := Leases}) ->
    St#{worker_leases => [new_lease(Worker, ReturnTo) | Leases]}.

%% @doc Cancels last lease for this client
-spec cancel_lease(state()) -> {ok, worker(), worker_group_id(), state()} | {error, no_leases | worker_not_found}.
cancel_lease(#{worker_leases := []}) ->
    {error, no_leases};
cancel_lease(St0 = #{worker_leases := [{LastWorker, _} | _]}) ->
    case return_lease(LastWorker, St0) of
        {ok, WorkerGroupId, St1} ->
            {ok, LastWorker, WorkerGroupId, St1};
        {error, _} = Error ->
            Error
    end.

%% @doc Returns the leased worker
-spec return_lease(worker(), state()) -> {ok, worker_group_id(), state()} | {error, no_leases | worker_not_found}.
return_lease(_Worker, #{worker_leases := []}) ->
    {error, no_leases};
return_lease(Worker, St = #{worker_leases := Leases}) ->
    case find_and_remove_lease(Worker, Leases) of
        {ok, WorkerGroupId, NewLeases} ->
            {ok, WorkerGroupId, St#{worker_leases => NewLeases}};
        {error, _} = Error ->
            Error
    end.

%% @doc Purges all the leases
-spec purge_leases(state()) -> {worker_leases(), state()}.
purge_leases(St = #{worker_leases := Leases}) ->
    {Leases, St#{worker_leases => []}}.

%%
%% Internal functions
%%

-spec new_lease(worker(), worker_group_id()) -> worker_lease().
new_lease(Worker, ReturnTo) ->
    {Worker, ReturnTo}.

-spec find_and_remove_lease(worker(), worker_leases()) ->
    {ok, worker_group_id(), worker_leases()} | {error, worker_not_found}.
find_and_remove_lease(Worker, Leases) ->
    case lists:keytake(Worker, 1, Leases) of
        {value, {_, ReturnTo}, NewLeases} ->
            {ok, ReturnTo, NewLeases};
        false ->
            {error, worker_not_found}
    end.
