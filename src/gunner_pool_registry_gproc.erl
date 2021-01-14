-module(gunner_pool_registry_gproc).

%% Behavour

-behaviour(gunner_pool_registry).

-export([via_tuple/1]).
-export([whereis/1]).

%% Internal types

-type via_tuple() :: gunner_pool_registry:via_tuple().
-type pool_id() :: gunner_pool:pool_id().

%%
%% Behavour
%%

-spec via_tuple(pool_id()) -> via_tuple().
via_tuple(PoolID) ->
    {via, gproc, name_tuple(PoolID)}.

-spec whereis(pool_id()) -> pid() | undefined.
whereis(PoolID) ->
    % Dirty dialyzer hack
    erlang:apply(gproc, whereis_name, [name_tuple(PoolID)]).

%%
%% Internal functions
%%

-spec name_tuple(pool_id()) -> {n, l, {?MODULE, pool_id()}}.
name_tuple(PoolID) ->
    {n, l, {?MODULE, PoolID}}.
