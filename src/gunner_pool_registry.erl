%% Now this is way overkill :/

-module(gunner_pool_registry).

%% API

-export([via_tuple/1]).
-export([whereis/1]).

%% Behavour

-callback via_tuple(pool_id()) -> via_tuple().
-callback whereis(pool_id()) -> pid() | undefined.

%% API Types

-type via_tuple() :: {via, module(), name_tuple()}.

-export_type([via_tuple/0]).

%% Internal types

-type pool_id() :: gunner_pool:pool_id().
-type name_tuple() :: tuple().

-define(DEFAULT_PROCESS_REGISTRY, gunner_pool_registry_global).

%%
%% API functions
%%

-spec via_tuple(pool_id()) -> via_tuple().
via_tuple(PoolID) ->
    Handler = get_handler(),
    Handler:via_tuple(PoolID).

-spec whereis(pool_id()) -> pid() | undefined.
whereis(PoolID) ->
    Handler = get_handler(),
    Handler:whereis(PoolID).

%%
%% Internal functions
%%

-spec get_handler() -> module().
get_handler() ->
    % I kinda miss compile-time env
    genlib_app:env(gunner, process_registry_handler, ?DEFAULT_PROCESS_REGISTRY).
