-module(gunner_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_pool/2]).
-export([stop_pool/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%
%% API functions
%%
-spec start_link() -> genlib_gen:start_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_pool(gunner_pool:pool_reg_name() | undefined, gunner_pool:pool_opts()) -> {ok, pid()} | {error, _}.
start_pool(PoolRegName, PoolOpts) ->
    supervisor:start_child(?SERVER, [PoolRegName, PoolOpts]).

-spec stop_pool(pid()) -> ok | {error, term()}.
stop_pool(Pid) ->
    supervisor:terminate_child(?SERVER, Pid).

%%
%% Supervisor callbacks
%%
-spec init(Args :: term()) -> genlib_gen:supervisor_ret().
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one
    },
    Children = [
        #{
            id => poolN,
            start => {gunner_pool, start_link, []},
            restart => permanent,
            shutdown => brutal_kill
        }
    ],
    {ok, {SupFlags, Children}}.
