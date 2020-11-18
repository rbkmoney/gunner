-module(gunner_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%
%% API functions
%%
-spec start_link() -> genlib_gen:start_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%
%% Supervisor callbacks
%%
-spec init(Args :: term()) -> genlib_gen:supervisor_ret().
init([]) ->
    SupFlags = #{
        strategy => one_for_all
    },
    Children = [
        #{
            id => gunner_connection_sup,
            start => {gunner_connection_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => brutal_kill
        },
        #{
            id => gunner_pool_sup,
            start => {gunner_pool_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => brutal_kill
        },
        #{
            id => gunner_manager,
            start => {gunner_manager, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => brutal_kill
        }
    ],
    {ok, {SupFlags, Children}}.
