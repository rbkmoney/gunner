-module(gunner_connection_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_connection/3]).
-export([stop_connection/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%
%% API functions
%%
-spec start_link() -> genlib_gen:start_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_connection(
    inet:hostname() | inet:ip_address(),
    inet:port_number(),
    gunner_connection:client_opts()
) -> {ok, pid()} | {error, _}.
start_connection(Host, Port, Opts) ->
    supervisor:start_child(?SERVER, [Host, Port, Opts]).

-spec stop_connection(pid()) -> ok | {error, term()}.
stop_connection(Pid) ->
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
            id => connectionN,
            start => {gunner_connection, start_link, []},
            restart => transient,
            shutdown => brutal_kill
        }
    ],
    {ok, {SupFlags, Children}}.
