-module(gunner_gun_worker_factory).

%% Behavour

-behaviour(gunner_worker_factory).

-export([on_acquire/1]).
-export([create_worker/1]).
-export([exit_worker/1]).

%% Internal types

-type worker() :: gunner_worker_factory:worker(pid()).
-type worker_args() :: gunner_worker_factory:worker_args(conn_args()).
-type worker_group_id() :: gunner_pool:worker_group_id().

-type conn_host() :: inet:hostname() | inet:ip_address().
-type conn_port() :: inet:port_number().
-type endpoint() :: {conn_host(), conn_port()}.
-type conn_args() :: {conn_host(), conn_port()}.
-type client_opts() :: gun:opts().

%%

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).

%%
%% Behavour
%%

-spec on_acquire(worker_args()) -> {worker_group_id(), worker_args()}.
on_acquire({Host, Port} = Opts) ->
    %% @TODO Can resolve here.
    Endpoint = endpoint_from_host_port(Host, Port),
    {Endpoint, Opts}.

-spec create_worker(worker_args()) -> {ok, worker()} | {error, Reason :: term()}.
create_worker({Host, Port}) ->
    ClientOpts = get_client_opts(),
    case gun:open(Host, Port, ClientOpts#{retry => 0}) of
        {ok, Client} ->
            Timeout = maps:get(connect_timeout, ClientOpts),
            case gun:await_up(Client, Timeout) of
                {ok, _} ->
                    {ok, Client};
                {error, Reason} ->
                    {error, {connection_failed, Reason}}
            end;
        {error, Reason = {options, _}} ->
            {error, {invalid_client_options, Reason, ClientOpts}}
    end.

-spec exit_worker(worker()) -> ok.
exit_worker(Worker) ->
    ok = gun:shutdown(Worker).

%% Internal functions

-spec endpoint_from_host_port(conn_host(), conn_port()) -> endpoint().
endpoint_from_host_port(Host, Port) ->
    {Host, Port}.

-spec get_client_opts() -> client_opts().
get_client_opts() ->
    genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS).
