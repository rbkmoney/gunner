-module(gunner_connection).

%% API functions

-export([start_link/3]).
-export([request/7]).

%% Gen Server callbacks
-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% API types
-type connection() :: pid().
-type client_opts() :: gun:opts().

-type connection_opts() :: #{
    host := conn_host(),
    port := conn_port(),
    client_opts := client_opts()
}.

-type response_data() :: #{
    headers := resp_headers(),
    status_code := status_code() | unknown,
    body => body()
}.

-export_type([connection/0]).
-export_type([client_opts/0]).
-export_type([connection_opts/0]).
-export_type([response_data/0]).

%% Internal types

-type client() :: pid().

-type conn_host() :: gunner:conn_host().
-type conn_port() :: gunner:conn_port().

-type state() :: #{
    client_pid := client(),
    monitor_ref := reference(),
    connection_opts := connection_opts(),
    requests := requests()
}.

-type requests() :: #{
    request_id() => request_state()
}.

-type request_id() :: reference().
-type request_state() :: #{
    from := request_owner(),
    response => response_data()
}.

-type request_owner() :: {pid(), _Tag :: any()}.

-type method() :: gunner:req_method().
-type path() :: gunner:req_path().
-type req_headers() :: gunner:req_headers().
-type req_opts() :: gunner:req_opts().
-type status_code() :: non_neg_integer().
-type body() :: gunner:body().

-type resp_headers() :: [{binary(), binary()}].

%% Errors

-type connection_init_error() ::
    {connection_failed, _Reason} |
    {invalid_client_options, _Reason, client_opts()}.

%%

-define(DEFAULT_MAX_CONNECTION_IDLE_TIME, 5000).

%%
%% API functions
%%

-spec start_link(conn_host(), conn_port(), client_opts()) -> genlib_gen:start_ret().
start_link(Host, Port, Opts) ->
    ConnOpts = #{host => Host, port => Port, client_opts => Opts},
    gen_server:start_link(?MODULE, [ConnOpts], []).

-spec request(connection(), path(), method(), req_headers(), body(), req_opts(), timeout()) ->
    {ok, response_data()} | no_return().
request(Connection, Path, Method, Headers, Body, Opts, Timeout) ->
    gen_server:call(Connection, {request, Path, Method, Headers, Body, Opts}, Timeout).

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()} | {stop, connection_init_error()}.
init([ConnOpts = #{host := Host, port := Port, client_opts := ClientOpts}]) ->
    case start_client(Host, Port, ClientOpts) of
        {ok, Client, Mref} ->
            false = process_flag(trap_exit, true),
            {ok, new_state(Client, Mref, ConnOpts)};
        {error, Reason} ->
            {stop, Reason}
    end.

-spec handle_call(any(), request_owner(), state()) -> {noreply, state()} | no_return().
handle_call({request, Path, Method, Headers, Body, Opts}, From, St0) ->
    Client = get_client(St0),
    StreamRef = gun:request(Client, Method, Path, Headers, Body, Opts),
    St1 = add_request(StreamRef, From, St0),
    {noreply, St1};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()} | {stop, _Reason, state()}.
handle_info({gun_response, Client, StreamRef, nofin, StatusCode, Headers}, #{client_pid := Client} = St0) ->
    St1 = process_gun_response(StreamRef, StatusCode, Headers, St0),
    {noreply, St1};
handle_info({gun_response, Client, StreamRef, fin, StatusCode, Headers}, #{client_pid := Client} = St0) ->
    St1 = handle_fin(StreamRef, process_gun_response(StreamRef, StatusCode, Headers, St0)),
    {noreply, St1};
handle_info({gun_data, Client, StreamRef, nofin, Data}, #{client_pid := Client} = St0) ->
    St1 = process_gun_data(StreamRef, Data, St0),
    {noreply, St1};
handle_info({gun_data, Client, StreamRef, fin, Data}, #{client_pid := Client} = St0) ->
    St1 = handle_fin(StreamRef, process_gun_data(StreamRef, Data, St0)),
    {noreply, St1};
handle_info({gun_down, Client, _Protocol, Reason, Killed}, St0 = #{client_pid := Client}) ->
    {Host, Port, ClientOpts} = get_connection_opts(St0),
    St1 = kill_requests(Killed, {gun_down, Reason}, St0),
    case start_client(Host, Port, ClientOpts) of
        {ok, Client1, Mref1} ->
            {noreply, St1#{client_pid => Client1, monitor => Mref1}};
        {error, Reason} ->
            {stop, normal, St0}
    end;
handle_info({'DOWN', Mref, process, Client, Reason}, St0 = #{client_pid := Client, monitor_ref := Mref}) ->
    {Host, Port, ClientOpts} = get_connection_opts(St0),
    St1 = kill_requests(all, {down, Reason}, St0),
    case start_client(Host, Port, ClientOpts) of
        {ok, Client1, Mref1} ->
            {noreply, St1#{client_pid => Client1, monitor => Mref1}};
        {error, Reason} ->
            {stop, normal, St0}
    end.

-spec terminate(any(), state()) -> ok.
terminate(_Reason, St0) ->
    _ = kill_requests(all, terminate, St0),
    ok.

%%
%% Internal functions
%%

-spec new_state(client(), MRef :: reference(), connection_opts()) -> state().
new_state(Client, MRef, Opts) ->
    #{
        client_pid => Client,
        monitor_ref => MRef,
        connection_opts => Opts,
        requests => #{}
    }.

-spec process_gun_response(request_id(), status_code(), resp_headers(), state()) -> state().
process_gun_response(RequestId, StatusCode, Headers, St0) ->
    Request0 = get_request(RequestId, St0),
    Request1 = set_response_status(StatusCode, Request0),
    Request2 = set_response_headers(Headers, Request1),
    update_request(RequestId, Request2, St0).

-spec process_gun_data(request_id(), body(), state()) -> state().
process_gun_data(RequestId, NewData, St0) ->
    Request0 = get_request(RequestId, St0),
    Data0 = get_response_body(Request0),
    Request1 = set_response_body(append_response_data(Data0, NewData), Request0),
    update_request(RequestId, Request1, St0).

-spec handle_fin(request_id(), state()) -> state().
handle_fin(RequestId, St0) ->
    #{from := From, response := Response} = get_request(RequestId, St0),
    ok = gen_server:reply(From, {ok, Response}),
    remove_request(RequestId, St0).

-spec get_client(state()) -> client().
get_client(#{client_pid := Client}) ->
    Client.

-spec get_connection_opts(state()) -> {conn_host(), conn_port(), client_opts()}.
get_connection_opts(#{connection_opts := #{host := Host, port := Port, client_opts := ClientOpts}}) ->
    {Host, Port, ClientOpts}.

-spec start_client(conn_host(), conn_port(), client_opts()) ->
    {ok, client(), Mref :: reference()} |
    {error, {connection_failed, Reason :: term()}} |
    {error, {invalid_client_options, Reason :: term(), client_opts()}}.
start_client(Host, Port, ClientOpts) ->
    case gun:open(Host, Port, ClientOpts#{retry => 0}) of
        {ok, Client} ->
            Timeout = maps:get(connect_timeout, ClientOpts),
            case gun:await_up(Client, Timeout) of
                {ok, _} ->
                    MRef = monitor(process, Client),
                    {ok, Client, MRef};
                {error, Reason} ->
                    {error, {connection_failed, Reason}}
            end;
        {error, Reason = {options, _}} ->
            {error, {invalid_client_options, Reason, ClientOpts}}
    end.

-spec kill_requests([request_id()] | all, Reason :: term(), state()) -> state().
kill_requests(all, Reason, St0 = #{requests := Requests}) ->
    kill_requests(maps:keys(Requests), Reason, St0);
kill_requests([], _Reason, St0) ->
    St0;
kill_requests([RequestID | Rest], Reason, St0) ->
    ok = kill_request(get_request(RequestID, St0), Reason),
    kill_requests(Rest, Reason, remove_request(RequestID, St0)).

-spec kill_request(request_state(), Reason :: term()) -> ok.
kill_request(#{from := From}, Reason) ->
    gen_server:reply(From, {error, {request_killed, Reason}}).

%%
%% Utility
%%

-spec create_request_state(request_owner()) -> request_state().
create_request_state(From) ->
    #{
        from => From,
        response => create_response()
    }.

-spec create_response() -> response_data().
create_response() ->
    #{
        headers => [],
        status_code => unknown
    }.

-spec add_request(request_id(), request_owner(), state()) -> state().
add_request(RequestId, From, St0) ->
    update_request(RequestId, create_request_state(From), St0).

-spec get_request(request_id(), state()) -> request_state().
get_request(RequestId, #{requests := Requests}) ->
    maps:get(RequestId, Requests).

-spec update_request(request_id(), request_state(), state()) -> state().
update_request(RequestId, RequestState, #{requests := Requests0} = St0) ->
    Requests1 = Requests0#{RequestId => RequestState},
    St0#{requests => Requests1}.

-spec remove_request(request_id(), state()) -> state().
remove_request(RequestId, #{requests := Requests0} = St0) ->
    Requests1 = maps:remove(RequestId, Requests0),
    St0#{requests => Requests1}.

%%

-spec set_response_status(status_code(), request_state()) -> request_state().
set_response_status(StatusCode, #{response := ResponseData0} = Request0) ->
    ResponseData1 = ResponseData0#{status_code => StatusCode},
    Request0#{response => ResponseData1}.

-spec set_response_headers(resp_headers(), request_state()) -> request_state().
set_response_headers(Headers, #{response := ResponseData0} = Request0) ->
    ResponseData1 = ResponseData0#{headers => Headers},
    Request0#{response => ResponseData1}.

-spec get_response_body(request_state()) -> body() | undefined.
get_response_body(#{response := ResponseData0}) ->
    maps:get(body, ResponseData0, undefined).

-spec set_response_body(body(), request_state()) -> request_state().
set_response_body(Body, #{response := ResponseData0} = Request0) ->
    ResponseData1 = ResponseData0#{body => Body},
    Request0#{response => ResponseData1}.

-spec append_response_data(body() | undefined, body()) -> body().
append_response_data(undefined, NewData) ->
    NewData;
append_response_data(Data0, NewData) ->
    <<Data0/binary, NewData/binary>>.
