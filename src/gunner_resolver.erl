-module(gunner_resolver).

-include_lib("kernel/include/inet.hrl").

%%

-type resolve_error() :: einval | nxdomain | timeout.

-type options() :: #{
    ip_picker => ip_picker(),
    timeout => timeout()
}.

-type ip_picker() :: {module(), atom()} | predefined_ip_picker().
-type predefined_ip_picker() ::
    random |
    first.

-export_type([options/0]).
-export_type([ip_picker/0]).
-export_type([predefined_ip_picker/0]).
-export_type([resolve_error/0]).

-export([resolve_endpoint/1]).
-export([resolve_endpoint/2]).

%%

-type endpoint() :: gunner:endpoint().

%%

-define(DEFAULT_RESOLVE_TIMEOUT, infinity).
-define(DEFAULT_IP_PICKER, first).

%%

-spec resolve_endpoint(endpoint()) ->
    {ok, endpoint()} |
    {error, resolve_error()}.
resolve_endpoint(Url) ->
    resolve_endpoint(Url, #{}).

-spec resolve_endpoint(endpoint(), options()) ->
    {ok, endpoint()} |
    {error, resolve_error()}.
resolve_endpoint(Endpoint = {Host, _}, Opts) ->
    case inet:parse_address(Host) of
        % url host is already an ip, move on
        {ok, _} ->
            {ok, Endpoint};
        {error, _} ->
            do_resolve_endpoint(Endpoint, Opts)
    end.

%%

do_resolve_endpoint({UnresolvedHost, Port}, Opts) ->
    case lookup_host(UnresolvedHost, Opts) of
        {ok, {ResolvedAddr, _}} ->
            {ok, {ResolvedAddr, Port}};
        {error, Reason} ->
            {error, Reason}
    end.

lookup_host(Host, Opts) ->
    Timeout = maps:get(timeout, Opts, ?DEFAULT_RESOLVE_TIMEOUT),
    IPFamilies = get_ip_family_preference(),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    lookup_host(Host, Opts, Deadline, IPFamilies).

lookup_host(Host, Opts, Deadline, [IPFamily | IPFamilies]) ->
    try
        Timeout = get_timeout_remaining(Deadline),
        case inet:gethostbyname(Host, IPFamily, Timeout) of
            {ok, HostEnt} ->
                {ok, parse_hostent(HostEnt, Opts)};
            {error, nxdomain} ->
                lookup_host(Host, Opts, Deadline, IPFamilies);
            {error, Reason} ->
                {error, Reason}
        end
    catch
        throw:deadline_reached ->
            {error, timeout}
    end;
lookup_host(_Host, _Opts, _Deadline, []) ->
    {error, nxdomain}.

parse_hostent(HostEnt, Opts) ->
    {get_ip(HostEnt, Opts), get_ip_family(HostEnt)}.

get_ip(HostEnt, Opts) ->
    Picker = maps:get(ip_picker, Opts, ?DEFAULT_IP_PICKER),
    apply_ip_picker(Picker, HostEnt#hostent.h_addr_list).

apply_ip_picker(first, [Head | _Tail]) ->
    Head;
apply_ip_picker(random, AddrList) ->
    lists:nth(rand:uniform(length(AddrList)), AddrList);
apply_ip_picker({M, F}, AddrList) ->
    erlang:apply(M, F, [AddrList]).

get_ip_family(HostEnt) ->
    HostEnt#hostent.h_addrtype.

-spec get_ip_family_preference() -> [inet:address_family()].
get_ip_family_preference() ->
    case inet_db:res_option(inet6) of
        true -> [inet6, inet];
        false -> [inet, inet6]
    end.

get_timeout_remaining(Deadline) ->
    case Deadline - erlang:monotonic_time(millisecond) of
        Timeout when Timeout > 0 ->
            Timeout;
        _ ->
            throw(deadline_reached)
    end.
