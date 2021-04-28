-record(gunner_init_event, {
    pool_opts :: gunner:pool_opts()
}).

-record(gunner_acquire_started_event, {
    group_id :: gunner_pool:group_id(),
    client :: pid(),
    is_locking :: boolean()
}).

%% Not sure if this needs to be an event
%% Theoretically we can determine if acquiring made a new connection by the amount of time it took
%% But on the other hand this is more explicit
-record(gunner_pool_starvation_event, {
    group_id :: gunner_pool:group_id()
}).

-record(gunner_acquire_finished_event, {
    result :: success | {fail, pool_unavailable | {failed_to_start_connection, Reason :: _}},
    connection :: gunner_pool:connection_pid() | undefined,
    group_id :: gunner_pool:group_id()
}).

-record(gunner_free_started_event, {
    connection :: gunner_pool:connection_pid(),
    client :: pid()
}).

-record(gunner_free_finished_event, {
    connection :: gunner_pool:connection_pid(),
    result :: success | not_locked
}).

-record(gunner_cleanup_started_event, {
    active_connections :: integer()
}).

-record(gunner_cleanup_finished_event, {
    active_connections :: integer()
}).

-record(gunner_client_down_event, {
    client :: pid(),
    reason :: any()
}).

-record(gunner_connection_up_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id()
}).

-record(gunner_connection_down_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    reason :: any()
}).

-record(gunner_terminate_event, {
    reason :: normal | shutdown | {shutdown, any()} | any()
}).
