-record(gunner_pool_init_event, {
    pool_opts :: gunner:pool_opts()
}).

-record(gunner_pool_terminate_event, {
    reason :: term()
}).

-record(gunner_acquire_started_event, {
    group_id :: gunner_pool:group_id(),
    client :: pid(),
    is_locking :: boolean()
}).

-record(gunner_acquire_finished_event, {
    group_id :: gunner_pool:group_id() | undefined,
    client :: pid() | undefined,
    result :: ok | {error, pool_unavailable | {connection_failed, Reason :: _}},
    connection :: gunner_pool:connection_pid() | undefined
}).

-record(gunner_connection_locked_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    client :: pid()
}).

-record(gunner_connection_unlocked_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    client :: pid()
}).

-record(gunner_free_started_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    client :: pid()
}).

-record(gunner_free_finished_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    client :: pid(),
    result :: ok | {error, not_locked}
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

-record(gunner_connection_init_started_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id()
}).

-record(gunner_connection_init_finished_event, {
    result :: ok | {error, Reason :: _},
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id()
}).

-record(gunner_connection_down_event, {
    connection :: gunner_pool:connection_pid(),
    group_id :: gunner_pool:group_id(),
    reason :: term()
}).
