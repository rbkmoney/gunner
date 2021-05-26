-define(EV_MATCH(Match), fun(Event) ->
    case Event of
        Match -> true;
        _ -> false
    end
end).

-define(pool_init(PoolOpts), #gunner_pool_init_event{
    pool_opts = PoolOpts
}).

-define(pool_terminate(Reason), #gunner_pool_terminate_event{
    reason = Reason
}).

-define(acquire_started(GroupId, Client, Locking), #gunner_acquire_started_event{
    group_id = GroupId,
    client = Client,
    is_locking = Locking
}).

-define(acquire_finished_ok(GroupId, Client, Connection), #gunner_acquire_finished_event{
    group_id = GroupId,
    client = Client,
    result = ok,
    connection = Connection
}).

-define(acquire_finished_error(Client, Error), #gunner_acquire_finished_event{
    client = Client,
    result = {error, Error}
}).

-define(connection_locked(GroupId, Client, Connection), #gunner_connection_locked_event{
    group_id = GroupId,
    client = Client,
    connection = Connection
}).

-define(connection_unlocked(GroupId, Client, Connection), #gunner_connection_unlocked_event{
    group_id = GroupId,
    client = Client,
    connection = Connection
}).

-define(free_started(GroupId, Client, Connection), #gunner_free_started_event{
    group_id = GroupId,
    client = Client,
    connection = Connection
}).

-define(free_finished(GroupId, Client, Connection, Result), #gunner_free_finished_event{
    group_id = GroupId,
    client = Client,
    connection = Connection,
    result = Result
}).

-define(cleanup_started(ActiveConnections), #gunner_cleanup_started_event{
    active_connections = ActiveConnections
}).

-define(cleanup_finished(ActiveConnections), #gunner_cleanup_finished_event{
    active_connections = ActiveConnections
}).

-define(client_down(Client, Reason), #gunner_client_down_event{
    client = Client,
    reason = Reason
}).

-define(connection_init_started(GroupId, Connection), #gunner_connection_init_started_event{
    group_id = GroupId,
    connection = Connection
}).

-define(connection_init_finished(GroupId, Connection, Result), #gunner_connection_init_finished_event{
    group_id = GroupId,
    connection = Connection,
    result = Result
}).

-define(connection_down(GroupId, Connection, Reason), #gunner_connection_down_event{
    group_id = GroupId,
    connection = Connection,
    reason = Reason
}).
