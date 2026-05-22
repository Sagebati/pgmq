//! SQL query constants loaded from `src/sql/*.sql` at compile time via `include_str!`.
//! Single source of truth — every backend adapter references these same strings.

pub const CREATE: &str = include_str!("../sql/create.sql");
pub const CREATE_UNLOGGED: &str = include_str!("../sql/create_unlogged.sql");
pub const CREATE_PARTITIONED: &str = include_str!("../sql/create_partitioned.sql");
pub const CREATE_PARTITIONED_EXISTS_CHECK: &str =
    include_str!("../sql/create_partitioned_exists_check.sql");
pub const DROP_QUEUE: &str = include_str!("../sql/drop_queue.sql");
pub const PURGE_QUEUE: &str = include_str!("../sql/purge_queue.sql");
pub const LIST_QUEUES: &str = include_str!("../sql/list_queues.sql");
pub const SET_VT: &str = include_str!("../sql/set_vt.sql");

pub const SEND: &str = include_str!("../sql/send.sql");
pub const SEND_BATCH: &str = include_str!("../sql/send_batch.sql");

pub const READ: &str = include_str!("../sql/read.sql");
pub const READ_WITH_POLL: &str = include_str!("../sql/read_with_poll.sql");
pub const READ_GROUPED: &str = include_str!("../sql/read_grouped.sql");
pub const READ_GROUPED_WITH_POLL: &str = include_str!("../sql/read_grouped_with_poll.sql");
pub const READ_GROUPED_HEAD: &str = include_str!("../sql/read_grouped_head.sql");
pub const READ_GROUPED_RR: &str = include_str!("../sql/read_grouped_rr.sql");
pub const READ_GROUPED_RR_WITH_POLL: &str = include_str!("../sql/read_grouped_rr_with_poll.sql");

pub const POP: &str = include_str!("../sql/pop.sql");

pub const ARCHIVE: &str = include_str!("../sql/archive.sql");
pub const ARCHIVE_BATCH: &str = include_str!("../sql/archive_batch.sql");
pub const DELETE: &str = include_str!("../sql/delete.sql");
pub const DELETE_BATCH: &str = include_str!("../sql/delete_batch.sql");

pub const CREATE_FIFO_INDEX: &str = include_str!("../sql/create_fifo_index.sql");
pub const CREATE_FIFO_INDEXES_ALL: &str = include_str!("../sql/create_fifo_indexes_all.sql");

pub const BIND_TOPIC: &str = include_str!("../sql/bind_topic.sql");
pub const UNBIND_TOPIC: &str = include_str!("../sql/unbind_topic.sql");
pub const LIST_TOPIC_BINDINGS: &str = include_str!("../sql/list_topic_bindings.sql");
pub const LIST_TOPIC_BINDINGS_ALL: &str = include_str!("../sql/list_topic_bindings_all.sql");
pub const SEND_TOPIC: &str = include_str!("../sql/send_topic.sql");
pub const SEND_BATCH_TOPIC: &str = include_str!("../sql/send_batch_topic.sql");

pub const ENABLE_NOTIFY_INSERT: &str = include_str!("../sql/enable_notify_insert.sql");
pub const DISABLE_NOTIFY_INSERT: &str = include_str!("../sql/disable_notify_insert.sql");
pub const UPDATE_NOTIFY_INSERT: &str = include_str!("../sql/update_notify_insert.sql");
pub const LIST_NOTIFY_INSERT_THROTTLES: &str =
    include_str!("../sql/list_notify_insert_throttles.sql");

pub const METRICS: &str = include_str!("../sql/metrics.sql");
pub const METRICS_ALL: &str = include_str!("../sql/metrics_all.sql");

/// Dynamically-built SQL for `convert_archive_partitioned` — params optional, so we can't keep
/// this as a static string. Returns the final SQL plus an ordered list of optional params to bind.
pub fn convert_archive_partitioned_sql(
    has_partition_interval: bool,
    has_retention_interval: bool,
) -> String {
    use std::fmt::Write;
    let mut sql = String::from("SELECT pgmq.convert_archive_partitioned(table_name=>$1::text");
    let mut idx = 2;
    if has_partition_interval {
        // `write!` to a String never fails — unwrap is safe.
        write!(sql, ", partition_interval=>${idx}::text").unwrap();
        idx += 1;
    }
    if has_retention_interval {
        write!(sql, ", retention_interval=>${idx}::text").unwrap();
    }
    sql.push_str(");");
    sql
}
