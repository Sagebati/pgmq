SELECT * FROM pgmq.delete(queue_name=>$1::text, msg_ids=>$2::bigint[]) AS t(was_deleted);
