SELECT pgmq.delete(queue_name=>$1::text, msg_id=>$2::bigint) AS was_deleted;
