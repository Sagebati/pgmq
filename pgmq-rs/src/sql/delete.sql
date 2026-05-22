SELECT * from pgmq.delete(queue_name=>$1::text, msg_id=>$2::bigint) AS delete;
