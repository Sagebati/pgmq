SELECT * from pgmq.archive(queue_name=>$1::text, msg_id=>$2::bigint) AS archive;
