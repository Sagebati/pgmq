SELECT * from pgmq.archive(queue_name=>$1::text, msg_ids=>$2::bigint[]) AS archive;
