SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.set_vt(queue_name=>$1::text, msg_id=>$2::bigint, vt=>$3::integer);
