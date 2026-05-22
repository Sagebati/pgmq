SELECT pgmq.update_notify_insert(queue_name=>$1::text, throttle_interval_ms=>$2::integer);
