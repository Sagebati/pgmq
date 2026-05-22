SELECT pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings(queue_name=>$1::text);
