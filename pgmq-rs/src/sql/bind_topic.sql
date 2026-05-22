SELECT pgmq.bind_topic(pattern=>$1::text, queue_name=>$2::text);
