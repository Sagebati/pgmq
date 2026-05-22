SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length FROM pgmq.metrics(queue_name=>$1::text);
