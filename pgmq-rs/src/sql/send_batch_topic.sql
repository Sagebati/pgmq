SELECT queue_name, msg_id from pgmq.send_batch_topic(routing_key=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer);
