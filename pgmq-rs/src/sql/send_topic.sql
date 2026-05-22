SELECT * from pgmq.send_topic(routing_key=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int) AS send_topic;
