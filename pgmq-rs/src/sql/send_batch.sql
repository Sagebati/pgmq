SELECT * from pgmq.send_batch(queue_name=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer) AS send_batch;
