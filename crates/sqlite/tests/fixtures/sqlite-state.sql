-- Execute AFTER exact legacy FRAMEWORK_DDL. Test owner inserts descriptor
-- through its normal schema helper before this data, or supplies matching schema.json.
CREATE TABLE Todo(id TEXT PRIMARY KEY,title TEXT NOT NULL,channel TEXT NOT NULL);
CREATE TABLE axton_before_Todo(id TEXT PRIMARY KEY,title TEXT NOT NULL,channel TEXT NOT NULL);
INSERT INTO Todo VALUES('live','saved snapshot','second queued Channel');
INSERT INTO axton_before_Todo VALUES('live','saved snapshot','Channel:literal/channel/unchanged');
INSERT INTO axton_client(client_id,next_ordinal,next_push,generation,last_completed_push,push_models,push_results,channel_membership_version,store_epoch,next_subscription)
VALUES('fixture-client',3,2,9,0,'{"Todo":1}','[]',1,4,4);
INSERT INTO axton_record(model,identity,stamp,base_state,evicted_at) VALUES ('Todo','{"id":"live"}',7,'materialized',0),('Todo','{"id":"gone"}',5,'evicted',4),('Todo','{"id":"tombstone"}',8,'absent',0);
INSERT INTO axton_channel_member VALUES('Channel:business-scope','Todo','{"id":"live"}',11,0),('Other','Todo','{"id":"live"}',3,1),('Channel:business-scope','Todo','{"id":"gone"}',9,0),('Channel:business-scope','Todo','{"id":"tombstone"}',8,1);
INSERT INTO axton_subscription(channel,subscription_id,starting_cursor,cursor,bootstrap_state,bootstrap_run,bootstrap_cursor,bootstrap_barrier,reconcile_state,reconcile_run,reconcile_cursor,reconcile_bound,reconcile_barrier)
VALUES('Channel:business-scope',1,5,11,'requested',2,4,11,'requested',3,6,11,11),('Other',2,0,3,'complete',1,3,3,'complete',1,3,3,3),('Uninitialized',3,NULL,NULL,'not_requested',0,0,NULL,'not_requested',0,0,NULL,NULL);
INSERT INTO axton_mutation(ordinal,name,version,push,store_epoch,call_id,args) VALUES(1,'Edit',1,1,4,'01890f47-1234-7123-8123-000000000001','{"todo":{"channel":"queued Channel","id":"live"}}');
INSERT INTO axton_mutation_operation VALUES(1,0,'wire','Todo','{"id":"live"}','update','{"channel":"queued Channel"}');
INSERT INTO axton_mutation(ordinal,name,version,push,store_epoch,call_id,args) VALUES(2,'Edit',1,NULL,4,'01890f47-1234-7123-8123-000000000004','{"todo":{"channel":"second queued Channel","id":"live"}}');
INSERT INTO axton_mutation_operation VALUES(2,0,'wire','Todo','{"id":"live"}','update','{"channel":"second queued Channel"}');
INSERT INTO axton_mutation_dependency VALUES(2,1,'sequence');
INSERT INTO axton_load(store_epoch,load_id,seq,ready,name,version,args,models,continuation,run,phase,pages,call_id,intent,retry,attempts) VALUES(4,'01890f47-1234-7123-8123-000000000002',1,1,'Scan',1,'{"channel":"Channel:literal/channel/unchanged"}','{"Todo":1}','{"state":{"channel":"opaque continuation","memberships":[{"channel":"opaque nested"}]}}',2,'pending',1,'01890f47-1234-7123-8123-000000000003','{"args":{"channel":"Channel:literal/channel/unchanged"},"callId":"01890f47-1234-7123-8123-000000000003","continuation":{"state":{"channel":"opaque continuation","memberships":[{"channel":"opaque nested"}]}},"loadId":"01890f47-1234-7123-8123-000000000002","models":{"Todo":1},"name":"Scan","version":1}','transport',2);
INSERT INTO axton_load_once VALUES('opaque Channel once key','Scan',1,'{"channel":"Channel:literal/channel/unchanged"}','{"Todo":1}','01890f47-1234-7123-8123-000000000002');
