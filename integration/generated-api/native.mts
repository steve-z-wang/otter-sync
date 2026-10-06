import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {GeneratedClient} from './client.ts';
const directory=await mkdtemp(join(tmpdir(),'generated-native-'));
const connection={url:'http://127.0.0.1:1',token:'offline',identity:{backend:'generated',viewer:'viewer',contract:'generated-v04'}};
const client=await GeneratedClient.open({path:join(directory,'state.sqlite'),stream:'User:viewer',connection});
await client.client.connection?.close();
try {
 const id='123e4567-e89b-42d3-a456-426614174000';
 await client.models.entry.create({id,title:'native',note:'before',at:new Date('2026-01-01T00:00:00Z'),tags:[],status:'active'});
 await client.models.entry.update({id},{note:null});
 const afterIndependentMutations=await client.models.entry.get({id});
 if(afterIndependentMutations?.note!==null)throw Error('independent mutations apply locally in order');
 const row=await client.models.entry.get({id});
 if(row?.note!==null||row.title!=='native'||!(row.at instanceof Date))throw Error('native roundtrip');
 if((await client.models.entry.query()).length!==1)throw Error('native query');
 const filtered=await client.models.entry.query({where:{at:new Date('2026-01-01T01:00:00+01:00'),note:null},orderBy:[{field:'title',direction:'descending'}],limit:1});
 if(filtered.length!==1)throw Error('typed query normalization');
 await client.models.book.create({id:'b',title:'Book'});
 await client.models.comment.create({id:'c',bookId:'b',text:'Comment'});
 if((await client.models.comment.book({id:'c'}))?.id!=='b')throw Error('forward relation');
 const pendingId='123e4567-e89b-42d3-a456-426614174002';
 const call=await client.mutations.publishEntry({entry:{id:pendingId,title:'pending',note:null,at:new Date(),tags:[],status:'active'},composition:id});
 const state=await client.models.entry.syncState({id:pendingId});
 if(!state.pending.some(p=>p.name==='PublishEntry'&&p.phase==='queued'))throw Error('typed record sync state');
 if((await client.syncState()).pending<1||client.clientId==='')throw Error('client sync state');
 if((await client.models.book.comments({id:'b'})).length!==1)throw Error('inverse relation');
 await client.transaction(async tx=>{await tx.models.book.create({id:'local',title:'Local only'});await tx.models.book.update({id:'local'},{title:'Local edited'});});
 if((await client.models.book.get({id:'local'}))?.title!=='Local edited')throw Error('local write');
 const seen:number[]=[];const stop=client.models.book.watch({},rows=>seen.push(rows.length));
 await client.transaction(tx=>tx.models.book.delete({id:'local'}));
 await new Promise(r=>setTimeout(r,20));stop();
 if(seen[0]!==2||seen[seen.length-1]!==1)throw Error(`watch ${seen}`);
 // Read-only SQL over several Models through the generated client (#184):
 // a commit to either joined Model re-emits the join.
 const joined:unknown[]=[];const stopJoined=client.watchSql('SELECT b.title AS title, c.text AS text FROM Book b JOIN Comment c ON c.bookId = b.id ORDER BY c.id',[],rows=>joined.push(rows.map(r=>`${r.title}:${r.text}`)));
 await new Promise(r=>setTimeout(r,20));
 await client.models.comment.create({id:'c2',bookId:'b',text:'Second'});
 await new Promise(r=>setTimeout(r,20));
 await client.transaction(tx=>tx.models.book.update({id:'b'},{title:'Renamed'}));
 await new Promise(r=>setTimeout(r,20));stopJoined();
 if(JSON.stringify(joined)!==JSON.stringify([['Book:Comment'],['Book:Comment','Book:Second'],['Renamed:Comment','Renamed:Second']]))throw Error(`watchSql ${JSON.stringify(joined)}`);
 // Creation defaults (#27): omitted fields of a fresh create are filled once by the native client.
 await client.transaction(async tx=>{await tx.models.draft.create({memo:null});await tx.models.draft.create({memo:'explicit',note:null,body:'mine'});});
 await client.models.draft.create({memo:'third'});
 const drafts=await client.models.draft.query();
 const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
 if(drafts.length!==3||new Set(drafts.map(d=>d.id)).size!==3||!drafts.every(d=>uuid.test(d.id)&&d.mood==='busy'&&d.created instanceof Date&&Math.abs(d.created.getTime()-Date.now())<60000))throw Error(`generated defaults ${JSON.stringify(drafts)}`);
 const defaulted=drafts.find(d=>d.memo===null);
 if(defaulted?.body!=='q \'single\' "double" \'\'\' """ $dollar ${x} \\ back\nline'||defaulted.note!=='n')throw Error(`literal defaults ${JSON.stringify(defaulted)}`);
 const explicit=drafts.find(d=>d.memo==='explicit');
 if(explicit?.body!=='mine'||explicit.note!==null)throw Error('explicit values and null win');
 if(call.status!=='pending')throw Error('named Call remains pending without backend settlement');
}finally{await client.close();await rm(directory,{recursive:true,force:true})}

// Failed setup must not retain physical ownership of the requested Store.
const {strict:assert}=await import('node:assert');
const failedDirectory=await mkdtemp(join(tmpdir(),'generated-failed-open-'));
let reopened:GeneratedClient|undefined;
try{
 const path=join(failedDirectory,'state.sqlite');
 await assert.rejects(GeneratedClient.open({path,stream:'User:viewer',connection:{...connection,url:'http://['}}),/Invalid URL/);
 reopened=await GeneratedClient.open({path,stream:'User:viewer',connection});
 assert.equal((await reopened.syncState()).pending,0);
}finally{await reopened?.close();await rm(failedDirectory,{recursive:true,force:true});}
