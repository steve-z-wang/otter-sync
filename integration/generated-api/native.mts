import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {GeneratedClient} from './client.ts';
const directory=await mkdtemp(join(tmpdir(),'generated-native-'));
const client=await GeneratedClient.open({path:join(directory,'state.sqlite')});
try {
 const id='123e4567-e89b-42d3-a456-426614174000';
 await client.mutate.createEntry({entry:{id,title:'native',note:'before',at:new Date('2026-01-01T00:00:00Z'),tags:[],status:'active'}});
 await client.mutate.editEntry({entry:{identity:{id},values:{note:null}}});
 const afterIndependentMutations=await client.models.entry.get({id});
 if(afterIndependentMutations?.note!==null)throw Error('independent mutations apply locally in order');
 const row=await client.models.entry.get({id});
 if(row?.note!==null||row.title!=='native'||!(row.at instanceof Date))throw Error('native roundtrip');
 if((await client.models.entry.query()).length!==1)throw Error('native query');
 const filtered=await client.models.entry.query({where:{at:new Date('2026-01-01T01:00:00+01:00'),note:null},orderBy:[{field:'title',direction:'descending'}],limit:1});
 if(filtered.length!==1)throw Error('typed query normalization');
 await client.mutate.addBook({book:{id:'b',title:'Book'}});
 await client.mutate.addComment({comment:{id:'c',bookId:'b',text:'Comment'}});
 if((await client.models.comment.book({id:'c'}))?.id!=='b')throw Error('forward relation');
 const ordinal=await client.mutate.editEntry({entry:{identity:{id},values:{note:'outside'}}});
 const state=await client.models.entry.syncState({id});
 if(!state.pending.some(p=>p.ordinal===ordinal&&p.name==='EditEntry'&&p.phase==='queued'))throw Error('typed record sync state');
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
 await client.mutate.addComment({comment:{id:'c2',bookId:'b',text:'Second'}});
 await new Promise(r=>setTimeout(r,20));
 await client.transaction(tx=>tx.models.book.update({id:'b'},{title:'Renamed'}));
 await new Promise(r=>setTimeout(r,20));stopJoined();
 if(JSON.stringify(joined)!==JSON.stringify([['Book:Comment'],['Book:Comment','Book:Second'],['Renamed:Comment','Renamed:Second']]))throw Error(`watchSql ${JSON.stringify(joined)}`);
 // Creation defaults (#27): omitted fields of a fresh create are filled once by the native client.
 await client.transaction(async tx=>{await tx.models.draft.create({memo:null});await tx.models.draft.create({memo:'explicit',note:null,body:'mine'});});
 await client.mutate.addDraft({draft:{memo:'queued'}});
 const drafts=await client.models.draft.query();
 const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
 if(drafts.length!==3||new Set(drafts.map(d=>d.id)).size!==3||!drafts.every(d=>uuid.test(d.id)&&d.mood==='busy'&&d.created instanceof Date&&Math.abs(d.created.getTime()-Date.now())<60000))throw Error(`generated defaults ${JSON.stringify(drafts)}`);
 const defaulted=drafts.find(d=>d.memo===null);
 if(defaulted?.body!=='q \'single\' "double" \'\'\' """ $dollar ${x} \\ back\nline'||defaulted.note!=='n')throw Error(`literal defaults ${JSON.stringify(defaulted)}`);
 const explicit=drafts.find(d=>d.memo==='explicit');
 if(explicit?.body!=='mine'||explicit.note!==null)throw Error('explicit values and null win');
 if(await client.client.freeze()===null)throw Error('native freeze');
}finally{await client.close();await rm(directory,{recursive:true,force:true})}

// A failed generated connection setup must close the real native handle before rethrowing.
const {Client}=await import('../../packages/client-js/index.mts');
const {strict:assert}=await import('node:assert');
const originalOpen=Client.open;
let opened:Awaited<ReturnType<typeof Client.open>>|undefined;
const failedDirectory=await mkdtemp(join(tmpdir(),'generated-failed-open-'));
Client.open=async options=>(opened=await originalOpen.call(Client,options));
try{
 await assert.rejects(GeneratedClient.open({path:join(failedDirectory,'state.sqlite'),server:{url:'http://[',token:'secret'}}),/Invalid URL/);
 assert.ok(opened);
 await assert.rejects(opened.syncState(),/client_closed/);
}finally{Client.open=originalOpen;await opened?.close();await rm(failedDirectory,{recursive:true,force:true});}
