import {createHash} from 'node:crypto';
// Real bound native Stores for SDK fixtures. The connection is closed after
// open so transport tests explicitly choose their active network session.
export const offlineNetwork = () => ({ open() {}, async push() { throw Error('offline'); } });
export async function openStore(Client, options) {
  const client=await Client.open({ ...options, stream: options.stream ?? 'User:viewer',
    connection: options.connection ?? {url:'http://127.0.0.1:1',token:'offline',
      identity:{backend:'sdk-test',viewer:'viewer',contract:'v04'}} });
  if(!options.connection) await client.connection?.close();
  return client;
}

const canonical=value=>Array.isArray(value)?`[${value.map(canonical).join(',')}]`:value&&typeof value==='object'?`{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`:JSON.stringify(value);
export function emptyPull(text){const body=typeof text==='string'?JSON.parse(text):text;return JSON.stringify(body.kind==='start'?{context:body.context,manifestId:'fixed',start:0,total:0}:body.kind==='tail'?{context:body.context,manifestId:body.manifestId,head:0}:{context:body.context,pageId:'empty',from:body.after,to:body.after,head:body.after,units:[]});}
export function emptyRead(text){const body=typeof text==='string'?JSON.parse(text):text;return JSON.stringify({context:body.context,completion:{callId:body.callId,outcome:{status:'succeeded',result:null}},records:[]});}
export function emptyMutation(text){const body=typeof text==='string'?JSON.parse(text):text;return JSON.stringify({context:body.context,intentDigest:createHash('sha256').update('axton:protocol4:sha256:mutation-intent\0').update(canonical(body)).digest('hex'),completion:{callId:body.callId,outcome:{status:'succeeded',result:null}},targets:[]});}
