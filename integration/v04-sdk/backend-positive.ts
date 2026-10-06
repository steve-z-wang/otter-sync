import type {Options,MutationContext,QueryContext,TransactionCall,LoaderHooks} from './backend.ts';
const query=(ctx:QueryContext<object>)=>{
  ctx.stream.track.entry('e');
  ctx.streams(['User:a','User:b']).track.entry('e');
  // @ts-expect-error Query is track only
  ctx.stream.invalidate.entry('e');
  // @ts-expect-error current Stream is a handle, not a selector
  ctx.stream('User:b');
};
const mutation=(ctx:MutationContext<object>)=>{ctx.stream.track.entry('e');ctx.stream.invalidate.entry('e');ctx.streams(['User:b']).invalidate.entry('e');ctx.invalidate.entry('e');};
const background=(ctx:TransactionCall<object>)=>{
  ctx.streams(['User:a']).track.entry('e');
  // @ts-expect-error background has no implicit Stream
  ctx.stream.track.entry('e');
};
const hooks:LoaderHooks<object>={entry:{async prepareForViewer(call){call.streams(['User:a']).track.entry(call.ids);call.invalidate.entry(call.ids);}}};
const bootstrap:NonNullable<Options<object>['bootstrap']>=async({ctx})=>{ctx.stream.track.entry('e');};
