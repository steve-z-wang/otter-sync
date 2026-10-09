import test from 'node:test';
import assert from 'node:assert/strict';
import {AxtonReport} from './bindings/connection.mts';
test('format5 report preserves record diagnostics without a stamp',()=>{
 const report=new AxtonReport({kind:'diverged',model:'Entry',identity:{id:'e'},ordinal:7,code:'local.diverged',detail:{reason:'refused'}});
 assert.equal(report.kind,'diverged');assert.equal(report.ordinal,7);assert.deepEqual(report.identity,{id:'e'});assert.equal(report.code,'local.diverged');assert.deepEqual(report.detail,{reason:'refused'});assert.equal('stamp' in report,false);assert.equal(report.message,'diverged: Entry {"id":"e"} (local.diverged) (mutation 7)');
});
