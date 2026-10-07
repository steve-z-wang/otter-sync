import test from 'node:test';
import assert from 'node:assert/strict';
import {AxtonReport} from './connection.mts';
test('format5 report preserves record diagnostics without a stamp',()=>{
 const report=new AxtonReport({kind:'conflict',model:'Entry',identity:{id:'e'},code:'loader.failed',detail:{reason:'refused'}});
 assert.equal(report.kind,'conflict');assert.deepEqual(report.identity,{id:'e'});assert.equal(report.code,'loader.failed');assert.deepEqual(report.detail,{reason:'refused'});assert.equal('stamp' in report,false);assert.equal(report.message,'conflict: Entry {"id":"e"} (loader.failed)');
});
