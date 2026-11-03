import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';
import { googleActions, installGoogleTools, sharedBlocked } from './google-runtime.mjs';
const require = createRequire('/opt/assistant/runtime/pi/package.json');
const { Type } = require('typebox');
const sanitize = text => ({text});

function fixture(t, {privateDM=true,platform='slack',signedIn=true}={}) {
 const dir=mkdtempSync(join(tmpdir(),'employee-google-test-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
 const file=join(dir,'google-access.json');
 writeFileSync(file,JSON.stringify(signedIn ? {email:'employee@example.test',access_token:'SYNTHETIC_ACCESS',expires:new Date(Date.now()+3600000).toISOString(),scopes:[]} : {}),{mode:0o600});
 const tools=new Map(),calls=[];
 const authorized=installGoogleTools({registerTool:tool=>tools.set(tool.name,tool)},Type,sanitize,{HEIMDALL_GOOGLE_ACCESS_FILE:file,HEIMDALL_TURN_DIR:dir,PI_EXTERNAL_MCP_PRIVATE:privateDM?'1':'0',PI_EXTERNAL_PLATFORM:platform},async(...args)=>{calls.push(args);return {status:'success',draft:{id:'PRIVATE_DRAFT',body:'PRIVATE_BODY'},sent:false};});
 return {tools,calls,authorized};
}

test('registers only the 19 allowed Google tools, with explicit write assertions',t=>{
 const {tools,authorized}=fixture(t);
 assert.equal(tools.size,19);
 for(const action of googleActions)assert(tools.has('google_'+action));
 for(const name of ['google_connect','google_disconnect','google_gmail_send','google_calendar_delete_event','bash','mcp__owner__read'])assert(!authorized({toolName:name}));
 for(const [name,tool]of tools) {
  assert(authorized({toolName:name}));
  assert(!tool.parameters.properties?.subject || name==='google_gmail_create_draft');
 }
 for(const name of ['google_drive_upload_text','google_drive_upload_file','google_drive_update_file','google_drive_create_folder','google_sheets_append','google_sheets_write','google_sheets_create_tab','google_calendar_create_event','google_calendar_update_event','google_gmail_create_draft'])assert(tools.get(name).parameters.required.includes('confirm_write'));
});

test('all disconnected tools return one deterministic login line before any helper call',async t=>{
 const {tools,calls}=fixture(t,{signedIn:false,privateDM:false});
 for(const tool of tools.values()) {
  const data=await tool.execute('id',{},undefined);
  assert.equal(data.content[0].text,'Send `google login` in a DM.');
 }
 assert.equal(calls.length,0);
});

test('shared-channel guard covers both platforms and all Calendar mutations',async t=>{
 for(const platform of ['slack','telegram']) {
  const {tools,calls}=fixture(t,{privateDM:false,platform});
  for(const action of googleActions.filter(a=>/^(calendar_|gmail_)/.test(a))) {
   const allowed=platform==='slack'&&action==='gmail_create_draft';
   const data=await tools.get('google_'+action).execute('id',{confirm_write:true,subject:'subject'},undefined);
   assert.equal(data.details.status,allowed?'success':'blocked');
   assert.equal(sharedBlocked(action,false,platform),!allowed);
   assert(!JSON.stringify(data).includes('PRIVATE_'));
  }
  assert.equal(calls.length,platform==='slack'?1:0);
 }
});

test('private Calendar update and Gmail draft dispatch under the fixed caller',async t=>{
 const {tools,calls}=fixture(t);
 await tools.get('google_calendar_update_event').execute('id',{confirm_write:true,calendar_id:'shared',event_id:'event',title:'Updated'},undefined);
 await tools.get('google_gmail_create_draft').execute('id',{confirm_write:true,subject:'Subject',to:'to@example.test',body:'body'},undefined);
 assert.equal(calls[0][0],'calendar_update_event');assert.equal(calls[0][1].calendar_id,'shared');
 assert.equal(calls[1][1].email_subject,'Subject');assert(!('subject' in calls[1][1]));
});

test('writes with no explicit-request assertion never reach Google',async t=>{
 const {tools,calls}=fixture(t);
 const data=await tools.get('google_calendar_create_event').execute('id',{title:'Unrequested'},undefined);
 assert.equal(data.details.status,'blocked');assert.equal(calls.length,0);
});
