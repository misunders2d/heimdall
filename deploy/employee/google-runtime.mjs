// Trusted access-only runner. No OAuth client, refresh, enrollment or arbitrary
// action/path entry point is exposed to the model.
import { readFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { registerGoogleTools } from './google-tools.mjs';

export const googleActions = Object.freeze(['drive_list','drive_metadata','drive_download','drive_upload_text','drive_upload_file','drive_update_file','drive_create_folder','sheets_list_tabs','sheets_read','sheets_append','sheets_write','sheets_create_tab','calendar_list','calendar_events','calendar_create_event','calendar_update_event','gmail_search','gmail_read','gmail_create_draft']);
const names = new Set(googleActions.map(a => 'google_' + a));
const writes = new Set(['drive_upload_text','drive_upload_file','drive_update_file','drive_create_folder','sheets_append','sheets_write','sheets_create_tab','calendar_create_event','calendar_update_event','gmail_create_draft']);
const login = 'Send `google login` in a DM.';
export function sharedBlocked(action, privateDM, platform) {
  return !privateDM && /^(gmail_|calendar_)/.test(action) && !(platform === 'slack' && action === 'gmail_create_draft');
}
function result(data, sanitize) {
  // Details carry only the same bounded sanitized content. No raw external
  // body is stashed alongside the safe presentation.
  const text = sanitize(JSON.stringify(data, null, 2), 'employee Google').text;
  return { content: [{type:'text', text: text.slice(0, 50000)}], details: {status:data.status} };
}
export function installGoogleTools(pi, Type, sanitize, env = process.env, execute = runHelper) {
  const call = async (action, args, signal) => {
    if (!googleActions.includes(action)) throw new Error('Google action denied');
    let access;
    try { access = JSON.parse(readFileSync(env.HEIMDALL_GOOGLE_ACCESS_FILE, 'utf8')); } catch {}
    if (!access?.access_token || !Number.isFinite(Date.parse(access.expires)) || Date.parse(access.expires) <= Date.now()) return {content:[{type:'text',text:login}],details:{status:'auth_needed'}};
    if (sharedBlocked(action, env.PI_EXTERNAL_MCP_PRIVATE === '1', env.PI_EXTERNAL_PLATFORM)) return result({status:'blocked',message:'Ask in DM for Gmail/Calendar access. Shared Slack permits Gmail draft creation only.'}, sanitize);
    if (writes.has(action) && args.confirm_write !== true) return result({status:'blocked',message:"confirm_write=true requires the caller's explicit request for this write."},sanitize);
    const data = await execute(action,args,signal,env);
    if (env.PI_EXTERNAL_MCP_PRIVATE !== '1' && action === 'gmail_create_draft') return result({status:data.status,sent:false,message:data.status === 'success' ? 'Draft created. No email sent.' : 'Draft creation failed. Ask in DM to check access.'},sanitize);
    return result(data,sanitize);
  };
  registerGoogleTools(pi,Type,call);
  return event => names.has(event.toolName);
}
function runHelper(action,args,signal,env) {
  // Do not inherit credentials or a caller-selected Python configuration.
  const childEnv = Object.fromEntries(['HOME','PATH','LANG','TZ','HEIMDALL_TURN_DIR','HEIMDALL_GOOGLE_ACCESS_FILE','PI_EXTERNAL_MCP_PRIVATE','PI_EXTERNAL_PLATFORM'].filter(k => env[k]).map(k => [k,env[k]]));
  childEnv.PYTHONDONTWRITEBYTECODE='1';
  return new Promise(resolve => {
    const child = spawn('/opt/assistant/runtime/google/bin/python', ['-I','-B','/opt/assistant/heimdall/google-workspace.py',action], {stdio:['pipe','pipe','pipe'],env:childEnv,signal,timeout:90000});
    let output='',size=0;
    child.stdout.on('data',chunk=> {size+=chunk.length;if(size>20*1024*1024) child.kill();else output+=chunk;});
    child.stderr.resume(); // Provider exceptions/URLs never become model output.
    let settled=false;
    const finish=data=>{if(!settled){settled=true;resolve(data);}};
    child.on('error',()=>finish({status:'error',message:'Google helper unavailable.'}));
    child.on('close',()=>{try{finish(JSON.parse(output));}catch{finish({status:'error',message:'Google helper failed.'});}});
    child.stdin.on('error',()=>{});
    child.stdin.end(JSON.stringify(args));
  });
}
