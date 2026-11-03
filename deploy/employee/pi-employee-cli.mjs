// Root-owned launcher: no owner wrapper, config, refresh token or extension discovery.
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
const root = '/opt/assistant';
const packageDir = root + '/runtime/pi';
if (process.getuid() === 0 || process.env.PI_RESTRICTED_EXTERNAL_USER !== '1' || process.env.MEMENTO_PROCESSING !== '1') throw new Error('Restricted employee context required');
const agentDir = process.env.PI_CODING_AGENT_DIR;
if (!agentDir?.startsWith(process.env.HOME + '/.pi/turn-agents/')) throw new Error('Employee agent route denied');
const require = createRequire(join(packageDir, 'package.json'));
const { createJiti } = require('jiti');
const { Type } = await import(pathToFileURL(require.resolve('typebox')).href);
const alias = Object.fromEntries(['pi-coding-agent','pi-ai','pi-tui'].map(name => ['@earendil-works/' + name, name === 'pi-coding-agent' ? join(packageDir,'dist/index.js') : join(packageDir,'node_modules/@earendil-works',name,'dist/index.js')]));
const jiti = createJiti(import.meta.url, { tryNative: false, alias });
process.env.PI_CODING_AGENT_DIST_DIR = join(packageDir, 'dist');
const { sanitizeUntrustedText } = await jiti.import(root + '/pi/extensions/.shared/prompt-injection-guard.ts');
const { createHubNativeFactories } = await jiti.import(root + '/pi/scripts/native-mcp-compat.mjs');
const factories = await createHubNativeFactories(packageDir, { agentDir, scopeRoot: process.env.HEIMDALL_MCP_SCOPE_ROOT }, sanitizeUntrustedText);
const privileged = (await jiti.import(root + '/pi/extensions/privileged-tool-guard.ts')).default;
const lifecycle = (await jiti.import(root + '/pi/extensions/admin-lifecycle-guard.ts')).default;
const guard = (await jiti.import(root + '/heimdall/employee-guard.ts')).default;
// Use the proven guards for local powers. Business MCP admission comes from
// Heimdall's verified caller scope; OAuth enforces employee entitlements remotely.
factories.push({name:'employee-boundary',factory:async pi => {
  const approved = JSON.parse(process.env.PI_MCP_OAUTH_SERVERS || '[]');
  if (!approved.length || approved.some(s => !['mellanni','linear'].includes(s))) throw new Error('Employee server route denied');
  const authorized = event => {
    if (event.toolName === 'employee_mcp_status') return true;
    const state = globalThis[Symbol.for('pi.hub.native-mcp.compat.v1')];
    const tool = pi.getAllTools().find(t => t.name === event.toolName);
    if (['list_mcp_resources','list_mcp_resource_templates','read_mcp_resource'].includes(event.toolName)) return tool?.sourceInfo?.path === 'builtin:mcp' && state?.snapshot?.servers.length > 0 && (!event.input?.server || approved.includes(event.input.server));
    return tool?.sourceInfo?.path === 'builtin:mcp' && approved.some(s => tool.namespace?.name === 'mcp__' + s)
      && state?.snapshot?.servers.some(s => approved.includes(s.name) && tool.namespace?.name === 'mcp__' + s.name && s.config.enabled !== false);
  };
  privileged({...pi,on(event,handler) { pi.on(event,event==='tool_call' ? async (e,c) => authorized(e) ? undefined : handler(e,c) : handler); }});
  pi.registerTool({name:'employee_mcp_status',label:'Business MCP sign-in status',description:'Check availability of your approved Mellanni/Linear tools and get private sign-in instructions.',parameters:Type.Object({server:Type.Union(approved.map(s => Type.Literal(s)))}),async execute(_id,params) {
    if (!approved.includes(params.server)) throw new Error('Employee server route denied');
    const state = globalThis[Symbol.for('pi.hub.native-mcp.compat.v1')];
    if (!state?.snapshot?.servers.some(s => s.name === params.server)) throw new Error('Business MCP setup unavailable');
    const ready = pi.getAllTools().some(t => t.sourceInfo?.path === 'builtin:mcp' && t.namespace?.name === 'mcp__' + params.server && t.exposure !== 'hidden');
    return {content:[{type:'text',text:ready ? params.server + ' tools are available under your own OAuth identity.' : 'DM the bot `mcp login ' + params.server + '` to sign in with your own account.'}],details:{server:params.server,ready}};
  }});
  lifecycle(pi);
  guard(pi, authorized);
  pi.on('before_agent_start', async () => ({systemPrompt: undefined, message:{role:'custom',customType:'employee-oauth-help',content:'Use your own Mellanni/Linear OAuth. If Mellanni needs sign-in, tell the caller: DM the bot `mcp login mellanni`. Never start sign-in from a model tool call.',display:false}}));
}});
// Pi reports extension-load errors and can otherwise continue. Employee launches
// must fail closed if any required native factory/guard cannot initialize.
for (const entry of factories) {
  const factory = entry.factory;
  entry.factory = async (...args) => {
    try { return await factory(...args); }
    catch { process.stderr.write('Employee safety extension failed to initialize\n'); process.exit(1); }
  };
}
const args = process.argv.slice(2);
const { main } = await import(pathToFileURL(join(packageDir,'dist/main.js')).href);
await main(args, {extensionFactories: factories});
