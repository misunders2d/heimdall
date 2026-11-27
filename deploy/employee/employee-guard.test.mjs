// node deploy/employee/employee-guard.test.mjs (Node >= 22.18 strips TS types)
import assert from 'node:assert/strict';
const { default: guard, approvedMCP } = await import('./employee-guard.ts');
process.env.PI_RESTRICTED_EXTERNAL_USER = '1';
process.env.HEIMDALL_EMPLOYEE_MCP = '["mellanni"]';
let handler;
guard({ on: (_e, h) => { handler = h; } });
const decide = async name => (await handler({ toolName: name, input: {} })) ? 'block' : 'allow';
assert.equal(await decide('mcp__mellanni_search_keywords'), 'allow');
for (const name of ['mcp__linear_list_issues', 'mcp__t3_launch', 'mcp__memento_recall', 'mcp__slack_search', 'mcp__mellanni_', 'mcp__mellannix_a', 'mcp__xmellanni_a', 'mellanni_search', 'bash', 'write'])
  assert.equal(await decide(name), 'block', name);
process.env.HEIMDALL_EMPLOYEE_MCP = '["t3"]';
assert.equal(approvedMCP({ toolName: 'mcp__t3_launch' }), false);
process.env.HEIMDALL_EMPLOYEE_MCP = 'not json';
assert.equal(approvedMCP({ toolName: 'mcp__mellanni_x' }), false);
console.log('employee-guard: ok');
