// Runs one prompt through the Claude Agent SDK for agentlink-worker.
//
// stdin:  {"cwd", "prompt", "session_id", "resume", "options"} — options go to query()
// stdout: one JSON line, {"type":"result", ...} or {"type":"error", "error", "session_id"}
// SIGTERM aborts the query; the session keeps what was done so far.
import { query } from '@anthropic-ai/claude-agent-sdk';

const chunks = [];
for await (const c of process.stdin) chunks.push(c);
const req = JSON.parse(Buffer.concat(chunks).toString('utf8'));

const abortController = new AbortController();
process.on('SIGTERM', () => abortController.abort());

const options = { ...(req.options ?? {}), cwd: req.cwd, abortController };
if (req.resume) options.resume = req.session_id; // continues the same session id
else options.sessionId = req.session_id; // a new session under the id the worker chose

const print = (o) => process.stdout.write(JSON.stringify(o) + '\n');
let sessionId = req.session_id;
let result;
try {
  for await (const m of query({ prompt: req.prompt, options })) {
    if (m.session_id) sessionId = m.session_id;
    if (m.type === 'result') result = m;
  }
} catch (e) {
  print({ type: 'error', error: abortController.signal.aborted ? 'aborted' : String(e?.message ?? e), session_id: sessionId });
  process.exit(1);
}
if (!result) {
  print({ type: 'error', error: 'the SDK returned no result', session_id: sessionId });
  process.exit(1);
}
const ok = result.subtype === 'success';
print({
  type: 'result',
  text: ok ? result.result : (result.errors ?? []).join('\n') || result.subtype,
  session_id: result.session_id,
  is_error: result.is_error || !ok,
  num_turns: result.num_turns,
  total_cost_usd: result.total_cost_usd,
  denials: result.permission_denials?.length ?? 0,
});
