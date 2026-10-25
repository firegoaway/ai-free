import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import { Writable } from "node:stream";

import { StreamParser, resolveAdaptiveThinking } from "../api/openai-handler.mjs";

// ═══════════════════════════════════════════════════════════════
// Эмуляция thinking-канала: Qwen phase:"think" → delta.reasoning_content
// в OpenAI-совместимом SSE, чтобы Hermes рисовал Reasoning-блоки
// (как с GLM-5.3 через z.ai). Инцидент 25.10: client.mjs отдаёт
// onThinking, но handleQwenStream его не слушает, а StreamParser
// не умеет слать reasoning_content — данные выбрасывались.
// ═══════════════════════════════════════════════════════════════

function makeWritableResponse() {
  const res = new Writable({
    write(chunk, _enc, cb) {
      this.chunks.push(Buffer.from(chunk));
      cb();
    },
  });
  res.chunks = [];
  res.setHeader = () => {};
  res.statusCode = 200;
  return res;
}

function collectSse(res) {
  return Buffer.concat(res.chunks)
    .toString("utf8")
    .split("\n\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => JSON.parse(l.slice(6)));
}

describe("StreamParser reasoning_content channel", () => {
  it("emits reasoning_content deltas and keeps them out of content", async () => {
    const res = makeWritableResponse();
    const parser = new StreamParser("qwen3.8-max", res, { tools: [] });

    parser.onThinking("Разбираю задачу");
    parser.onThinking(": смотрю на файлы");
    parser.onText("Готово");

    await parser.onEnd();

    const events = collectSse(res);
    const reasoning = events
      .map((e) => e.choices[0].delta.reasoning_content)
      .filter(Boolean)
      .join("");
    const content = events
      .map((e) => e.choices[0].delta.content)
      .filter(Boolean)
      .join("");

    assert.equal(reasoning, "Разбираю задачу: смотрю на файлы", "thinking must stream as reasoning_content");
    assert.equal(content, "Готово", "visible text stays in content");
  });

  it("sends the role frame before reasoning when thinking comes first", async () => {
    const res = makeWritableResponse();
    const parser = new StreamParser("qwen3.8-max", res, { tools: [] });

    parser.onThinking("Сначала подумаю");
    await parser.onEnd();

    const events = collectSse(res);
    assert.equal(events[0].choices[0].delta.role, "assistant", "role frame precedes reasoning deltas");
    const reasoning = events.map((e) => e.choices[0].delta.reasoning_content).filter(Boolean).join("");
    assert.equal(reasoning, "Сначала подумаю");
  });

  it("does not leak thinking into the fabrication window or tool detection", async () => {
    const res = makeWritableResponse();
    const tools = [{
      type: "function",
      function: {
        name: "write_file",
        parameters: { type: "object", properties: { path: { type: "string" }, content: { type: "string" } }, required: ["path", "content"] },
      },
    }];
    const parser = new StreamParser("qwen3.8-max", res, { tools });

    // Черновики tool-call JSON внутри reasoning НЕ должны попасть в
    // toolsBuffer/content-канал (инцидент «Tool X does not exists»).
    parser.onThinking('Замысел: вызвать [{"name": "write_file", "arguments": {"path": "x"}}]');
    parser.onText('```tool_calls\n[{"name": "write_file", "arguments": {"path": "y", "content": "z"}}]\n```');

    await parser.onEnd();

    const events = collectSse(res);
    const args = events.flatMap((e) => e.choices[0].delta.tool_calls || []);
    assert.equal(args.length, 1, "only the real tool call is delivered");
    assert.equal(JSON.parse(args[0].function.arguments).path, "y", "draft from thinking must not override the real call");
  });
});

describe("adaptive thinking default depth 200", () => {
  it("keeps thinking on tool loops deeper than 20 (up to 200 messages)", () => {
    assert.equal(resolveAdaptiveThinking({ thinking: true, toolCount: 34, messageCount: 25, env: {} }), true, "depth 25 keeps thinking now");
    assert.equal(resolveAdaptiveThinking({ thinking: true, toolCount: 34, messageCount: 150, env: {} }), true, "depth 150 keeps thinking");
  });

  it("still moderates extremely deep loops", () => {
    assert.equal(resolveAdaptiveThinking({ thinking: true, toolCount: 34, messageCount: 250, env: {} }), false, "depth 250 turns thinking off");
  });

  it("env override still wins", () => {
    assert.equal(resolveAdaptiveThinking({ thinking: true, toolCount: 34, messageCount: 250, env: { QWEN_ADAPTIVE_THINKING_DEPTH: "300" } }), true);
    assert.equal(resolveAdaptiveThinking({ thinking: true, toolCount: 34, messageCount: 25, env: { QWEN_ADAPTIVE_THINKING: "off" } }), true);
  });
});
