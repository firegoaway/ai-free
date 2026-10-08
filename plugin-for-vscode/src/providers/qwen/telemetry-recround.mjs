/**
 * Телеметрический самописец Qwen-прокси (жучок).
 *
 * Включение: QWEN_TELEMETRY=1 в окружении запуска `npm run server`.
 * Что пишет (events.jsonl в telemetry/run-<UTC-ts>-<label>/):
 *   - punish_nav        — навигация страницы на /_____tmd_____/punish;
 *   - antibot_response  — ответ антибота (initialize/report/csig) с телом;
 *   - fourier_telemetry — POST на fourier.taobao.com (накопление таймингов);
 *   - solver_timeline   — полный таймлайн драга (каждый ход, координаты, dt);
 *   - solver_result     — исход попытки солва (solved/tries/error, x5sec);
 *   - fetch_span        — начало/конец POST /completions и его судьба;
 *   - console_error     — pageerror со страницы;
 *   - cooldown          — включение punish-кулдауна (account/pool).
 * Ротация: events.jsonl -> events-<N>.jsonl на 50 МБ (лимит не жёсткий).
 * Всё асинхронно, запись никогда не бросает и не блокирует прокси; при
 * закрытии — flush. Секреты (Authorization/token/cookie-значения) вырезаются.
 *
 * Зачем: Baxia реагирует быстро, но понять ПОЧЕМУ солвер не прошёл
 * (траектория? окружение? тайминг?) можно только по полной картине
 * запросов и движений. HAR даёт сеть, но не наши драги; это даёт всё.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

const ROTATE_BYTES = 50 * 1024 * 1024;

/** Включена ли телеметрия по env? */
export function shouldRecordTelemetry(env = process.env) {
  return /^(1|true|yes|on)$/i.test(String(env.QWEN_TELEMETRY ?? ""));
}

const SECRET_KEY_RE = /^(authorization|token|password|secret|cookie|cookies|bearer|api[_-]?key)$/i;
const SECRET_URL_RE = /[?&](token|sig|sign|signature|secret|password|access[_-]?token|auth)=[^&]*/gi;

/** Рекурсивно вырезать секреты из объекта/строки перед записью на диск. */
export function scrubSecrets(value, seen = new WeakSet(), depth = 0) {
  if (value == null) return value;
  if (typeof value === "string") {
    return value.length > 4000
      ? value.replace(SECRET_URL_RE, (m) => m.split("=")[0] + "=[REDACTED]").slice(0, 4000) + "…[truncated]"
      : value.replace(SECRET_URL_RE, (m) => m.split("=")[0] + "=[REDACTED]");
  }
  if (typeof value !== "object") return value;
  if (seen.has(value)) return "[Circular]";
  if (depth > 8) return "[Deep]";
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      return value.slice(0, 64).map((v) => scrubSecrets(v, seen, depth + 1));
    }
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = SECRET_KEY_RE.test(k) ? "[REDACTED]" : scrubSecrets(v, seen, depth + 1);
    }
    return out;
  } finally {
    seen.delete(value);
  }
}

/**
 * Создать самописец. Возвращает { dir, record, attachToPage, attachToContext,
 * flush, close }. Никогда не бросает: запись в best-effort.
 */
export function createTelemetryRecorder({ root, label = "default", rotateBytes = ROTATE_BYTES } = {}) {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const dir = path.join(root, `run-${ts}-${label}`);
  fs.mkdirSync(dir, { recursive: true });

  let fileIndex = 0;
  let bytesWritten = 0;
  const eventsPath = () => path.join(dir, fileIndex === 0 ? "events.jsonl" : `events-${fileIndex}.jsonl`);
  let stream = null;
  let closed = false;
  const startedAt = Date.now();
  let eventCount = 0;

  const manifest = {
    label,
    startedAt: new Date(startedAt).toISOString(),
    pid: process.pid,
    node: process.version,
    rotateBytes,
    solverVersion: "baxia-nc-drag-v2",
  };
  try { fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(manifest, null, 2)); } catch {}

  function openStream() {
    if (stream || closed) return;
    try {
      stream = fs.createWriteStream(eventsPath(), { flags: "a" });
      stream.on("error", () => { try { stream?.destroy?.(); } catch {} stream = null; });
      // файл появляется асинхронно; помечаем готовность по событию open
      stream.on("open", () => { streamReady = true; });
    } catch {
      stream = null;
    }
  }

  let streamReady = false;

  function record(type, data) {
    if (closed) return;
    eventCount += 1;
    const line = JSON.stringify({ ts: Date.now(), t: Date.now() - startedAt, n: eventCount, type, data: scrubSecrets(data) }) + "\n";
    openStream();
    if (!stream) return;
    if (bytesWritten > rotateBytes) {
      const old = stream;
      fileIndex += 1;
      bytesWritten = 0;
      streamReady = false;
      stream = null;
      old.end(() => openStream());
      // ротация редкая — строку текущую пишем уже в новый файл после open
      pendingLines.push(line);
      return;
    }
    bytesWritten += line.length;
    if (streamReady) stream.write(line);
    else pendingLines.push(line);
  }

  const pendingLines = [];

  function drainPending() {
    if (!stream || !streamReady) return;
    while (pendingLines.length) {
      const line = pendingLines.shift();
      bytesWritten += line.length;
      stream.write(line);
    }
  }

  // drain периодически
  const drainTimer = setInterval(() => {
    drainPending();
  }, 50);
  drainTimer.unref?.();

  async function flush() {
    for (let i = 0; i < 100 && (!stream || !streamReady); i += 1) {
      await new Promise((r) => setTimeout(r, 10));
    }
    drainPending();
    await new Promise((resolve) => {
      if (!stream) return resolve();
      stream.write("", () => resolve());
    });
  }

  async function close() {
    closed = true;
    await flush();
    await new Promise((resolve) => {
      if (!stream) return resolve();
      stream.end(resolve);
      stream = null;
    });
  }

  /** Навесить листенеры на страницу (навигации, ошибки, console). */
  function attachToPage(page, pageLabel) {
    try {
      page.on("framenavigated", (frame) => {
        try {
          const url = frame?.url?.() || "";
          // только top-level навигации; punish может быть и в iframe
          if (/_____tmd_____\/punish/.test(url)) {
            let main = null;
            try { main = frame === page.mainFrame(); } catch {}
            record("punish_nav", { page: pageLabel, url, main });
          }
        } catch {}
      });
      page.on("pageerror", (err) => {
        record("console_error", { page: pageLabel, message: String(err?.message || err).slice(0, 500) });
      });
    } catch {}
  }

  /** Навесить листенеры на browser context (сеть: антибот/fourier). */
  function attachToContext(context) {
    try {
      context.on("response", async (response) => {
        try {
          const url = response.url();
          // Антибот-эндпоинты: initialize / report / csig / verify
          if (/cf\.aliyun\.com\/nocaptcha\/(initialize|verify|report|csig)/.test(url) ||
              /fourier\.taobao\.com\/ts/.test(url)) {
            const kind = url.includes("fourier") ? "fourier_telemetry" : "antibot_response";
            let body = null;
            try {
              body = (await response.text()).slice(0, 4000);
            } catch {}
            record(kind, {
              url: url.slice(0, 300),
              status: response.status(),
              ...(kind === "fourier_telemetry" && body ? { bodyLength: body.length } : {}),
              ...(kind === "antibot_response" && body ? { body } : {}),
            });
          }
        } catch {}
      });
      context.on("request", (request) => {
        try {
          const url = request.url();
          if (/fourier\.taobao\.com\/ts/.test(url) && request.method() === "POST") {
            // Тело репорта AWSC — закодированная телеметрия траектории драга.
            // Именно оно нужно для сравнения наших драгов с человеческими.
            const postData = request.postData() || "";
            record("fourier_telemetry", {
              url: url.slice(0, 300),
              method: "POST",
              bodyLength: postData.length,
              body: postData.slice(0, 50_000),
            });
          }
        } catch {}
      });
    } catch {}
  }

  return { dir, record, attachToPage, attachToContext, flush, close };
}

/** Быстрый дайджест содержимого (для отладки анализа телеметрии). */
export function telemetryEventDigest(event) {
  if (!event || typeof event !== "object") return "invalid";
  return `${event.type}:${String(event.data?.page || event.data?.url || event.data?.profile || "").slice(0, 40)}`;
}
