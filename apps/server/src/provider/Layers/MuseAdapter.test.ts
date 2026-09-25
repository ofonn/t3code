// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  ApprovalRequestId,
  MuseSettings,
  ProviderDriverKind,
  ThreadId,
  ProviderInstanceId,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type { MuseAdapterShape } from "../Services/MuseAdapter.ts";
import { makeMuseAdapter } from "./MuseAdapter.ts";
const decodeMuseSettings = Schema.decodeSync(MuseSettings);

// Test-local service tag so the rest of the file can keep using `yield* MuseAdapter`.
class MuseAdapter extends Context.Service<MuseAdapter, MuseAdapterShape>()(
  "t3/provider/Layers/MuseAdapter.test/MuseAdapter",
) {}

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockHostPath = NodePath.join(__dirname, "../../../scripts/msp-mock-host.ts");
const mockHostCommand = "node";
const mockHostArgs = [mockHostPath] as const;

async function makeMockHostWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-muse.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
exec ${JSON.stringify(mockHostCommand)} ${mockHostArgs.map((arg) => JSON.stringify(arg)).join(" ")} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function waitForFileContent(filePath: string, attempts = 120) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const raw = await NodeFSP.readFile(filePath, "utf8");
      if (raw.trim().length > 0) {
        return raw;
      }
    } catch {}
    await NodeTimersPromises.setTimeout(25);
  }
  throw new Error(`Timed out waiting for file content at ${filePath}`);
}

async function waitForLogMethod(filePath: string, method: string, attempts = 200) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const frames = await readJsonLines(filePath);
      if (frames.some((frame) => frame.method === method)) {
        return frames;
      }
    } catch {}
    await NodeTimersPromises.setTimeout(25);
  }
  throw new Error(`Timed out waiting for ${method} at ${filePath}`);
}

// Tests mutate `ServerSettingsService` mid-flight (e.g. setting
// `providers.muse.binaryPath` to a mock MSP wrapper). The adapter captures
// `museSettings` once at construction, so without a resolver the mutation
// is invisible. Wiring `resolveSettings` through
// `ServerSettingsService.getSettings` makes each session read the latest
// snapshot.
const makeResolveMuseSettings = Effect.gen(function* () {
  const serverSettings = yield* ServerSettingsService;
  return yield* Effect.succeed(
    serverSettings.getSettings.pipe(
      Effect.map((snapshot) => snapshot.providers.muse),
      Effect.orDie,
    ),
  );
});

const museAdapterTestLayer = it.layer(
  Layer.effect(
    MuseAdapter,
    Effect.gen(function* () {
      const museConfig = decodeMuseSettings({});
      const resolveSettings = yield* makeResolveMuseSettings;
      return yield* makeMuseAdapter(museConfig, { resolveSettings });
    }),
  ).pipe(
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3code-muse-adapter-test-",
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  ),
);

museAdapterTestLayer("MuseAdapter", (it) => {
  it.effect("rejects startSession for another provider", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const failure = yield* adapter
        .startSession({
          threadId: ThreadId.make("muse-wrong-provider"),
          provider: ProviderDriverKind.make("cursor"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );

  it.effect("requires a non-empty cwd", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const failure = yield* adapter
        .startSession({
          threadId: ThreadId.make("muse-empty-cwd"),
          provider: ProviderDriverKind.make("muse"),
          cwd: "   ",
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );

  it.effect("starts a session against the mock host and emits session events", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-mock-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockHostWrapper());
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* Stream.take(adapter.streamEvents, 3).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      assert.equal(session.provider, "muse");
      assert.equal(session.status, "ready");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-msp-session-1",
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      assert.deepStrictEqual(
        runtimeEvents.map((event) => event.type),
        ["session.started", "session.state.changed", "thread.started"],
      );
      const threadStarted = runtimeEvents.find((event) => event.type === "thread.started");
      assert.isDefined(threadStarted);
      if (threadStarted?.type === "thread.started") {
        assert.equal(threadStarted.payload.providerThreadId, "mock-msp-session-1");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("tracks sessions across list, has, and stop", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-track-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockHostWrapper());
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      assert.isTrue(yield* adapter.hasSession(threadId));
      const sessions = yield* adapter.listSessions();
      assert.isTrue(sessions.some((entry) => entry.threadId === threadId));

      yield* adapter.stopSession(threadId);

      assert.isFalse(yield* adapter.hasSession(threadId));
      const remaining = yield* adapter.listSessions();
      assert.isFalse(remaining.some((entry) => entry.threadId === threadId));
    }),
  );

  it.effect("resumes the stored session when given a resume cursor", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-resume-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "prior-msp-session" },
      });
      assert.equal(session.status, "ready");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "prior-msp-session",
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const resumeFrame = frames.find((frame) => frame.method === "session/resume");
      assert.isDefined(resumeFrame);
      const resumeParams = resumeFrame?.params as Record<string, unknown>;
      assert.equal(resumeParams.sessionId, "prior-msp-session");
      assert.isTrue(frames.every((frame) => frame.method !== "session/start"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("starts fresh when given a resume cursor the host cannot load", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-resume-fallback-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
          T3_MSP_RESUME_FAIL: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "stale-session" },
      });
      assert.equal(session.status, "ready");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-msp-session-1",
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const methods = frames.map((frame) => frame.method);
      assert.include(methods, "session/resume");
      assert.include(methods, "session/start");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("starts fresh when given a malformed resume cursor", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-resume-malformed-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 99, sessionId: "" },
      });
      assert.equal(session.status, "ready");

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(frames.every((frame) => frame.method !== "session/resume"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reads and rolls back the local turn ledger", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-ledger-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockHostWrapper());
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const snapshot = yield* adapter.readThread(threadId);
      assert.deepStrictEqual(snapshot.turns, []);

      const rolledBack = yield* adapter.rollbackThread(threadId, 1);
      assert.deepStrictEqual(rolledBack.turns, []);

      const failure = yield* adapter.rollbackThread(threadId, 0).pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("passes sandbox flags and approval mode from session input", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const argvLogPath = NodePath.join(logDir, "argv.log");
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_ARGV_LOG_PATH: argvLogPath,
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const openThread = ThreadId.make("muse-sandbox-open");
      yield* adapter.startSession({
        threadId: openThread,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const lockedThread = ThreadId.make("muse-sandbox-locked");
      yield* adapter.startSession({
        threadId: lockedThread,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        sandboxMode: "read-only",
      });

      const defaultThread = ThreadId.make("muse-sandbox-default");
      yield* adapter.startSession({
        threadId: defaultThread,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const argvLines = yield* Effect.promise(async () => {
        const raw = await NodeFSP.readFile(argvLogPath, "utf8");
        return raw
          .split("\n")
          .map((line) => line.trim())
          .filter((line) => line.length > 0);
      });
      assert.deepStrictEqual(argvLines, [
        "serve\t--trust-workspace\t--disable-sandbox",
        "serve\t--trust-workspace\t--disable-write\t--disable-shell",
        "serve\t--trust-workspace",
      ]);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const starts = requests.filter((entry) => entry.method === "session/start");
      assert.equal(starts.length, 3);
      const openParams = starts[0]?.params as Record<string, unknown>;
      const lockedParams = starts[1]?.params as Record<string, unknown>;
      const defaultParams = starts[2]?.params as Record<string, unknown>;
      assert.isUndefined(openParams.approvalMode);
      assert.equal(openParams.workspaceRoot, process.cwd());
      assert.equal(openParams.modelId, "muse-spark-1.3-contributor");
      assert.isUndefined(lockedParams.approvalMode);
      assert.equal(lockedParams.modelId, "muse-spark-1.3-contributor");
      assert.isUndefined(defaultParams.approvalMode);
      assert.equal(defaultParams.modelId, "muse-spark-1.3-contributor");
      const modeSets = requests.filter((entry) => entry.method === "session/setApprovalMode");
      assert.equal(modeSets.length, 1);
      const modeSet = modeSets[0];
      assert.isDefined(modeSet);
      assert.equal((modeSet.params as Record<string, unknown>).mode, "allowAll");

      yield* adapter.stopSession(openThread);
      yield* adapter.stopSession(lockedThread);
      yield* adapter.stopSession(defaultThread);
    }),
  );

  it.effect("sends a turn and maps the mock turn flow to runtime events", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-turn-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockHostWrapper());
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });
      const result = yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
      });

      assert.isDefined(result.turnId);
      assert.deepStrictEqual(result.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-msp-session-1",
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const types = runtimeEvents.map((event) => event.type);
      for (const t of [
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "item.started",
        "content.delta",
        "item.completed",
        "turn.completed",
      ] as const) {
        assert.include(types, t);
      }

      const assistantStarted = runtimeEvents.find(
        (event) => event.type === "item.started" && event.payload.itemType === "assistant_message",
      );
      assert.isDefined(assistantStarted);

      const delta = runtimeEvents.find((event) => event.type === "content.delta");
      assert.isDefined(delta);
      if (delta?.type === "content.delta") {
        assert.equal(delta.payload.delta, "hello from mock");
      }

      const completed = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.isDefined(completed);
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "completed");
      }

      const snapshot = yield* adapter.readThread(threadId);
      assert.equal(snapshot.turns.length, 1);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("steers a running turn instead of opening a new one on mid-turn sendTurn", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-steer-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_TURN_DELAY_MS: "1500" }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const firstTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "run 5 commands", attachments: [] })
        .pipe(Effect.forkChild);

      yield* Effect.gen(function* () {
        for (let attempt = 0; attempt < 200; attempt += 1) {
          const sessions = yield* adapter.listSessions();
          const session = sessions.find((entry) => entry.threadId === threadId);
          if (session?.activeTurnId !== undefined) return;
          yield* TestClock.adjust("10 millis");
        }
        throw new Error("Timed out waiting for the first prompt to be in flight.");
      });

      const steeredTurn = yield* adapter.sendTurn({
        threadId,
        input: "actually run 15",
        attachments: [],
      });
      const firstTurn = yield* Fiber.join(firstTurnFiber);
      assert.equal(String(steeredTurn.turnId), String(firstTurn.turnId));

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const turnStartedEvents = runtimeEvents.filter((event) => event.type === "turn.started");
      const turnCompletedEvents = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(turnStartedEvents.length, 1);
      assert.equal(String(turnStartedEvents[0]?.turnId), String(firstTurn.turnId));
      assert.equal(turnCompletedEvents.length, 1);
      assert.equal(String(turnCompletedEvents[0]?.turnId), String(firstTurn.turnId));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("interrupts a hanging turn and reports cancellation", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-interrupt-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_TURN_HANG: "1" }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "hang forever", attachments: [] })
        .pipe(Effect.forkChild);

      yield* Effect.gen(function* () {
        for (let attempt = 0; attempt < 200; attempt += 1) {
          const sessions = yield* adapter.listSessions();
          const session = sessions.find((entry) => entry.threadId === threadId);
          if (session?.activeTurnId !== undefined) return;
          yield* TestClock.adjust("10 millis");
        }
        throw new Error("Timed out waiting for the turn to start.");
      });

      yield* adapter.interruptTurn(threadId);
      const result = yield* Fiber.join(turnFiber);
      assert.isDefined(result.turnId);

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const completed = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.isDefined(completed);
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "cancelled");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("maps failed turns to failed completion", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-failed-turn-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_TURN_FAIL: "1" }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "fail please", attachments: [] });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const completed = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.isDefined(completed);
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "failed");
        assert.equal(completed.payload.errorMessage, "mock turn failed");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("routes approval requests through decide", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-approval-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-approval-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_EMIT_APPROVAL: "1",
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      const approvalFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId && event.type === "request.opened"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "list files", attachments: [] })
        .pipe(Effect.forkChild);

      const [opened] = Array.from(yield* Fiber.join(approvalFiber));
      assert.isDefined(opened);
      if (opened?.type !== "request.opened") throw new Error("expected request.opened");
      assert.equal(opened.payload.requestType, "command_execution_approval");
      assert.deepStrictEqual(
        opened.payload.options?.map((option) => option.label),
        ["Allow once", "Allow for session", "Deny"],
      );
      assert.isDefined(opened.requestId);

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(String(opened.requestId)),
        "accept",
      );
      yield* Fiber.join(turnFiber);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const decides = requests.filter((entry) => entry.method === "approval/decide");
      assert.equal(decides.length, 1);
      const firstDecide = decides[0];
      assert.isDefined(firstDecide);
      assert.equal((firstDecide.params as Record<string, unknown>).choiceId, "allow-once");

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const resolved = runtimeEvents.filter((event) => event.type === "request.resolved");
      assert.equal(resolved.length, 1);
      if (resolved[0]?.type === "request.resolved") {
        assert.equal(resolved[0].payload.decision, "accept");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("re-asks T3 on multi-stage approvals", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-approval-multistage-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-multistage-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_APPROVAL_MULTISTAGE: "1",
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      const firstApprovalFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId && event.type === "request.opened"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "list files", attachments: [] })
        .pipe(Effect.forkChild);

      const [first] = Array.from(yield* Fiber.join(firstApprovalFiber));
      if (first?.type !== "request.opened") throw new Error("expected request.opened");
      const requestId = ApprovalRequestId.make(String(first.requestId));
      yield* adapter.respondToRequest(threadId, requestId, "accept");

      const secondApprovalFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId && event.type === "request.opened"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      const [second] = Array.from(yield* Fiber.join(secondApprovalFiber));
      if (second?.type !== "request.opened") throw new Error("expected second request.opened");
      assert.equal(String(second.requestId), String(first.requestId));
      yield* adapter.respondToRequest(threadId, requestId, "acceptForSession");
      yield* Fiber.join(turnFiber);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const decides = requests.filter((entry) => entry.method === "approval/decide");
      assert.equal(decides.length, 2);
      const firstDecide = decides[0];
      const secondDecide = decides[1];
      assert.isDefined(firstDecide);
      assert.isDefined(secondDecide);
      const firstParams = firstDecide.params as Record<string, unknown>;
      const secondParams = secondDecide.params as Record<string, unknown>;
      assert.deepStrictEqual(firstParams.requirementId, {
        approvalId: firstParams.approvalId,
        sourceIndex: 0,
      });
      assert.deepStrictEqual(secondParams.requirementId, {
        approvalId: secondParams.approvalId,
        sourceIndex: 1,
      });
      assert.equal(secondParams.choiceId, "allow-session");

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const resolved = runtimeEvents.filter((event) => event.type === "request.resolved");
      assert.equal(resolved.length, 1);
      if (resolved[0]?.type === "request.resolved") {
        assert.equal(resolved[0].payload.decision, "acceptForSession");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("treats host-resolved approvals as settled without deciding", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-approval-timeout-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-timeout-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_APPROVAL_TIMEOUT: "1",
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const openedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId && event.type === "request.opened"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "list files", attachments: [] })
        .pipe(Effect.forkChild);

      const [opened] = Array.from(yield* Fiber.join(openedFiber));
      if (opened?.type !== "request.opened") throw new Error("expected request.opened");

      const resolvedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId && event.type === "request.resolved"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      const [resolved] = Array.from(yield* Fiber.join(resolvedFiber));
      if (resolved?.type !== "request.resolved") throw new Error("expected request.resolved");
      assert.isUndefined(resolved.payload.decision);

      const failure = yield* adapter
        .respondToRequest(threadId, ApprovalRequestId.make(String(opened.requestId)), "accept")
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterRequestError");

      yield* Fiber.join(turnFiber);
      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isFalse(requests.some((entry) => entry.method === "approval/decide"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("routes user-input requests through answer", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-user-input-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-userinput-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_EMIT_USER_INPUT: "1",
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const inputFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) => event.threadId === threadId && event.type === "user-input.requested",
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "ask me", attachments: [] })
        .pipe(Effect.forkChild);

      const [requested] = Array.from(yield* Fiber.join(inputFiber));
      if (requested?.type !== "user-input.requested") {
        throw new Error("expected user-input.requested");
      }
      assert.deepStrictEqual(
        requested.payload.questions.map((question) => ({
          id: question.id,
          options: question.options.map((option) => option.label),
          multiSelect: question.multiSelect,
        })),
        [{ id: "q1", options: ["A", "B"], multiSelect: false }],
      );

      const resolvedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) => event.threadId === threadId && event.type === "user-input.resolved",
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.respondToUserInput(
        threadId,
        ApprovalRequestId.make(String(requested.requestId)),
        { q1: "B" },
      );
      const [resolved] = Array.from(yield* Fiber.join(resolvedFiber));
      if (resolved?.type !== "user-input.resolved") {
        throw new Error("expected user-input.resolved");
      }
      assert.deepStrictEqual(resolved.payload.answers, { q1: "B" });
      yield* Fiber.join(turnFiber);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const answers = requests.filter((entry) => entry.method === "userInput/answer");
      assert.equal(answers.length, 1);
      const firstAnswer = answers[0];
      assert.isDefined(firstAnswer);
      assert.deepStrictEqual((firstAnswer.params as Record<string, unknown>).answers, [
        { questionId: "q1", selectedLabel: "B" },
      ]);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("sends unmatched user-input text as free text", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-user-input-freetext-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-freetext-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_EMIT_USER_INPUT: "1",
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const inputFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) => event.threadId === threadId && event.type === "user-input.requested",
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnFiber = yield* adapter
        .sendTurn({ threadId, input: "ask me", attachments: [] })
        .pipe(Effect.forkChild);

      const [requested] = Array.from(yield* Fiber.join(inputFiber));
      if (requested?.type !== "user-input.requested") {
        throw new Error("expected user-input.requested");
      }
      yield* adapter.respondToUserInput(
        threadId,
        ApprovalRequestId.make(String(requested.requestId)),
        { q1: "something custom" },
      );
      yield* Fiber.join(turnFiber);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const answers = requests.filter((entry) => entry.method === "userInput/answer");
      assert.equal(answers.length, 1);
      const firstAnswer = answers[0];
      assert.isDefined(firstAnswer);
      assert.deepStrictEqual((firstAnswer.params as Record<string, unknown>).answers, [
        { questionId: "q1", freeText: "something custom" },
      ]);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("switches the session model in-session", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-model-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-model-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "custom-model" },
      });
      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "other-model" },
      });

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const starts = requests.filter((entry) => entry.method === "session/start");
      assert.equal(starts.length, 1);
      const firstStart = starts[0];
      assert.isDefined(firstStart);
      assert.equal((firstStart.params as Record<string, unknown>).modelId, "custom-model");
      const modelSets = requests.filter((entry) => entry.method === "session/setModel");
      assert.equal(modelSets.length, 1);
      const firstModelSet = modelSets[0];
      assert.isDefined(firstModelSet);
      assert.deepStrictEqual((firstModelSet.params as Record<string, unknown>).model, {
        modelId: "other-model",
      });

      const sessions = yield* adapter.listSessions();
      assert.equal(sessions.find((entry) => entry.threadId === threadId)?.model, "other-model");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("pins default model selections to the contributor tier", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-pin-model-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });
      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const starts = frames.filter((frame) => frame.method === "session/start");
      assert.equal(starts.length, 1);
      const firstStart = starts[0];
      assert.isDefined(firstStart);
      assert.equal(
        (firstStart.params as Record<string, unknown>).modelId,
        "muse-spark-1.3-contributor",
      );
      assert.isTrue(frames.every((frame) => frame.method !== "session/setModel"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("switches back to the pinned model when default is selected", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-unpin-model-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "custom-model" },
      });
      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const modelSets = frames.filter((frame) => frame.method === "session/setModel");
      assert.equal(modelSets.length, 1);
      const firstModelSet = modelSets[0];
      assert.isDefined(firstModelSet);
      assert.deepStrictEqual((firstModelSet.params as Record<string, unknown>).model, {
        modelId: "muse-spark-1.3-contributor",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("syncs reasoning effort at session start", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-effort-start-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("muse"),
          model: "default",
          options: [{ id: "reasoningEffort", value: "low" }],
        },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const effortSets = frames.filter((frame) => frame.method === "session/setReasoningEffort");
      assert.equal(effortSets.length, 1);
      const firstEffortSet = effortSets[0];
      assert.isDefined(firstEffortSet);
      assert.equal((firstEffortSet.params as Record<string, unknown>).reasoningEffort, "low");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("syncs the default effort on fresh starts without an explicit choice", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-effort-default-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const effortSets = frames.filter((frame) => frame.method === "session/setReasoningEffort");
      assert.equal(effortSets.length, 1);
      const firstEffortSet = effortSets[0];
      assert.isDefined(firstEffortSet);
      assert.equal((firstEffortSet.params as Record<string, unknown>).reasoningEffort, "max");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("leaves standing effort alone when resuming without an explicit choice", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-effort-resume-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "prior-msp-session" },
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(frames.some((frame) => frame.method === "session/resume"));
      assert.isTrue(frames.every((frame) => frame.method !== "session/setReasoningEffort"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("syncs reasoning effort changes per turn", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-effort-turn-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turnSelection = (effort: string) => ({
        instanceId: ProviderInstanceId.make("muse"),
        model: "default",
        options: [{ id: "reasoningEffort", value: effort }],
      });
      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: turnSelection("medium"),
      });
      yield* adapter.sendTurn({
        threadId,
        input: "hello again",
        attachments: [],
        modelSelection: turnSelection("medium"),
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const effortSets = frames.filter((frame) => frame.method === "session/setReasoningEffort");
      assert.equal(effortSets.length, 2);
      assert.deepStrictEqual(
        effortSets.map((frame) => (frame.params as Record<string, unknown>).reasoningEffort),
        ["max", "medium"],
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("ignores invalid reasoning effort tiers", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-effort-invalid-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_REQUEST_LOG_PATH: requestLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("muse"),
          model: "default",
          options: [{ id: "reasoningEffort", value: "bogus" }],
        },
      });
      assert.equal(session.status, "ready");
      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: {
          instanceId: ProviderInstanceId.make("muse"),
          model: "default",
          options: [{ id: "reasoningEffort", value: "bogus" }],
        },
      });

      yield* Effect.promise(() => waitForFileContent(requestLogPath));
      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const effortSets = frames.filter((frame) => frame.method === "session/setReasoningEffort");
      assert.equal(effortSets.length, 1);
      assert.deepStrictEqual(
        effortSets.map((frame) => (frame.params as Record<string, unknown>).reasoningEffort),
        ["max"],
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("falls back to a fresh turn when steer hits a terminal turn", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-steer-fallback-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
          T3_MSP_TURN_DELAY_MS: "2000",
          T3_MSP_STEER_FAIL: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const firstSelection = {
        instanceId: ProviderInstanceId.make("muse"),
        model: "default",
      };
      const firstFiber = yield* Effect.forkChild(
        adapter.sendTurn({
          threadId,
          input: "first",
          attachments: [],
          modelSelection: firstSelection,
        }),
      );
      yield* Effect.promise(() => waitForLogMethod(requestLogPath, "turn/start"));
      const second = yield* adapter.sendTurn({
        threadId,
        input: "second",
        attachments: [],
        modelSelection: firstSelection,
      });
      assert.isDefined(second.turnId);

      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const methods = frames.map((frame) => frame.method);
      assert.isTrue(methods.includes("turn/steer"));
      assert.equal(methods.filter((method) => method === "turn/start").length, 2);

      yield* Fiber.interrupt(firstFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("re-establishes the session after an idle close", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-session-close-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
          T3_MSP_CLOSE_SESSION: "1",
          T3_MSP_CLOSE_ON_NEXT_REQUEST: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      // The mock idle-closes the session ahead of the next request, so the
      // close notification always precedes its response on the wire and is
      // processed before startSession returns. The brief wall-clock pause
      // only absorbs scheduling jitter; ordering no longer depends on it.
      // Real timers: this suite runs under TestClock, which Effect.sleep
      // would wait on forever.
      yield* Effect.promise(() => NodeTimersPromises.setTimeout(500));
      const result = yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });
      assert.isDefined(result.turnId);

      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.equal(frames.filter((frame) => frame.method === "session/start").length, 2);
      const starts = frames.filter((frame) => frame.method === "turn/start");
      assert.equal(starts.length, 1);
      const firstStart = starts[0];
      assert.isDefined(firstStart);
      assert.equal((firstStart.params as Record<string, unknown>).sessionId, "mock-msp-session-2");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("retries once when a turn hits an unloaded session", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-session-retry-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
          T3_MSP_NOT_LOADED_ONCE: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const result = yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });
      assert.isDefined(result.turnId);

      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.equal(frames.filter((frame) => frame.method === "turn/start").length, 2);
      assert.equal(frames.filter((frame) => frame.method === "session/resume").length, 1);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("replaces a stalled resumed turn with a fresh session and completes", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-stall-recovery-thread");
      const logDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-msp-log-")),
      );
      const requestLogPath = NodePath.join(logDir, "requests.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_REQUEST_LOG_PATH: requestLogPath,
          T3_MSP_TURN_HANG_ONCE: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      // Resume a session the host accepts but never drives: the zombie
      // the production host served after the original crash.
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "mock-msp-session-99" },
      });

      const turnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "hello mock",
          attachments: [],
          modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
        })
        .pipe(Effect.forkChild);

      // The first turn/start hangs; outwait the stall timeout on the test
      // clock in small steps so an in-flight protocol ack never trips its
      // own 30s timeout while the clock jumps.
      yield* Effect.promise(() => waitForLogMethod(requestLogPath, "turn/start"));
      for (let attempt = 0; attempt < 40; attempt += 1) {
        const seen = yield* Effect.promise(() => readJsonLines(requestLogPath));
        if (seen.some((frame) => frame.method === "session/start")) break;
        yield* TestClock.adjust("5 seconds");
      }

      const recoveryFrames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(recoveryFrames.some((frame) => frame.method === "session/start"));

      const result = yield* Fiber.join(turnFiber);
      assert.isDefined(result.turnId);
      assert.equal(
        (result.resumeCursor as Record<string, unknown>).sessionId,
        "mock-msp-session-1",
      );

      const frames = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const starts = frames.filter((frame) => frame.method === "turn/start");
      assert.equal(starts.length, 2);
      const firstStart = starts[0];
      const secondStart = starts[1];
      assert.isDefined(firstStart);
      assert.isDefined(secondStart);
      assert.equal((firstStart.params as Record<string, unknown>).sessionId, "mock-msp-session-99");
      assert.equal((secondStart.params as Record<string, unknown>).sessionId, "mock-msp-session-1");
      // The zombie cursor is never resumed again: one resume at start,
      // then a fresh session for the retry.
      assert.equal(frames.filter((frame) => frame.method === "session/resume").length, 1);
      assert.equal(frames.filter((frame) => frame.method === "session/start").length, 1);

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const completed = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.isDefined(completed);
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "completed");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("skips reminder items in the transcript", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-reminder-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_EMIT_REMINDER: "1" }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "hello mock", attachments: [] });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      assert.isTrue(runtimeEvents.some((event) => event.type === "turn.completed"));
      const reminderEvents = runtimeEvents.filter(
        (event) => "itemId" in event && event.itemId === "reminder-item-1",
      );
      assert.deepStrictEqual(reminderEvents, []);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("exposes tool call commands in the transcript", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-tool-detail-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_EMIT_TOOLCALL: "1" }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "hello mock", attachments: [] });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const toolCompleted = runtimeEvents.find(
        (event) => event.type === "item.completed" && event.itemId === "tool-item-1",
      );
      assert.isDefined(toolCompleted);
      if (toolCompleted?.type !== "item.completed") return;
      assert.equal(toolCompleted.payload.title, "bash: ls /tmp/mock");
      assert.include(toolCompleted.payload.detail ?? "", "ls /tmp/mock");
      assert.include(toolCompleted.payload.detail ?? "", "mock-a.txt");
      const toolData = toolCompleted.payload.data as Record<string, unknown>;
      const toolItem = toolData.item as Record<string, unknown>;
      assert.equal(toolItem.command, "ls /tmp/mock");

      const shellCompleted = runtimeEvents.find(
        (event) => event.type === "item.completed" && event.itemId === "shell-item-1",
      );
      assert.isDefined(shellCompleted);
      if (shellCompleted?.type !== "item.completed") return;
      assert.equal(shellCompleted.payload.title, "echo shell-ok");
      assert.include(shellCompleted.payload.detail ?? "", "<exited with exit code 0>");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("keeps working when tool args are not JSON", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-tool-bad-args-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({
          T3_MSP_EMIT_TOOLCALL: "1",
          T3_MSP_TOOLCALL_BAD_ARGS: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "hello mock", attachments: [] });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      assert.isTrue(runtimeEvents.some((event) => event.type === "turn.completed"));
      const toolCompleted = runtimeEvents.find(
        (event) => event.type === "item.completed" && event.itemId === "tool-item-1",
      );
      assert.isDefined(toolCompleted);
      if (toolCompleted?.type !== "item.completed") return;
      assert.equal(toolCompleted.payload.title, "bash");
      assert.include(toolCompleted.payload.detail ?? "", "{not json");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("closes the MSP child process when a session stops", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-stop-session-close");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-adapter-exit-log-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_EXIT_LOG_PATH: exitLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("muse"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("muse"), model: "default" },
      });

      yield* adapter.stopSession(threadId);

      const exitLog = yield* Effect.promise(() => waitForFileContent(exitLogPath));
      assert.include(exitLog, "exit");
    }),
  );

  it.effect("serializes concurrent startSession calls and closes the replaced session", () =>
    Effect.gen(function* () {
      const adapter = yield* MuseAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("muse-concurrent-start-session");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-adapter-concurrent-exit-log-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockHostWrapper({ T3_MSP_EXIT_LOG_PATH: exitLogPath }),
      );
      yield* settings.updateSettings({ providers: { muse: { binaryPath: wrapperPath } } });

      yield* Effect.all(
        [
          adapter.startSession({
            threadId,
            provider: ProviderDriverKind.make("muse"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
          }),
          adapter.startSession({
            threadId,
            provider: ProviderDriverKind.make("muse"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
          }),
        ],
        { concurrency: 2 },
      );

      const sessions = yield* adapter.listSessions();
      assert.equal(sessions.filter((entry) => entry.threadId === threadId).length, 1);
      const exitLog = yield* Effect.promise(() => waitForFileContent(exitLogPath));
      assert.include(exitLog, "exit");

      yield* adapter.stopSession(threadId);
    }),
  );
});
