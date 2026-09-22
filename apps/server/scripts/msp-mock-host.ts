#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeReadline from "node:readline";
import * as NodeTimers from "node:timers";

const requestLogPath = process.env.T3_MSP_REQUEST_LOG_PATH;
const argvLogPath = process.env.T3_MSP_ARGV_LOG_PATH;
const fixedSessionId = process.env.T3_MSP_SESSION_ID ?? "mock-msp-session-1";
const failStart = process.env.T3_MSP_FAIL_START === "1";
const resumeFail = process.env.T3_MSP_RESUME_FAIL === "1";
const turnResponseText = process.env.T3_MSP_TURN_RESPONSE_TEXT ?? "hello from mock";
const turnDelayMs = Number(process.env.T3_MSP_TURN_DELAY_MS ?? "20");
const turnFail = process.env.T3_MSP_TURN_FAIL === "1";
const hangTurn = process.env.T3_MSP_TURN_HANG === "1";
const emitApproval = process.env.T3_MSP_EMIT_APPROVAL === "1";
const approvalMultistage = process.env.T3_MSP_APPROVAL_MULTISTAGE === "1";
const approvalTimeout = process.env.T3_MSP_APPROVAL_TIMEOUT === "1";
const emitUserInput = process.env.T3_MSP_EMIT_USER_INPUT === "1";
const emitReminder = process.env.T3_MSP_EMIT_REMINDER === "1";
const emitToolCall = process.env.T3_MSP_EMIT_TOOLCALL === "1";
const exitLogPath = process.env.T3_MSP_EXIT_LOG_PATH;

if (exitLogPath) {
  const markExit = (reason: string) => {
    try {
      NodeFS.appendFileSync(exitLogPath, `${reason}\n`);
    } catch {}
  };
  process.on("SIGTERM", () => {
    markExit("SIGTERM");
    process.exit(0);
  });
  process.on("exit", () => markExit("exit"));
}

if (argvLogPath) {
  NodeFS.appendFileSync(argvLogPath, `${process.argv.slice(2).join("\t")}\n`);
}

const logRequest = (frame: unknown) => {
  if (!requestLogPath) return;
  NodeFS.appendFileSync(requestLogPath, `${JSON.stringify(frame)}\n`);
};

const respond = (id: unknown, result: unknown) => {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\n`);
};

const respondError = (id: unknown, code: number, message: string) => {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, error: { code, message } })}\n`);
};

const notify = (method: string, params: unknown) => {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
};

const nowIso = () => "2026-01-01T00:00:00.000Z";
let cursorSeq = 2;
const nextCursor = () => `cursor-${cursorSeq++}`;
let currentTurnId: string | undefined;
const settledTurns = new Set<string>();

const mockRange = () => ({
  first: { id: "rec-1", sequence: 1 },
  last: { id: "rec-1", sequence: 1 },
  stream: {},
});

const approvalChoices = [
  { choiceId: "allow-once", decision: "approved", label: "Allow once", scope: "once" },
  {
    choiceId: "allow-session",
    decision: "approvedForSession",
    label: "Allow for session",
    scope: "session",
  },
  { choiceId: "deny", decision: "denied", label: "Deny", scope: "once" },
];

const approvalFor = (turnId: string) => `approval-${turnId.slice(0, 8)}`;
const userInputFor = (turnId: string) => `user-input-${turnId.slice(0, 8)}`;
const decideCounts = new Map<string, number>();

const emitApprovalRequest = (turnId: string, stage: number) => {
  notify("approval/request", {
    approvalId: approvalFor(turnId),
    availableChoices: approvalChoices,
    currentRequirementId: { approvalId: approvalFor(turnId), sourceIndex: stage },
    itemId: `item-${turnId.slice(0, 8)}`,
    judgeEscalated: false,
    protectedWrite: false,
    rawArgs: "ls /tmp/mock",
    sessionId: fixedSessionId,
    sourceRange: mockRange(),
    subject: { kind: "shell", command: "ls /tmp/mock" },
    taskId: "task-1",
    toolCallId: "call-1",
    toolName: "shell",
    turnId,
    viewCursor: nextCursor(),
  });
};

const emitUserInputRequest = (turnId: string) => {
  notify("userInput/request", {
    itemId: `item-${turnId.slice(0, 8)}`,
    questions: [
      {
        header: "Pick",
        id: "q1",
        options: [
          { label: "A", description: "First option" },
          { label: "B", description: "Second option" },
        ],
        question: "Choose one",
        selection: { mode: "single" },
      },
    ],
    sessionId: fixedSessionId,
    toolCallId: "call-2",
    toolName: "request_user_input",
    turnId,
    userInputId: userInputFor(turnId),
    viewCursor: nextCursor(),
  });
};

const emitTurnHead = (turnId: string) => {
  const itemId = `item-${turnId.slice(0, 8)}`;
  notify("turn/started", {
    commandId: turnId,
    sessionId: fixedSessionId,
    sourceRange: mockRange(),
    turnId,
    viewCursor: nextCursor(),
  });
  notify("item/started", {
    item: { itemId, kind: "agentMessage", revision: 1, status: "inProgress", turnId, text: "" },
    sessionId: fixedSessionId,
    sourceRange: mockRange(),
    viewCursor: nextCursor(),
  });
};

const emitTurnTail = (turnId: string, terminal: string) => {
  if (settledTurns.has(turnId)) return;
  settledTurns.add(turnId);
  const itemId = `item-${turnId.slice(0, 8)}`;
  notify("item/delta", {
    delta: turnResponseText,
    itemId,
    sessionId: fixedSessionId,
    viewCursor: nextCursor(),
  });
  notify("item/completed", {
    item: {
      itemId,
      kind: "agentMessage",
      revision: 2,
      status: "completed",
      turnId,
      text: turnResponseText,
    },
    sessionId: fixedSessionId,
    sourceRange: mockRange(),
    viewCursor: nextCursor(),
  });
  notify("turn/completed", {
    sessionId: fixedSessionId,
    sourceRange: mockRange(),
    terminal,
    turnId,
    viewCursor: nextCursor(),
    usage: { cachedTokens: 0, inputTokens: 11, outputTokens: 7, reasoningTokens: 0 },
    durationMs: turnDelayMs,
    ...(terminal === "failed"
      ? {
          error: { kind: "modelError", message: "mock turn failed", retryable: false },
          reason: "mock failure",
        }
      : {}),
  });
};

const mockSession = (sessionId: string) => ({
  sessionId,
  activeTurnId: null,
  createdAt: nowIso(),
  forkedFrom: null,
  modelId: null,
  path: "",
  providerId: null,
  status: "idle",
  turnCount: 0,
  updatedAt: nowIso(),
  workspaceRoot: null,
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const lineReader = NodeReadline.createInterface({ input: process.stdin });
lineReader.on("close", () => process.exit(0));
lineReader.on("line", (line) => {
  if (!line.trim()) return;
  let frame: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) return;
    frame = parsed;
  } catch {
    return;
  }
  logRequest(frame);
  const id = frame.id;
  const method = frame.method;
  const params = isRecord(frame.params) ? frame.params : {};
  if (typeof method !== "string" || id === undefined) return;
  switch (method) {
    case "initialize": {
      respond(id, {
        experimentalApi: false,
        grantedCapabilities: [],
        museHome: "/tmp/fake-muse-home",
        platformFamily: "linux",
        platformOs: "linux",
        schema: { version: 1 },
        serverInfo: { name: "msp-mock-host", version: "0.0.0" },
        sessionDurability: "durable",
        userAgent: "msp-mock-host/0.0.0",
      });
      return;
    }
    case "session/start": {
      if (failStart) {
        respondError(id, -32001, "mock session start rejected");
        return;
      }
      const requested = typeof params.sessionId === "string" ? params.sessionId : undefined;
      respond(id, {
        session: mockSession(requested ?? fixedSessionId),
        viewCursor: "cursor-start-1",
      });
      return;
    }
    case "session/resume": {
      if (resumeFail) {
        respondError(id, -32002, "mock session resume rejected");
        return;
      }
      const target = typeof params.sessionId === "string" ? params.sessionId : fixedSessionId;
      respond(id, {
        history: { items: [], mode: "none", snapshot: null },
        pendingRequests: [],
        session: mockSession(target),
        viewCursor: "cursor-resume-1",
      });
      return;
    }
    case "session/setApprovalMode": {
      respond(id, {
        applyOutcome: "completed",
        commandId: frame.id,
        effectiveMode: { mode: params.mode ?? "allowAll", source: "approvalReconfigure" },
        status: "accepted",
      });
      return;
    }
    case "turn/start": {
      const turnId = String(frame.id);
      currentTurnId = turnId;
      respond(id, {
        commandId: frame.id,
        disposition: "started",
        startedNewTurn: true,
        status: "accepted",
        turnId,
      });
      if (!hangTurn) {
        // @effect-diagnostics-next-line globalTimers:off - Standalone Node mock host, not an Effect program.
        NodeTimers.setTimeout(() => {
          emitTurnHead(turnId);
          if (emitReminder) {
            notify("item/started", {
              item: {
                itemId: "reminder-item-1",
                kind: "reminderChild",
                revision: 1,
                status: "inProgress",
                turnId,
                text: "",
              },
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
            notify("item/delta", {
              delta: "nudge",
              itemId: "reminder-item-1",
              sessionId: fixedSessionId,
              viewCursor: nextCursor(),
            });
            notify("item/completed", {
              item: {
                itemId: "reminder-item-1",
                kind: "reminderChild",
                revision: 1,
                status: "completed",
                turnId,
                text: "",
              },
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
          }
          if (emitToolCall) {
            const toolItem = {
              itemId: "tool-item-1",
              kind: "toolCall",
              revision: 1,
              status: "inProgress",
              turnId,
              text: "",
              tool: "bash",
              args:
                process.env.T3_MSP_TOOLCALL_BAD_ARGS === "1"
                  ? "{not json"
                  : JSON.stringify({ command: "ls /tmp/mock", description: "List mocks" }),
            };
            notify("item/started", {
              item: toolItem,
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
            notify("item/completed", {
              item: {
                ...toolItem,
                revision: 2,
                status: "completed",
                visibleOutput: "mock-a.txt\nmock-b.txt",
              },
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
            const shellItem = {
              itemId: "shell-item-1",
              kind: "userShell",
              revision: 1,
              status: "inProgress",
              turnId,
              text: "",
              commandText: "echo shell-ok",
            };
            notify("item/started", {
              item: shellItem,
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
            notify("item/completed", {
              item: { ...shellItem, revision: 2, status: "completed", exitCode: 0 },
              sessionId: fixedSessionId,
              sourceRange: mockRange(),
              viewCursor: nextCursor(),
            });
          }
          if (emitUserInput) {
            emitUserInputRequest(turnId);
          } else if (emitApproval || approvalTimeout || approvalMultistage) {
            emitApprovalRequest(turnId, 0);
            if (approvalTimeout) {
              // @effect-diagnostics-next-line globalTimers:off - Standalone Node mock host, not an Effect program.
              NodeTimers.setTimeout(() => {
                notify("approval/resolved", {
                  approvalId: approvalFor(turnId),
                  decision: "timedOut",
                  itemId: `item-${turnId.slice(0, 8)}`,
                  policyResult: "deny",
                  resolvedBy: "timeout",
                  sessionId: fixedSessionId,
                  sourceRange: mockRange(),
                  stageEvidence: [],
                  turnId,
                  viewCursor: nextCursor(),
                });
                emitTurnTail(turnId, "completed");
              }, 100);
            }
          } else {
            emitTurnTail(turnId, turnFail ? "failed" : "completed");
          }
        }, turnDelayMs);
      }
      return;
    }
    case "approval/decide": {
      const approvalId = typeof params.approvalId === "string" ? params.approvalId : "unknown";
      const count = (decideCounts.get(approvalId) ?? 0) + 1;
      decideCounts.set(approvalId, count);
      const decideTurnId = currentTurnId ?? "none";
      if (approvalMultistage && count === 1) {
        respond(id, {
          approvalId,
          commandId: frame.id,
          status: "accepted",
          terminal: false,
        });
        emitApprovalRequest(decideTurnId, 1);
        return;
      }
      respond(id, { approvalId, commandId: frame.id, status: "accepted", terminal: true });
      notify("approval/resolved", {
        approvalId,
        decision: "approved",
        itemId: `item-${decideTurnId.slice(0, 8)}`,
        policyResult: "allow",
        resolvedBy: "client",
        sessionId: fixedSessionId,
        sourceRange: mockRange(),
        stageEvidence: [],
        turnId: decideTurnId,
        viewCursor: nextCursor(),
      });
      emitTurnTail(decideTurnId, "completed");
      return;
    }
    case "userInput/answer": {
      const userInputId = typeof params.userInputId === "string" ? params.userInputId : "unknown";
      const answerTurnId = currentTurnId ?? "none";
      respond(id, { commandId: frame.id, status: "accepted", userInputId });
      notify("userInput/settled", {
        answers: Array.isArray(params.answers) ? params.answers : [],
        clarification: null,
        decidedByCommandId: frame.id,
        outcome: "answered",
        reason: null,
        sessionId: fixedSessionId,
        sourceRange: mockRange(),
        userInputId,
        viewCursor: nextCursor(),
      });
      emitTurnTail(answerTurnId, "completed");
      return;
    }
    case "session/setModel": {
      respond(id, { commandId: frame.id, status: "accepted" });
      return;
    }
    case "session/setReasoningEffort": {
      respond(id, { commandId: frame.id, status: "accepted" });
      return;
    }
    case "turn/steer": {
      const expected = typeof params.expectedTurnId === "string" ? params.expectedTurnId : "none";
      respond(id, { commandId: frame.id, status: "accepted", turnId: expected });
      return;
    }
    case "turn/interrupt": {
      const target = typeof params.turnId === "string" ? params.turnId : (currentTurnId ?? "none");
      respond(id, { commandId: frame.id, status: "accepted", turnId: target });
      if (target !== "none") {
        emitTurnHead(target);
        emitTurnTail(target, "cancelled");
      }
      return;
    }
    default: {
      respondError(id, -32601, `mock host has no method '${method}'`);
    }
  }
});
