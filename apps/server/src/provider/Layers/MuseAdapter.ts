/**
 * MuseAdapter — Muse CLI (`muse serve`) via MSP (Muse Session Protocol).
 *
 * One `muse serve` host process per T3 thread session, mirroring the
 * per-session child layout of the sibling ACP adapters. The host speaks
 * newline-delimited JSON-RPC 2.0 over stdio; every command carries a
 * client-minted UUIDv7 `commandId` for idempotency (tdd SS3.1.1).
 *
 * @module MuseAdapter
 */
import {
  ApprovalRequestId,
  type CanonicalItemType,
  defaultInstanceIdForDriver,
  EventId,
  type MuseSettings,
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  ProviderInstanceId,
  type ProviderSandboxMode,
  type ProviderSession,
  type CanonicalRequestType,
  type RuntimeContentStreamKind,
  RuntimeItemId,
  RuntimeRequestId,
  type RuntimeItemStatus,
  type RuntimeMode,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { makeMuseProtocol, type MuseMessage } from "../muse/MuseProtocol.ts";
import { type MuseAdapterShape } from "../Services/MuseAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = "muse" as ProviderDriverKind;
const MSP_CLIENT_NAME = "t3_code";
const MSP_CLIENT_VERSION = "0.0.0";
const MUSE_RESUME_VERSION = 1 as const;
/**
 * The only model a default Muse session ever runs. Passed explicitly on
 * every session start so the choice never depends on the host or user
 * configuration default.
 */
const MUSE_PINNED_MODEL_ID = "muse-spark-1.3-contributor" as const;

const museWireModelId = (selection: string | undefined): string =>
  selection !== undefined && selection !== "default" ? selection : MUSE_PINNED_MODEL_ID;

/** Closed tier vocabulary the host accepts; anything else is ignored, never sent. */
const MUSE_REASONING_EFFORT_TIERS = new Set([
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
  "ultra",
]);

const parseMuseReasoningEffort = (value: string | undefined): string | undefined =>
  value !== undefined && MUSE_REASONING_EFFORT_TIERS.has(value) ? value : undefined;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseMuseResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== MUSE_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

/** Lean decodes of the MSP results this adapter reads. Unknown fields strip. */
const MspSessionRef = Schema.Struct({ sessionId: Schema.String });
const MspSessionStartResult = Schema.Struct({
  session: MspSessionRef,
  viewCursor: Schema.String,
});
const MspSessionResumeResult = Schema.Struct({
  session: MspSessionRef,
  viewCursor: Schema.String,
});
const MspSessionSetApprovalModeResult = Schema.Unknown;
const MspTurnStartResult = Schema.Struct({
  turnId: Schema.String,
  disposition: Schema.String,
});
const MspTurnSteerResult = Schema.Struct({ turnId: Schema.String });
const MspTurnInterruptResult = Schema.Struct({ turnId: Schema.String });
const MspTurnCompleted = Schema.Struct({
  turnId: Schema.String,
  terminal: Schema.String,
  reason: Schema.optional(Schema.String),
  error: Schema.optional(Schema.Struct({ message: Schema.String })),
  usage: Schema.optional(Schema.Unknown),
});
const MspItem = Schema.Struct({
  itemId: Schema.String,
  kind: Schema.String,
  status: Schema.optional(Schema.String),
  text: Schema.optional(Schema.String),
  tool: Schema.optional(Schema.String),
  args: Schema.optional(Schema.String),
  visibleOutput: Schema.optional(Schema.String),
  commandText: Schema.optional(Schema.String),
  exitCode: Schema.optional(Schema.Number),
  fallbackText: Schema.optional(Schema.String),
  turnId: Schema.optional(Schema.NullOr(Schema.String)),
});
type MspItem = typeof MspItem.Type;
const MspItemEvent = Schema.Struct({ item: MspItem });
const MspItemDelta = Schema.Struct({
  itemId: Schema.String,
  delta: Schema.String,
  field: Schema.optional(Schema.String),
});
const MspApprovalChoice = Schema.Struct({
  choiceId: Schema.String,
  decision: Schema.String,
  label: Schema.String,
  scope: Schema.String,
});
const MspApprovalSubject = Schema.Struct({
  kind: Schema.String,
  command: Schema.optional(Schema.String),
  path: Schema.optional(Schema.String),
  target: Schema.optional(Schema.String),
  toolName: Schema.optional(Schema.String),
  access: Schema.optional(Schema.String),
});
const MspApprovalRequest = Schema.Struct({
  approvalId: Schema.String,
  availableChoices: Schema.Array(MspApprovalChoice),
  currentRequirementId: Schema.Unknown,
  subject: MspApprovalSubject,
  rawArgs: Schema.optional(Schema.String),
  toolName: Schema.optional(Schema.String),
  turnId: Schema.optional(Schema.String),
});
const MspApprovalDecideResult = Schema.Struct({ terminal: Schema.Boolean });
const MspApprovalResolved = Schema.Struct({ approvalId: Schema.String });
const MspApprovalUpdated = Schema.Struct({
  approvalId: Schema.String,
  availableChoices: Schema.Array(MspApprovalChoice),
  currentRequirementId: Schema.Unknown,
  subject: MspApprovalSubject,
});
const MspUserInputOption = Schema.Struct({
  label: Schema.String,
  description: Schema.optional(Schema.String),
});
const MspUserInputQuestion = Schema.Struct({
  header: Schema.String,
  id: Schema.String,
  options: Schema.Array(MspUserInputOption),
  question: Schema.String,
  selection: Schema.Struct({ mode: Schema.String }),
});
const MspUserInputRequest = Schema.Struct({
  userInputId: Schema.String,
  questions: Schema.Array(MspUserInputQuestion),
  turnId: Schema.optional(Schema.String),
});
const MspUserInputSettledAnswer = Schema.Struct({
  questionId: Schema.String,
  selectedLabel: Schema.optional(Schema.String),
  selectedLabels: Schema.optional(Schema.Array(Schema.String)),
  freeText: Schema.optional(Schema.String),
});
const MspUserInputSettled = Schema.Struct({
  userInputId: Schema.String,
  answers: Schema.Array(MspUserInputSettledAnswer),
});
const decodeMspTurnCompleted = Schema.decodeUnknownEffect(MspTurnCompleted);
const decodeMspItemEvent = Schema.decodeUnknownEffect(MspItemEvent);
const decodeMspItemDelta = Schema.decodeUnknownEffect(MspItemDelta);
const decodeMspApprovalRequest = Schema.decodeUnknownEffect(MspApprovalRequest);
const decodeMspApprovalUpdated = Schema.decodeUnknownEffect(MspApprovalUpdated);
const decodeMspApprovalResolved = Schema.decodeUnknownEffect(MspApprovalResolved);
const decodeMspUserInputRequest = Schema.decodeUnknownEffect(MspUserInputRequest);
const decodeMspUserInputSettled = Schema.decodeUnknownEffect(MspUserInputSettled);

interface MspTurnCompletion {
  readonly terminal: string;
  readonly reason?: string;
  readonly errorMessage?: string;
  readonly usage?: unknown;
}

/** Structural handle over `makeMuseProtocol` so sessions can hold it. */
interface MspProtocolHandle {
  readonly request: <A>(
    method: string,
    params: unknown,
    schema: Schema.Codec<A, unknown>,
  ) => Effect.Effect<A, ProviderAdapterRequestError>;
  readonly notify: (method: string) => Effect.Effect<void, ProviderAdapterRequestError>;
}

/**
 * Host-wide sandbox posture for `muse serve`. The sandbox itself stays on
 * unless the session explicitly asks for full access; read-only sessions
 * lose both non-shell writes and shell execution. Every posture trusts the
 * session workspace, so each thread loads its skills and rules without a
 * per-thread trust prompt; trust only affects instruction loading, never
 * the approval gate.
 */
export const museServeArgs = (
  sandboxMode: ProviderSandboxMode | undefined,
  runtimeMode: RuntimeMode,
): ReadonlyArray<string> => {
  // An explicit restriction always wins. Otherwise Full access means no
  // host sandbox, matching the sibling runtime-mode translation
  // (CodexSessionRuntime): the T3 approval flow, not the host sandbox,
  // is the gate. No producer ever sets sandboxMode, so keying posture
  // off it alone would sandbox every real thread.
  if (sandboxMode === "read-only") {
    return ["serve", "--trust-workspace", "--disable-write", "--disable-shell"];
  }
  if (sandboxMode === "danger-full-access" || runtimeMode === "full-access") {
    return ["serve", "--trust-workspace", "--disable-sandbox"];
  }
  return ["serve", "--trust-workspace"];
};

/**
 * Whether the session may approve tool calls without asking. Only
 * `full-access` sessions auto-allow; every other runtime mode leaves the
 * host's default prompt-first enforcement in place.
 */
const museAutoAllow = (runtimeMode: RuntimeMode): boolean => runtimeMode === "full-access";

/**
 * Item kinds that never reach the transcript. Reminder children are
 * session-internal nudges with their own drill-down, not work the user
 * asked about, so emitting them renders junk tool rows.
 */
const isMspHiddenItemKind = (kind: string | undefined): boolean => kind === "reminderChild";

const mspItemType = (kind: string): CanonicalItemType => {
  switch (kind) {
    case "userMessage":
      return "user_message";
    case "agentMessage":
      return "assistant_message";
    case "reasoning":
      return "reasoning";
    case "userShell":
      return "command_execution";
    case "toolCall":
    case "subagent":
    case "workflow":
    case "reminderChild":
      return "dynamic_tool_call";
    case "compaction":
      return "context_compaction";
    default:
      return "unknown";
  }
};

const trimmedOrUndefined = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
};

const firstLineOrUndefined = (value: string | undefined): string | undefined => {
  const line = trimmedOrUndefined(value)?.split("\n").at(0)?.trim();
  return line ? line : undefined;
};

/** Model-authored args are verbatim, occasionally almost-JSON. Never throws. */
const tryParseMspArgs = (args: string | undefined): Record<string, unknown> | undefined => {
  if (!args?.trim()) return undefined;
  try {
    const parsed: unknown = JSON.parse(args);
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
};

const stringFieldOrUndefined = (
  record: Record<string, unknown> | undefined,
  key: string,
): string | undefined => {
  const value = record?.[key];
  return typeof value === "string" ? trimmedOrUndefined(value) : undefined;
};

/** Shell command behind a toolCall/userShell item, when the wire names one. */
const mspItemShellCommand = (item: MspItem): string | undefined => {
  if (item.kind === "userShell") return trimmedOrUndefined(item.commandText);
  if (item.kind !== "toolCall") return undefined;
  const args = tryParseMspArgs(item.args);
  for (const key of ["command", "cmd", "script"]) {
    const command = stringFieldOrUndefined(args, key);
    if (command) return command;
  }
  return undefined;
};

const mspItemTitle = (item: MspItem): string | undefined => {
  const shellCommand = mspItemShellCommand(item);
  if (item.kind === "userShell") {
    return (shellCommand ? firstLineOrUndefined(shellCommand) : undefined) ?? "Shell command";
  }
  if (item.kind === "toolCall") {
    const tool = trimmedOrUndefined(item.tool) ?? "Tool";
    const firstLine = shellCommand ? firstLineOrUndefined(shellCommand) : undefined;
    return (firstLine ? `${tool}: ${firstLine}` : tool).slice(0, 200);
  }
  return item.tool?.slice(0, 200);
};

const mspItemDetail = (item: MspItem): string | undefined => {
  if (item.kind !== "toolCall" && item.kind !== "userShell") return item.fallbackText;
  const blocks: Array<string> = [];
  const parsedArgs = tryParseMspArgs(item.args);
  if (parsedArgs) {
    blocks.push(JSON.stringify(parsedArgs, null, 2));
  } else {
    const rawArgs = trimmedOrUndefined(item.args);
    if (rawArgs) blocks.push(rawArgs);
  }
  const output = trimmedOrUndefined(item.visibleOutput);
  if (output) blocks.push(output);
  if (item.kind === "userShell" && item.exitCode !== undefined) {
    blocks.push(`<exited with exit code ${item.exitCode}>`);
  }
  const detail = blocks.length > 0 ? blocks.join("\n\n").slice(0, 2000) : undefined;
  return detail ?? item.fallbackText;
};

/**
 * Structured data the timeline reads: `item.command` feeds the expanded
 * command body, `item.input`/`rawInput` feed path and search extraction.
 */
const mspItemData = (item: MspItem): Record<string, unknown> | undefined => {
  if (item.kind !== "toolCall" && item.kind !== "userShell") return undefined;
  const shellCommand = mspItemShellCommand(item);
  const parsedArgs = tryParseMspArgs(item.args);
  if (!shellCommand && !parsedArgs) return undefined;
  return {
    item: {
      ...(shellCommand ? { command: shellCommand } : {}),
      ...(parsedArgs ? { input: parsedArgs } : {}),
    },
    ...(parsedArgs ? { rawInput: parsedArgs } : {}),
  };
};

const mspItemStatus = (status: string | undefined): RuntimeItemStatus => {
  switch (status) {
    case "inProgress":
      return "inProgress";
    case "failed":
    case "cancelled":
    case "timedOut":
      return "failed";
    case "rejected":
      return "declined";
    default:
      return "completed";
  }
};

const mspStreamKind = (
  kind: string | undefined,
  field: string | undefined,
): RuntimeContentStreamKind => {
  if (field?.startsWith("summary")) return "reasoning_summary_text";
  switch (kind) {
    case "agentMessage":
      return "assistant_text";
    case "reasoning":
      return "reasoning_text";
    case "toolCall":
    case "userShell":
      return "command_output";
    default:
      return "unknown";
  }
};

const mspTurnState = (terminal: string): "completed" | "failed" | "cancelled" => {
  switch (terminal) {
    case "completed":
      return "completed";
    case "cancelled":
      return "cancelled";
    default:
      return "failed";
  }
};

const mspRequestType = (kind: string, access: string | undefined): CanonicalRequestType => {
  switch (kind) {
    case "shell":
      return "command_execution_approval";
    case "process":
      return "exec_command_approval";
    case "fileAccess":
      return access === "read" ? "file_read_approval" : "file_change_approval";
    case "tool":
    case "network":
    case "unixSocket":
      return "dynamic_tool_call";
    default:
      return "unknown";
  }
};

const mspApprovalDetail = (input: {
  readonly kind: string;
  readonly command?: string;
  readonly path?: string;
  readonly target?: string;
  readonly toolName?: string;
  readonly rawArgs?: string;
}): string => {
  const target =
    input.command ?? input.path ?? input.target ?? input.toolName ?? "approval request";
  const base = `${input.kind}: ${target}`;
  return (input.rawArgs ? `${base} ${input.rawArgs}` : base).slice(0, 2000);
};

const mspChoiceDecisionToT3 = (decision: string): ProviderApprovalDecision => {
  switch (decision) {
    case "approved":
      return "accept";
    case "approvedForSession":
      return "acceptForSession";
    case "approvedPolicyAmendment":
      return "acceptAlways";
    case "denied":
    case "deniedPolicyAmendment":
      return "decline";
    default:
      return "cancel";
  }
};

/**
 * Pick the host choice a T3 decision names. Matching stays strict on the
 * decision family: an approval never silently escalates to a wider scope
 * or degrades to a denial, and a denial never becomes an approval.
 */
const selectMspChoice = (
  choices: ReadonlyArray<MspApprovalChoice>,
  decision: ProviderApprovalDecision,
): MspApprovalChoice | undefined => {
  const wanted: ReadonlyArray<string> =
    decision === "accept"
      ? ["approved"]
      : decision === "acceptForSession"
        ? ["approvedForSession"]
        : decision === "acceptAlways"
          ? ["approvedPolicyAmendment"]
          : ["denied", "deniedPolicyAmendment"];
  const pool = choices.filter((choice) => wanted.includes(choice.decision));
  return (
    pool.find((choice) => choice.scope === "once") ??
    pool.find((choice) => choice.scope === "session") ??
    pool[0]
  );
};

export interface MuseAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /**
   * Selections are honored when `modelSelection.instanceId` matches this value.
   * Defaults to the legacy built-in instance id (`muse`).
   */
  readonly instanceId?: ProviderInstanceId;
  /**
   * Optional per-session settings resolver. Production leaves this
   * undefined and uses the `museSettings` captured at construction;
   * tests pass a resolver that reads the latest snapshot so mid-suite
   * `updateSettings({ providers: { muse: { binaryPath } } })` calls
   * take effect when the next session spawns.
   */
  readonly resolveSettings?: Effect.Effect<MuseSettings>;
}

interface MspApprovalChoice {
  readonly choiceId: string;
  readonly decision: string;
  readonly label: string;
  readonly scope: string;
}

interface PendingApproval {
  readonly approvalId: string;
  readonly turnId: TurnId | undefined;
  requirementId: unknown;
  choices: ReadonlyArray<MspApprovalChoice>;
  requestType: CanonicalRequestType;
  detail: string;
  args: unknown;
  rawParams: unknown;
  decision: Deferred.Deferred<ProviderApprovalDecision>;
  requirementWaiter: Deferred.Deferred<void> | undefined;
  resolved: boolean;
}

interface MspUserInputOption {
  readonly label: string;
  readonly description?: string;
}

interface MspUserInputQuestion {
  readonly header: string;
  readonly id: string;
  readonly options: ReadonlyArray<MspUserInputOption>;
  readonly question: string;
  readonly multiSelect: boolean;
}

interface PendingUserInput {
  readonly userInputId: string;
  readonly turnId: TurnId | undefined;
  readonly questions: ReadonlyArray<MspUserInputQuestion>;
  settled: boolean;
}

interface MuseSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly protocol: MspProtocolHandle;
  readonly mspSessionId: string;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly turnWaiters: Map<
    string,
    Deferred.Deferred<MspTurnCompletion, ProviderAdapterRequestError>
  >;
  readonly mspTurnToT3: Map<string, TurnId>;
  readonly t3TurnToMsp: Map<TurnId, string>;
  readonly itemKinds: Map<string, string>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  activeMspTurnId: string | undefined;
  /** Last reasoning effort synced to the host, if any. */
  reasoningEffort: string | undefined;
  /** Number of sendTurn prompts currently in flight or being prepared. */
  promptsInFlight: number;
  stopped: boolean;
}

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  const pendingEntries = Array.from(pendingApprovals.values());
  return Effect.forEach(
    pendingEntries,
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    { discard: true },
  );
}

const mspUserInputAnswer = (
  question: MspUserInputQuestion,
  value: unknown,
): Record<string, unknown> => {
  if (Array.isArray(value)) {
    return {
      questionId: question.id,
      selectedLabels: value.filter((entry): entry is string => typeof entry === "string"),
    };
  }
  if (typeof value === "string") {
    return question.options.some((option) => option.label === value)
      ? { questionId: question.id, selectedLabel: value }
      : { questionId: question.id, freeText: value };
  }
  if (value === undefined) return { questionId: question.id };
  return { questionId: question.id, freeText: String(value) };
};

export function makeMuseAdapter(museSettings: MuseSettings, options?: MuseAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? defaultInstanceIdForDriver(PROVIDER);
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const serverConfig = yield* Effect.service(ServerConfig);
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;

    const sessions = new Map<ThreadId, MuseSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextCommandId = crypto.randomUUIDv7.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv7",
            detail: "Failed to generate Muse command identifier.",
            cause,
          }),
      ),
    );
    const randomUUID = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Muse runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUID, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUID,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      });

    const failTurnWaiters = (ctx: MuseSessionContext, error: ProviderAdapterRequestError) =>
      Effect.gen(function* () {
        for (const waiter of ctx.turnWaiters.values()) {
          yield* Deferred.fail(waiter, error).pipe(Effect.ignore);
        }
        ctx.turnWaiters.clear();
      });

    const settlePendingUserInputsAsEmpty = (ctx: MuseSessionContext) =>
      Effect.forEach(
        Array.from(ctx.pendingUserInputs.entries()),
        ([requestId, pending]) =>
          Effect.gen(function* () {
            if (pending.settled) return;
            pending.settled = true;
            ctx.pendingUserInputs.delete(requestId);
            yield* offerRuntimeEvent({
              type: "user-input.resolved",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(pending.turnId !== undefined ? { turnId: pending.turnId } : {}),
              requestId: RuntimeRequestId.make(requestId),
              payload: { answers: {} },
            });
          }),
        { discard: true },
      );

    const handleNotification = (threadId: ThreadId, message: MuseMessage) =>
      Effect.gen(function* () {
        yield* logNative(threadId, message.method, message.params);
        const target = sessions.get(threadId);
        if (!target || target.stopped) return;
        switch (message.method) {
          case "turn/completed": {
            const decoded = yield* decodeMspTurnCompleted(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const completion = decoded.value;
            const waiter = target.turnWaiters.get(completion.turnId);
            target.turnWaiters.delete(completion.turnId);
            if (waiter === undefined) return;
            yield* Deferred.succeed(waiter, {
              terminal: completion.terminal,
              ...(completion.reason !== undefined ? { reason: completion.reason } : {}),
              ...(completion.error?.message !== undefined
                ? { errorMessage: completion.error.message }
                : {}),
              ...(completion.usage !== undefined ? { usage: completion.usage } : {}),
            }).pipe(Effect.ignore);
            return;
          }
          case "item/started":
          case "item/updated":
          case "item/completed": {
            const decoded = yield* decodeMspItemEvent(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const item = decoded.value.item;
            target.itemKinds.set(item.itemId, item.kind);
            if (isMspHiddenItemKind(item.kind)) return;
            const lifecycle =
              message.method === "item/started"
                ? "item.started"
                : message.method === "item/updated"
                  ? "item.updated"
                  : "item.completed";
            const mappedTurnId =
              (item.turnId ? target.mspTurnToT3.get(item.turnId) : undefined) ??
              target.activeTurnId;
            const itemTitle = mspItemTitle(item);
            const itemDetail = mspItemDetail(item);
            const itemData = mspItemData(item);
            yield* offerRuntimeEvent({
              type: lifecycle,
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: target.threadId,
              ...(mappedTurnId !== undefined ? { turnId: mappedTurnId } : {}),
              itemId: RuntimeItemId.make(item.itemId),
              payload: {
                itemType: mspItemType(item.kind),
                status: lifecycle === "item.started" ? "inProgress" : mspItemStatus(item.status),
                ...(itemTitle ? { title: itemTitle } : {}),
                ...(itemDetail ? { detail: itemDetail.slice(0, 2000) } : {}),
                ...(itemData ? { data: itemData } : {}),
              },
              raw: { source: "muse.msp", method: message.method, payload: message.params },
            });
            return;
          }
          case "item/delta": {
            const decoded = yield* decodeMspItemDelta(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const delta = decoded.value;
            if (isMspHiddenItemKind(target.itemKinds.get(delta.itemId))) return;
            yield* offerRuntimeEvent({
              type: "content.delta",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: target.threadId,
              ...(target.activeTurnId !== undefined ? { turnId: target.activeTurnId } : {}),
              itemId: RuntimeItemId.make(delta.itemId),
              payload: {
                streamKind: mspStreamKind(target.itemKinds.get(delta.itemId), delta.field),
                delta: delta.delta,
              },
              raw: { source: "muse.msp", method: message.method, payload: message.params },
            });
            return;
          }
          case "approval/request":
          case "approval/requested": {
            const decoded = yield* decodeMspApprovalRequest(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const request = decoded.value;
            const reissued = Array.from(target.pendingApprovals.values()).find(
              (pending) => pending.approvalId === request.approvalId,
            );
            if (reissued) {
              reissued.requirementId = request.currentRequirementId;
              reissued.choices = request.availableChoices;
              reissued.rawParams = message.params;
              if (reissued.requirementWaiter) {
                yield* Deferred.succeed(reissued.requirementWaiter, undefined).pipe(Effect.ignore);
              }
              return;
            }
            const requestId = ApprovalRequestId.make(yield* randomUUID);
            const mappedTurnId =
              (request.turnId ? target.mspTurnToT3.get(request.turnId) : undefined) ??
              target.activeTurnId;
            const approvalToolName = request.subject.toolName ?? request.toolName;
            target.pendingApprovals.set(requestId, {
              approvalId: request.approvalId,
              turnId: mappedTurnId,
              requirementId: request.currentRequirementId,
              choices: request.availableChoices,
              requestType: mspRequestType(request.subject.kind, request.subject.access),
              detail: mspApprovalDetail({
                kind: request.subject.kind,
                ...(request.subject.command !== undefined
                  ? { command: request.subject.command }
                  : {}),
                ...(request.subject.path !== undefined ? { path: request.subject.path } : {}),
                ...(request.subject.target !== undefined ? { target: request.subject.target } : {}),
                ...(approvalToolName !== undefined ? { toolName: approvalToolName } : {}),
                ...(request.rawArgs !== undefined ? { rawArgs: request.rawArgs } : {}),
              }),
              args: {
                ...(request.rawArgs !== undefined ? { rawArgs: request.rawArgs } : {}),
                ...(request.toolName !== undefined ? { toolName: request.toolName } : {}),
              },
              rawParams: message.params,
              decision: yield* Deferred.make<ProviderApprovalDecision>(),
              requirementWaiter: undefined,
              resolved: false,
            });
            yield* runApprovalRound(target, requestId).pipe(
              Effect.forkIn(target.scope),
              Effect.asVoid,
            );
            return;
          }
          case "approval/updated": {
            const decoded = yield* decodeMspApprovalUpdated(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const updated = decoded.value;
            for (const pending of target.pendingApprovals.values()) {
              if (pending.approvalId !== updated.approvalId || pending.resolved) continue;
              pending.requirementId = updated.currentRequirementId;
              pending.choices = updated.availableChoices;
              pending.requestType = mspRequestType(updated.subject.kind, updated.subject.access);
              if (pending.requirementWaiter) {
                yield* Deferred.succeed(pending.requirementWaiter, undefined).pipe(Effect.ignore);
              }
            }
            return;
          }
          case "approval/resolved": {
            const decoded = yield* decodeMspApprovalResolved(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            yield* settleApprovalFromHost(target, decoded.value.approvalId);
            return;
          }
          case "userInput/request":
          case "userInput/requested": {
            const decoded = yield* decodeMspUserInputRequest(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const request = decoded.value;
            const duplicate = Array.from(target.pendingUserInputs.values()).some(
              (pending) => pending.userInputId === request.userInputId,
            );
            if (duplicate) return;
            const requestId = ApprovalRequestId.make(yield* randomUUID);
            const mappedTurnId =
              (request.turnId ? target.mspTurnToT3.get(request.turnId) : undefined) ??
              target.activeTurnId;
            const questions: ReadonlyArray<MspUserInputQuestion> = request.questions.map(
              (question) => ({
                header: question.header,
                id: question.id,
                options: question.options.map((option) => ({
                  label: option.label,
                  ...(option.description !== undefined ? { description: option.description } : {}),
                })),
                question: question.question,
                multiSelect: question.selection.mode === "multiple",
              }),
            );
            target.pendingUserInputs.set(requestId, {
              userInputId: request.userInputId,
              turnId: mappedTurnId,
              questions,
              settled: false,
            });
            yield* offerRuntimeEvent({
              type: "user-input.requested",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: target.threadId,
              ...(mappedTurnId !== undefined ? { turnId: mappedTurnId } : {}),
              requestId: RuntimeRequestId.make(requestId),
              payload: {
                questions: questions.map((question) => ({
                  id: question.id,
                  header: question.header,
                  question: question.question,
                  options: question.options.map((option) => ({
                    label: option.label,
                    description:
                      option.description?.trim() && option.description.trim().length > 0
                        ? option.description
                        : option.label,
                  })),
                  multiSelect: question.multiSelect,
                })),
              },
              raw: { source: "muse.msp", method: "userInput/request", payload: message.params },
            });
            return;
          }
          case "userInput/settled": {
            const decoded = yield* decodeMspUserInputSettled(message.params).pipe(Effect.option);
            if (Option.isNone(decoded)) return;
            const settled = decoded.value;
            for (const [requestId, pending] of target.pendingUserInputs) {
              if (pending.userInputId !== settled.userInputId || pending.settled) continue;
              pending.settled = true;
              target.pendingUserInputs.delete(requestId);
              const answers: Record<string, unknown> = {};
              for (const answer of settled.answers) {
                answers[answer.questionId] =
                  answer.selectedLabel ?? answer.selectedLabels ?? answer.freeText ?? null;
              }
              yield* offerRuntimeEvent({
                type: "user-input.resolved",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: target.threadId,
                ...(pending.turnId !== undefined ? { turnId: pending.turnId } : {}),
                requestId: RuntimeRequestId.make(requestId),
                payload: { answers },
              });
            }
            return;
          }
          default:
            return;
        }
      }).pipe(
        Effect.catch((cause) => Effect.logError("Failed to process Muse notification.", { cause })),
      );

    const emitApprovalResolved = (
      ctx: MuseSessionContext,
      requestId: ApprovalRequestId,
      pending: Pick<PendingApproval, "requestType" | "turnId">,
      resolution: { readonly decision?: ProviderApprovalDecision; readonly choiceId?: string },
    ) =>
      Effect.flatMap(makeEventStamp(), (stamp) =>
        offerRuntimeEvent({
          type: "request.resolved",
          ...stamp,
          provider: PROVIDER,
          threadId: ctx.threadId,
          ...(pending.turnId !== undefined ? { turnId: pending.turnId } : {}),
          requestId: RuntimeRequestId.make(requestId),
          payload: {
            requestType: pending.requestType,
            ...(resolution.decision !== undefined ? { decision: resolution.decision } : {}),
            ...(resolution.choiceId !== undefined
              ? { resolution: { choiceId: resolution.choiceId } }
              : { resolution: { hostResolved: true } }),
          },
        }),
      );

    const runApprovalRound = (ctx: MuseSessionContext, requestId: ApprovalRequestId) =>
      Effect.gen(function* () {
        while (true) {
          if (ctx.stopped) return;
          const opened = ctx.pendingApprovals.get(requestId);
          if (!opened || opened.resolved) {
            if (opened) {
              opened.resolved = true;
              ctx.pendingApprovals.delete(requestId);
              yield* emitApprovalResolved(ctx, requestId, opened, {});
            }
            return;
          }
          const decisionDeferred = yield* Deferred.make<ProviderApprovalDecision>();
          opened.decision = decisionDeferred;
          opened.requirementWaiter = undefined;
          yield* offerRuntimeEvent({
            type: "request.opened",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            ...(opened.turnId !== undefined ? { turnId: opened.turnId } : {}),
            requestId: RuntimeRequestId.make(requestId),
            payload: {
              requestType: opened.requestType,
              detail: opened.detail,
              options: opened.choices.map((choice) => ({
                decision: mspChoiceDecisionToT3(choice.decision),
                label: choice.label,
              })),
              args: opened.args,
            },
            raw: { source: "muse.msp", method: "approval/request", payload: opened.rawParams },
          });
          const decision = yield* Deferred.await(decisionDeferred);
          const current = ctx.pendingApprovals.get(requestId);
          if (!current || current.resolved || ctx.stopped) {
            if (current && !ctx.stopped) {
              current.resolved = true;
              ctx.pendingApprovals.delete(requestId);
              yield* emitApprovalResolved(ctx, requestId, current, {});
            }
            return;
          }
          const choice = selectMspChoice(current.choices, decision);
          if (!choice) {
            current.resolved = true;
            ctx.pendingApprovals.delete(requestId);
            yield* Effect.logError("No Muse approval choice matches the T3 decision.", {
              approvalId: current.approvalId,
              decision,
            });
            yield* emitApprovalResolved(ctx, requestId, current, { decision });
            return;
          }
          const result = yield* ctx.protocol.request(
            "approval/decide",
            {
              approvalId: current.approvalId,
              choiceId: choice.choiceId,
              commandId: yield* nextCommandId,
              requirementId: current.requirementId,
              sessionId: ctx.mspSessionId,
            },
            MspApprovalDecideResult,
          );
          const afterDecide = ctx.pendingApprovals.get(requestId);
          if (!afterDecide || afterDecide.resolved || ctx.stopped) {
            if (afterDecide && !ctx.stopped) {
              afterDecide.resolved = true;
              ctx.pendingApprovals.delete(requestId);
              yield* emitApprovalResolved(ctx, requestId, afterDecide, {});
            }
            return;
          }
          if (result.terminal) {
            afterDecide.resolved = true;
            ctx.pendingApprovals.delete(requestId);
            yield* emitApprovalResolved(ctx, requestId, afterDecide, {
              decision,
              choiceId: choice.choiceId,
            });
            return;
          }
          const waiter = yield* Deferred.make<void>();
          afterDecide.requirementWaiter = waiter;
          yield* Deferred.await(waiter);
        }
      });

    const settleApprovalFromHost = (ctx: MuseSessionContext, approvalId: string) =>
      Effect.gen(function* () {
        for (const pending of ctx.pendingApprovals.values()) {
          if (pending.approvalId !== approvalId || pending.resolved) continue;
          pending.resolved = true;
          yield* Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore);
          if (pending.requirementWaiter) {
            yield* Deferred.succeed(pending.requirementWaiter, undefined).pipe(Effect.ignore);
          }
        }
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<MuseSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: MuseSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsEmpty(ctx);
        yield* failTurnWaiters(
          ctx,
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "turn/start",
            detail: "Muse session stopped.",
          }),
        );
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: MuseAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const museModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
          const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );
          let ctx: MuseSessionContext | undefined;

          const resumeSessionId = parseMuseResume(input.resumeCursor)?.sessionId;
          const effectiveMuseSettings = options?.resolveSettings
            ? yield* options.resolveSettings
            : museSettings;

          const protocol = yield* makeMuseProtocol({
            command: effectiveMuseSettings.binaryPath,
            args: museServeArgs(input.sandboxMode, input.runtimeMode),
            cwd,
            ...(options?.environment ? { environment: options.environment } : {}),
            onMessage: (message) => handleNotification(input.threadId, message),
            onExit: (error) =>
              Effect.gen(function* () {
                if (!ctx || ctx.stopped) return;
                ctx.stopped = true;
                yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
                yield* settlePendingUserInputsAsEmpty(ctx);
                yield* failTurnWaiters(ctx, error);
                sessions.delete(ctx.threadId);
                yield* offerRuntimeEvent({
                  type: "session.exited",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  payload: { exitKind: "error", reason: error.detail },
                });
              }).pipe(
                Effect.catch((cause) =>
                  Effect.logWarning("Failed to handle Muse host exit.", { cause }),
                ),
              ),
          }).pipe(
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
          );

          yield* protocol.request(
            "initialize",
            {
              clientInfo: {
                name: MSP_CLIENT_NAME,
                title: "T3 Code",
                version: MSP_CLIENT_VERSION,
              },
            },
            Schema.Unknown,
          );
          // LSP-style second handshake step: the host serves session
          // methods only after the client announces `initialized`.
          yield* protocol.notify("initialized");

          // A stored cursor reopens the durable host session so a
          // thread restarted after a server reboot keeps its agent
          // context. History stays server-side; T3 already persists
          // the transcript. When the host cannot load the session
          // (pruned, stale id), fall back to a fresh session instead
          // of failing the thread restart.
          let mspSessionId: string | undefined;
          if (resumeSessionId !== undefined) {
            const resumedExit = yield* Effect.exit(
              protocol.request(
                "session/resume",
                {
                  commandId: yield* nextCommandId,
                  sessionId: resumeSessionId,
                  excludeItems: true,
                },
                MspSessionResumeResult,
              ),
            );
            if (Exit.isSuccess(resumedExit)) {
              mspSessionId = resumedExit.value.session.sessionId;
            } else {
              yield* Effect.logWarning("Muse session resume failed; starting a fresh session.", {
                cause: resumedExit.cause,
              });
            }
          }
          // No approvalMode here: the host rejects explicit modes
          // above its sealed start ceiling, so full-access sessions
          // switch to allowAll with session/setApprovalMode below.
          mspSessionId ??= (yield* protocol.request(
            "session/start",
            {
              commandId: yield* nextCommandId,
              workspaceRoot: cwd,
              modelId: museWireModelId(museModelSelection?.model),
            },
            MspSessionStartResult,
          )).session.sessionId;

          if (museAutoAllow(input.runtimeMode)) {
            yield* protocol.request(
              "session/setApprovalMode",
              {
                commandId: yield* nextCommandId,
                sessionId: mspSessionId,
                mode: "allowAll",
              },
              MspSessionSetApprovalModeResult,
            );
          }

          const startEffort = parseMuseReasoningEffort(
            getModelSelectionStringOptionValue(museModelSelection, "reasoningEffort"),
          );
          if (startEffort !== undefined) {
            yield* protocol.request(
              "session/setReasoningEffort",
              {
                commandId: yield* nextCommandId,
                sessionId: mspSessionId,
                reasoningEffort: startEffort,
              },
              Schema.Unknown,
            );
          }

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            model: museModelSelection?.model,
            threadId: input.threadId,
            createdAt: now,
            updatedAt: now,
            resumeCursor: {
              schemaVersion: MUSE_RESUME_VERSION,
              sessionId: mspSessionId,
            },
          };

          const started: MuseSessionContext = {
            threadId: input.threadId,
            session,
            scope: sessionScope,
            protocol,
            mspSessionId,
            pendingApprovals,
            pendingUserInputs,
            turnWaiters: new Map(),
            mspTurnToT3: new Map(),
            t3TurnToMsp: new Map(),
            itemKinds: new Map(),
            turns: [],
            activeTurnId: undefined,
            activeMspTurnId: undefined,
            reasoningEffort: startEffort,
            promptsInFlight: 0,
            stopped: false,
          };
          ctx = started;
          sessions.set(input.threadId, started);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: { sessionId: mspSessionId } },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Muse MSP session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: mspSessionId },
          });

          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: MuseAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        // Submit under the thread lock so a concurrent steer always sees
        // the recorded MSP turn id; the completion wait runs outside it.
        const submitted = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
            const turnId = steeringTurnId ?? TurnId.make(yield* randomUUID);
            ctx.promptsInFlight += 1;

            const parts: Array<Record<string, unknown>> = [];
            if (input.input?.trim()) {
              parts.push({ type: "text", text: input.input.trim() });
            }
            if (input.attachments && input.attachments.length > 0) {
              for (const attachment of input.attachments) {
                // Muse ingests images only. Generic files reach the agent
                // through the path line ProviderService puts in the prompt.
                if (attachment.type !== "image") continue;
                const attachmentPath = resolveAttachmentPath({
                  attachmentsDir: serverConfig.attachmentsDir,
                  attachment,
                });
                if (!attachmentPath) {
                  return yield* new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "turn/start",
                    detail: `Invalid attachment id '${attachment.id}'.`,
                  });
                }
                const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "turn/start",
                        detail: cause.message,
                        cause,
                      }),
                  ),
                );
                parts.push({
                  type: "image",
                  base64Data: Buffer.from(bytes).toString("base64"),
                  mediaType: attachment.mimeType,
                });
              }
            }
            if (parts.length === 0) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "sendTurn",
                issue: "Turn requires non-empty text or attachments.",
              });
            }

            const turnModelSelection =
              input.modelSelection?.instanceId === boundInstanceId
                ? input.modelSelection
                : undefined;
            const requestedModel = turnModelSelection?.model;
            const requestedWireModel =
              requestedModel !== undefined ? museWireModelId(requestedModel) : undefined;
            if (
              requestedWireModel !== undefined &&
              requestedWireModel !== museWireModelId(ctx.session.model)
            ) {
              yield* ctx.protocol.request(
                "session/setModel",
                {
                  commandId: yield* nextCommandId,
                  model: { modelId: requestedWireModel },
                  sessionId: ctx.mspSessionId,
                },
                Schema.Unknown,
              );
              ctx.session = {
                ...ctx.session,
                model: requestedModel,
                updatedAt: yield* nowIso,
              };
            }
            const requestedEffort = parseMuseReasoningEffort(
              getModelSelectionStringOptionValue(turnModelSelection, "reasoningEffort"),
            );
            if (requestedEffort !== undefined && requestedEffort !== ctx.reasoningEffort) {
              yield* ctx.protocol.request(
                "session/setReasoningEffort",
                {
                  commandId: yield* nextCommandId,
                  sessionId: ctx.mspSessionId,
                  reasoningEffort: requestedEffort,
                },
                Schema.Unknown,
              );
              ctx.reasoningEffort = requestedEffort;
            }

            ctx.activeTurnId = turnId;
            ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };
            if (steeringTurnId === undefined) {
              yield* offerRuntimeEvent({
                type: "turn.started",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: { model: ctx.session.model },
              });
            }

            const mspTurnId =
              steeringTurnId !== undefined && ctx.activeMspTurnId !== undefined
                ? (yield* ctx.protocol.request(
                    "turn/steer",
                    {
                      commandId: yield* nextCommandId,
                      expectedTurnId: ctx.activeMspTurnId,
                      input: parts,
                      sessionId: ctx.mspSessionId,
                    },
                    MspTurnSteerResult,
                  )).turnId
                : (yield* ctx.protocol.request(
                    "turn/start",
                    {
                      commandId: yield* nextCommandId,
                      input: parts,
                      sessionId: ctx.mspSessionId,
                    },
                    MspTurnStartResult,
                  )).turnId;
            ctx.activeMspTurnId = mspTurnId;
            ctx.mspTurnToT3.set(mspTurnId, turnId);
            ctx.t3TurnToMsp.set(turnId, mspTurnId);
            let waiter = ctx.turnWaiters.get(mspTurnId);
            if (!waiter) {
              waiter = yield* Deferred.make<MspTurnCompletion, ProviderAdapterRequestError>();
              ctx.turnWaiters.set(mspTurnId, waiter);
            }
            return { turnId, waiter };
          }),
        ).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
            }),
          ),
        );

        return yield* Effect.gen(function* () {
          const completion = yield* Deferred.await(submitted.waiter);
          const entry = {
            input: input.input,
            mspTurnId: ctx.t3TurnToMsp.get(submitted.turnId),
            terminal: completion.terminal,
          };
          const turnRecord = ctx.turns.find((turn) => turn.id === submitted.turnId);
          if (turnRecord) turnRecord.items.push(entry);
          else ctx.turns.push({ id: submitted.turnId, items: [entry] });
          ctx.session = {
            ...ctx.session,
            activeTurnId: submitted.turnId,
            updatedAt: yield* nowIso,
          };

          // Only the last remaining prompt settles the turn — a steer-
          // superseded prompt resolving while another is in flight must
          // leave the merged turn running.
          if (ctx.promptsInFlight === 1) {
            yield* offerRuntimeEvent({
              type: "turn.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId: submitted.turnId,
              payload: {
                state: mspTurnState(completion.terminal),
                stopReason: completion.reason ?? completion.errorMessage ?? null,
                ...(completion.errorMessage !== undefined
                  ? { errorMessage: completion.errorMessage }
                  : {}),
                ...(completion.usage !== undefined ? { usage: completion.usage } : {}),
              },
            });
          }

          return {
            threadId: input.threadId,
            turnId: submitted.turnId,
            resumeCursor: ctx.session.resumeCursor,
          };
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
            }),
          ),
        );
      });

    const interruptTurn: MuseAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsEmpty(ctx);
        const mspTurnId = (turnId ? ctx.t3TurnToMsp.get(turnId) : undefined) ?? ctx.activeMspTurnId;
        yield* Effect.ignore(
          ctx.protocol.request(
            "turn/interrupt",
            {
              commandId: yield* nextCommandId,
              sessionId: ctx.mspSessionId,
              ...(mspTurnId !== undefined ? { turnId: mspTurnId } : {}),
            },
            MspTurnInterruptResult,
          ),
        );
      });

    const respondToRequest: MuseAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending || pending.resolved) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "approval/decide",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: MuseAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUserInputs.get(requestId);
        if (!pending || pending.settled) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "userInput/answer",
            detail: `Unknown pending user-input request: ${requestId}`,
          });
        }
        yield* ctx.protocol.request(
          "userInput/answer",
          {
            answers: pending.questions.map((question) =>
              mspUserInputAnswer(question, answers[question.id]),
            ),
            commandId: yield* nextCommandId,
            sessionId: ctx.mspSessionId,
            userInputId: pending.userInputId,
          },
          Schema.Unknown,
        );
        pending.settled = true;
        ctx.pendingUserInputs.delete(requestId);
        yield* offerRuntimeEvent({
          type: "user-input.resolved",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          ...(pending.turnId !== undefined ? { turnId: pending.turnId } : {}),
          requestId: RuntimeRequestId.make(requestId),
          payload: { answers },
        });
      });

    const readThread: MuseAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: MuseAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        const nextLength = Math.max(0, ctx.turns.length - numTurns);
        ctx.turns.splice(nextLength);
        return { threadId, turns: ctx.turns };
      });

    const stopSession: MuseAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: MuseAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: MuseAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: MuseAdapterShape["stopAll"] = () =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true }).pipe(
        Effect.catch((cause) =>
          Effect.logError("Failed to emit Muse session shutdown event.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies MuseAdapterShape;
  });
}
