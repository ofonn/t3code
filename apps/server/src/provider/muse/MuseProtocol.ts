import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { ProviderAdapterRequestError } from "../Errors.ts";

const Frame = Schema.Struct({
  id: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  method: Schema.optional(Schema.String),
  params: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Struct({ message: Schema.String })),
});
const JsonFrame = Schema.fromJsonString(Frame);
const JsonUnknown = Schema.fromJsonString(Schema.Unknown);
const encodeJsonFrame = Schema.encodeEffect(JsonUnknown);
const decodeJsonFrame = Schema.decodeEffect(JsonFrame);
export type MuseMessage = { readonly method: string; readonly params?: unknown };
export const museError = (method: string, detail: string, cause?: unknown) =>
  new ProviderAdapterRequestError({ provider: "muse", method, detail, cause });

/** MSP v1 uses newline-delimited JSON-RPC. The scope owns the child and all readers. */
export const makeMuseProtocol = Effect.fn("makeMuseProtocol")(function* (options: {
  command: string;
  args: ReadonlyArray<string>;
  cwd: string;
  environment?: NodeJS.ProcessEnv;
  onMessage: (message: MuseMessage) => Effect.Effect<void>;
  onExit: (error: ProviderAdapterRequestError) => Effect.Effect<void>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner
    .spawn(
      ChildProcess.make(options.command, options.args, {
        cwd: options.cwd,
        ...(options.environment ? { env: options.environment } : {}),
      }),
    )
    .pipe(
      Effect.mapError((cause) =>
        museError("spawn", "Could not start Muse. Check its executable path.", cause),
      ),
    );
  const outgoing = yield* Queue.unbounded<Uint8Array>();
  const pending = new Map<number, Deferred.Deferred<unknown, ProviderAdapterRequestError>>();
  let sequence = 0;
  let closed = false;
  const close = (error: ProviderAdapterRequestError) =>
    Effect.gen(function* () {
      if (closed) return;
      closed = true;
      for (const waiter of pending.values()) yield* Deferred.fail(waiter, error);
      pending.clear();
      yield* options.onExit(error);
    });
  yield* Effect.addFinalizer(() => close(museError("transport", "Muse connection closed.")));
  const send = (frame: unknown) =>
    Effect.gen(function* () {
      if (closed) return yield* museError("transport", "Muse connection closed.");
      const json = yield* encodeJsonFrame(frame).pipe(
        Effect.mapError((cause) => museError("encode", "Could not encode Muse request.", cause)),
      );
      yield* Queue.offer(outgoing, new TextEncoder().encode(`${json}\n`));
    });
  const handleLine = (line: string) =>
    Effect.gen(function* () {
      const text = line.replace(/\r$/, "");
      if (!text.trim()) return;
      const frame = yield* decodeJsonFrame(text).pipe(
        Effect.mapError((cause) =>
          museError("decode", "Muse returned an invalid JSON-RPC frame.", cause),
        ),
      );
      if (frame.method) {
        yield* options.onMessage({ method: frame.method, params: frame.params });
        // MSP server requests acknowledge presentation; decisions use separate commands.
        if (frame.id !== undefined) yield* send({ jsonrpc: "2.0", id: frame.id, result: {} });
      } else if (typeof frame.id === "number") {
        const waiter = pending.get(frame.id);
        if (!waiter) return;
        pending.delete(frame.id);
        if (frame.error) yield* Deferred.fail(waiter, museError("request", frame.error.message));
        else yield* Deferred.succeed(waiter, frame.result);
      }
    });
  const stdoutRemainder = yield* Ref.make("");
  yield* child.stdout.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Ref.modify(stdoutRemainder, (current) => {
        const combined = current + chunk;
        const lines = combined.split("\n");
        const remainder = lines.pop() ?? "";
        return [lines, remainder] as const;
      }).pipe(Effect.flatMap((lines) => Effect.forEach(lines, handleLine, { discard: true }))),
    ),
    Effect.matchEffect({
      onFailure: (cause) =>
        close(museError("transport", "Muse connection closed while reading output.", cause)),
      onSuccess: () => close(museError("transport", "Muse connection closed.")),
    }),
    Effect.forkScoped,
  );
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catch((cause) =>
      close(museError("transport", "Muse connection closed while writing input.", cause)),
    ),
    Effect.forkScoped,
  );
  // Drain diagnostics without logging prompts, account details, or credentials.
  yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
  return {
    request: <A>(method: string, params: unknown, schema: Schema.Codec<A, unknown>) =>
      Effect.gen(function* () {
        const id = ++sequence;
        const waiter = yield* Deferred.make<unknown, ProviderAdapterRequestError>();
        pending.set(id, waiter);
        return yield* send({ jsonrpc: "2.0", id, method, params }).pipe(
          Effect.andThen(Deferred.await(waiter)),
          Effect.timeoutOrElse({
            duration: "30 seconds",
            orElse: () =>
              Effect.fail(
                museError(method, "Muse did not acknowledge the request within 30 seconds."),
              ),
          }),
          Effect.flatMap(Schema.decodeUnknownEffect(schema)),
          Effect.mapError((cause) => museError(method, cause.message, cause)),
          Effect.ensuring(
            Effect.sync(() => {
              pending.delete(id);
            }),
          ),
        );
      }),
    notify: (method: string) => send({ jsonrpc: "2.0", method }),
  };
});
