import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { type MuseSettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { spawnAndCollect } from "../provider/providerSnapshot.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";

const TerminalEvent = Schema.Struct({
  payload: Schema.Struct({
    kind: Schema.Literal("run_terminal"),
    terminal: Schema.String,
    text: Schema.optional(Schema.NullOr(Schema.String)),
  }),
});
const JsonStringCodec = Schema.fromJsonString(Schema.Unknown);
const encodeJsonString = Schema.encodeEffect(JsonStringCodec);
const JsonTerminalEvent = Schema.fromJsonString(TerminalEvent);
const decodeJsonTerminalEvent = Schema.decodeUnknownEffect(JsonTerminalEvent);

export const makeMuseTextGeneration = Effect.fn("makeMuseTextGeneration")(function* (
  settings: MuseSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runMuseJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const promptPath = yield* fileSystem.makeTempFileScoped({ prefix: "t3code-muse-prompt-" });
      const schemaPath = yield* fileSystem.makeTempFileScoped({ prefix: "t3code-muse-schema-" });
      yield* fileSystem.writeFileString(promptPath, prompt);
      const schemaJson = yield* encodeJsonString(toJsonSchemaObject(outputSchemaJson));
      yield* fileSystem.writeFileString(schemaPath, schemaJson);
      const spawn = yield* resolveSpawnCommand(
        settings.binaryPath,
        [
          "exec",
          "--json",
          "--prompt-file",
          promptPath,
          "--output-schema",
          schemaPath,
          "--workspace",
          cwd,
          "--disable-shell",
          "--disable-write",
          "--disable-web-tools",
          "--no-foreign-personal-context",
          "--no-session-log",
          "--max-model-steps",
          "3",
          ...(modelSelection.model === "default" ? [] : ["--model", modelSelection.model]),
        ],
        { env: environment },
      );
      const result = yield* spawnAndCollect(
        settings.binaryPath,
        ChildProcess.make(spawn.command, spawn.args, {
          cwd,
          env: environment,
          shell: spawn.shell,
        }),
      ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      if (result.code !== 0)
        return yield* new TextGenerationError({
          operation,
          detail: "Muse text generation failed.",
        });
      let finalText: string | undefined;
      for (const line of result.stdout.split("\n")) {
        const parsed = yield* decodeJsonTerminalEvent(line).pipe(Effect.orElseSucceed(() => null));
        if (parsed?.payload.terminal === "completed" && parsed.payload.text)
          finalText = parsed.payload.text;
      }
      if (!finalText)
        return yield* new TextGenerationError({
          operation,
          detail: "Muse returned no completed text response.",
        });
      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(outputSchemaJson))(
        extractJsonObject(finalText),
      );
    }).pipe(
      Effect.timeout(180_000),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Muse text generation failed or returned invalid output.",
            cause,
          }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("MuseTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runMuseJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("MuseTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runMuseJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("MuseTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runMuseJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("MuseTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runMuseJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
