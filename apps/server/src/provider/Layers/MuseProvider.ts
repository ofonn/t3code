import type { MuseSettings, ServerProviderSkill } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { ChildProcess } from "effect/unstable/process";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
} from "../providerSnapshot.ts";
import { discoverMuseSkills } from "../Drivers/MuseSkills.ts";

const MUSE_REASONING_EFFORT_OPTIONS = [
  { id: "none", label: "None" },
  { id: "minimal", label: "Minimal" },
  { id: "low", label: "Low" },
  { id: "medium", label: "Medium" },
  { id: "high", label: "High" },
  { id: "xhigh", label: "Extra High" },
  { id: "max", label: "Max" },
  { id: "ultra", label: "Ultra" },
] as const;

export const MUSE_DEFAULT_REASONING_EFFORT = "max" as const;

export function buildMuseModelCapabilities() {
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: MUSE_REASONING_EFFORT_OPTIONS.map((option) => ({
          id: option.id,
          label: option.label,
          ...(option.id === MUSE_DEFAULT_REASONING_EFFORT ? { isDefault: true } : {}),
        })),
        currentValue: MUSE_DEFAULT_REASONING_EFFORT,
      },
    ],
  });
}

const capabilities = buildMuseModelCapabilities();

const buildSnapshot = (
  settings: MuseSettings,
  probe: ProviderProbeResult,
  skills: ReadonlyArray<ServerProviderSkill> = [],
) =>
  Effect.map(DateTime.now, (now) =>
    buildServerProvider({
      presentation: {
        displayName: "Muse",
        showInteractionModeToggle: false,
        requiresNewThreadForModelChange: true,
      },
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(now),
      models: providerModelsFromSettings(
        [{ slug: "default", name: "Muse Spark 1.3", isCustom: false, capabilities }],
        settings.customModels,
        capabilities,
      ),
      skills,
      probe,
    }),
  );

export const buildInitialMuseProviderSnapshot = (settings: MuseSettings) =>
  buildSnapshot(settings, {
    installed: settings.enabled,
    version: null,
    status: "warning",
    auth: { status: "unknown" },
    message: settings.enabled
      ? "Checking Muse CLI availability..."
      : "Muse is disabled in T3 Code settings.",
  });

export const checkMuseProviderStatus = Effect.fn("checkMuseProviderStatus")(function* (
  settings: MuseSettings,
  environment?: NodeJS.ProcessEnv,
) {
  if (!settings.enabled) return yield* buildInitialMuseProviderSnapshot(settings);

  const probe = yield* Effect.gen(function* () {
    const spawn = yield* resolveSpawnCommand(settings.binaryPath, ["--version"], {
      ...(environment ? { env: environment } : {}),
    });
    return yield* spawnAndCollect(
      settings.binaryPath,
      ChildProcess.make(spawn.command, spawn.args, {
        shell: spawn.shell,
        ...(environment ? { env: environment } : { extendEnv: true }),
      }),
    );
  }).pipe(Effect.timeoutOption(4_000), Effect.result);

  if (Result.isFailure(probe)) {
    return yield* buildSnapshot(settings, {
      installed: !isCommandMissingCause(probe.failure),
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: isCommandMissingCause(probe.failure)
        ? "Muse CLI was not found. Set its executable path in provider settings."
        : "Failed to execute the Muse CLI health check.",
    });
  }
  if (Option.isNone(probe.success)) {
    return yield* buildSnapshot(settings, {
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "Muse CLI timed out while checking its version.",
    });
  }
  const result = probe.success.value;
  if (result.code !== 0) {
    return yield* buildSnapshot(settings, {
      installed: true,
      version: parseGenericCliVersion(result.stdout),
      status: "error",
      auth: { status: "unknown" },
      message: "Muse CLI version check failed.",
    });
  }
  const env = environment ?? process.env;
  const skills = yield* discoverMuseSkills(settings, env);
  // The CLI ships no auth status command, so read the same signals
  // `muse login` documents: META_API_KEY wins, otherwise the saved
  // credential file. Existence only, never contents.
  let authenticated = (env.META_API_KEY ?? "").trim().length > 0;
  if (!authenticated) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = (env.HOME ?? "").trim();
    const configHome =
      (env.XDG_CONFIG_HOME ?? "").trim() || (home ? path.join(home, ".config") : "");
    const candidates = [
      ...(configHome ? [path.join(configHome, "muse", "auth.json")] : []),
      ...(home ? [path.join(home, ".muse", "auth.json")] : []),
    ];
    for (const candidate of candidates) {
      const exists = yield* fileSystem.exists(candidate).pipe(Effect.orElseSucceed(() => false));
      if (exists) {
        authenticated = true;
        break;
      }
    }
  }
  return yield* buildSnapshot(
    settings,
    {
      installed: true,
      version: parseGenericCliVersion(result.stdout),
      status: "ready",
      auth: authenticated ? { status: "authenticated" } : { status: "unknown" },
    },
    skills,
  );
});
