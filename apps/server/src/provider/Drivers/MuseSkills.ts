/**
 * MuseSkills — skill discovery for the `/` picker via `muse skills list --json`.
 *
 * Like the Grok CLI, Muse reports its own skill catalog: `muse skills list
 * --json` returns `skills[]` with `name`, `description`, `short_description`,
 * `display_name`, `scope` (`user` / `project` / `bundled` / `plugin`), `path`,
 * and `activation` (`on` / `user-invocable-only`, plus an off state for
 * disabled skills). Asking the CLI beats scanning the filesystem because the
 * catalog honors Muse's own precedence (user roots `.agents` over `.claude`
 * over `.codex`, project over user) and includes bundled and plugin skills,
 * which live outside any `skills/` directory a flat scan could cover. User
 * paths carry a literal `$HOME` prefix and project paths are workspace
 * relative; both pass through verbatim because the web client treats `path`
 * as an opaque key. Discovery is best-effort: a missing binary, a timeout,
 * or malformed output yields an empty list, never a degraded provider
 * snapshot.
 *
 * @module provider/Drivers/MuseSkills
 */
import type { MuseSettings, ServerProviderSkill } from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { spawnAndCollect } from "../providerSnapshot.ts";

const MUSE_SKILLS_PROBE_TIMEOUT_MS = 4_000;

const trimString = (value: unknown): string => (typeof value === "string" ? value.trim() : "");

/**
 * Map `muse skills list --json` output onto provider skills. Entries without
 * a name or a path are skipped; skills the CLI reports as switched off are
 * kept but disabled so pickers that filter on `enabled` hide them.
 */
export function parseMuseSkillsList(stdout: string): ReadonlyArray<ServerProviderSkill> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    return [];
  }
  if (typeof parsed !== "object" || parsed === null) {
    return [];
  }
  const entries = (parsed as Record<string, unknown>).skills;
  if (!Array.isArray(entries)) {
    return [];
  }

  const skillsByName = new Map<string, ServerProviderSkill>();
  for (const entry of entries) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const record = entry as Record<string, unknown>;
    const name = trimString(record.name);
    const path = trimString(record.path);
    if (!name || !path) {
      continue;
    }
    const scope = trimString(record.scope);
    const description = trimString(record.description);
    const shortDescription = trimString(record.short_description);
    const displayName = trimString(record.display_name);
    const activation = trimString(record.activation).toLowerCase();
    skillsByName.set(name, {
      name,
      path,
      enabled: activation !== "off" && activation !== "disabled",
      ...(scope ? { scope } : {}),
      ...(description ? { description } : {}),
      ...(shortDescription ? { shortDescription } : {}),
      ...(displayName ? { displayName } : {}),
    });
  }

  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
}

/**
 * Run `muse skills list --json` and map the reported catalog onto provider
 * skills. Never fails: any spawn error, non-zero exit, or timeout resolves
 * to an empty list. Without an explicit cwd the probe inherits the server
 * cwd, so project skills reflect the server checkout, not the thread
 * workspace; provider snapshots have no thread context to do better.
 */
export const discoverMuseSkills = Effect.fn("discoverMuseSkills")(function* (
  museSettings: Pick<MuseSettings, "binaryPath">,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<
  ReadonlyArray<ServerProviderSkill>,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> {
  const command = museSettings.binaryPath || "muse";
  const listResult = yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(command, ["skills", "list", "--json"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(cwd ? { cwd } : {}),
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  }).pipe(Effect.timeoutOption(MUSE_SKILLS_PROBE_TIMEOUT_MS), Effect.result);

  if (Result.isFailure(listResult) || Option.isNone(listResult.success)) {
    yield* Effect.logDebug("Muse skill discovery failed; continuing without skills.");
    return [];
  }
  const output = listResult.success.value;
  if (output.code !== 0) {
    yield* Effect.logDebug("Muse skill discovery exited non-zero; continuing without skills.", {
      exitCode: output.code,
    });
    return [];
  }
  return parseMuseSkillsList(output.stdout);
});
