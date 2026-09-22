import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { discoverMuseSkills, parseMuseSkillsList } from "./MuseSkills.ts";

const listPayload = (skills: ReadonlyArray<unknown>) => JSON.stringify({ skills });

describe("parseMuseSkillsList", () => {
  it("maps list entries onto provider skills, sorted by name", () => {
    const skills = parseMuseSkillsList(
      listPayload([
        {
          id: "writing-docs",
          name: "writing-docs",
          display_name: "writing-docs",
          description: "Write user docs.",
          short_description: "Docs writer.",
          scope: "user",
          path: "$HOME/.agents/skills/writing-docs/SKILL.md",
          activation: "on",
        },
        {
          id: "bundled:plan",
          name: "plan",
          display_name: "plan",
          description: "Create a grounded plan.",
          scope: "bundled",
          path: "bundled://muse-core/skills/plan/SKILL.md",
          activation: "user-invocable-only",
        },
      ]),
    );

    expect(skills).toEqual([
      {
        name: "plan",
        path: "bundled://muse-core/skills/plan/SKILL.md",
        enabled: true,
        scope: "bundled",
        description: "Create a grounded plan.",
        displayName: "plan",
      },
      {
        name: "writing-docs",
        path: "$HOME/.agents/skills/writing-docs/SKILL.md",
        enabled: true,
        scope: "user",
        description: "Write user docs.",
        shortDescription: "Docs writer.",
        displayName: "writing-docs",
      },
    ]);
  });

  it("disables skills the CLI reports as switched off", () => {
    const skills = parseMuseSkillsList(
      listPayload([
        {
          name: "retired",
          scope: "user",
          path: "$HOME/.agents/skills/retired/SKILL.md",
          activation: "off",
        },
      ]),
    );

    expect(skills).toEqual([
      {
        name: "retired",
        path: "$HOME/.agents/skills/retired/SKILL.md",
        enabled: false,
        scope: "user",
      },
    ]);
  });

  it("skips entries without a name or a path", () => {
    const skills = parseMuseSkillsList(
      listPayload([
        { name: "  ", path: "$HOME/.agents/skills/a/SKILL.md" },
        { name: "no-path" },
        "not-an-object",
        { name: "kept", scope: "project", path: ".agents/skills/kept/SKILL.md" },
      ]),
    );

    expect(skills.map((skill) => skill.name)).toEqual(["kept"]);
  });

  it("returns an empty list for malformed or unexpected output", () => {
    expect(parseMuseSkillsList("not json")).toEqual([]);
    expect(parseMuseSkillsList("null")).toEqual([]);
    expect(parseMuseSkillsList(JSON.stringify({ skills: "nope" }))).toEqual([]);
    expect(parseMuseSkillsList(JSON.stringify({}))).toEqual([]);
  });
});

describe("discoverMuseSkills", () => {
  it.effect("spawns the list probe in the configured cwd", () => {
    const spawnCwds: Array<string | undefined> = [];
    const spawner = ChildProcessSpawner.make((command) => {
      spawnCwds.push(command._tag === "StandardCommand" ? command.options.cwd : undefined);
      return Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.encodeText(
            Stream.make(
              listPayload([
                {
                  name: "kept",
                  scope: "project",
                  path: ".agents/skills/kept/SKILL.md",
                },
              ]),
            ),
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      );
    });

    return Effect.gen(function* () {
      const skills = yield* discoverMuseSkills({ binaryPath: "muse" }, {}, "/workspaces/demo").pipe(
        Effect.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
      );

      expect(spawnCwds).toEqual(["/workspaces/demo"]);
      expect(skills.map((skill) => skill.name)).toEqual(["kept"]);
    });
  });
});
