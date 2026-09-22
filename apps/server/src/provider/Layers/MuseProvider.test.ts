import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { MuseSettings } from "@t3tools/contracts";

import { buildInitialMuseProviderSnapshot, checkMuseProviderStatus } from "./MuseProvider.ts";

const decodeMuseSettings = Schema.decodeSync(MuseSettings);

describe("buildInitialMuseProviderSnapshot", () => {
  it.effect("returns a disabled snapshot when disabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMuseProviderSnapshot(
        decodeMuseSettings({ enabled: false }),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
    }),
  );

  it.effect("returns a pending snapshot when enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMuseProviderSnapshot(
        decodeMuseSettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.message).toContain("Checking Muse");
    }),
  );

  it.effect("lists the pinned contributor tier model", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMuseProviderSnapshot(
        decodeMuseSettings({ enabled: true }),
      );
      expect(snapshot.models.some((model) => model.name === "Muse Spark 1.3")).toBe(true);
    }),
  );

  it.effect("advertises the reasoning effort selector", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMuseProviderSnapshot(
        decodeMuseSettings({ enabled: true }),
      );
      const model = snapshot.models.find((candidate) => candidate.slug === "default");
      expect(model).toBeDefined();
      const descriptor = model?.capabilities?.optionDescriptors?.find(
        (candidate) => candidate.id === "reasoningEffort",
      );
      expect(descriptor?.type).toBe("select");
      if (descriptor?.type === "select") {
        expect(descriptor.options.map((option) => option.id)).toEqual([
          "none",
          "minimal",
          "low",
          "medium",
          "high",
          "xhigh",
          "max",
          "ultra",
        ]);
        expect(descriptor.currentValue).toBe("high");
      }
    }),
  );
});

it.layer(NodeServices.layer)("checkMuseProviderStatus", (it) => {
  const writeVersionBinary = (dir: string) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const musePath = path.join(dir, "muse");
      yield* fs.writeFileString(
        musePath,
        ["#!/bin/sh", 'printf "%s\\n" "Muse Code 1.3.0 (1.3.0-R3401.1)"', "exit 0", ""].join("\n"),
      );
      yield* fs.chmod(musePath, 0o755);
      return musePath;
    });

  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkMuseProviderStatus(
        decodeMuseSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/muse-binary",
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
    }),
  );

  it.effect("reports authenticated when META_API_KEY is set", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-muse-home-" });
        const musePath = yield* writeVersionBinary(home);
        const snapshot = yield* checkMuseProviderStatus(
          decodeMuseSettings({ enabled: true, binaryPath: musePath }),
          { HOME: home, META_API_KEY: "test-key-value" },
        );
        expect(snapshot.status).toBe("ready");
        expect(snapshot.auth.status).toBe("authenticated");
      }),
    ),
  );

  it.effect("reports authenticated when a saved credential file exists", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-muse-home-" });
        yield* fs.makeDirectory(path.join(home, ".config", "muse"), { recursive: true });
        yield* fs.writeFileString(path.join(home, ".config", "muse", "auth.json"), "{}\n");
        const musePath = yield* writeVersionBinary(home);
        const snapshot = yield* checkMuseProviderStatus(
          decodeMuseSettings({ enabled: true, binaryPath: musePath }),
          { HOME: home },
        );
        expect(snapshot.status).toBe("ready");
        expect(snapshot.auth.status).toBe("authenticated");
      }),
    ),
  );

  it.effect("reports unknown auth when no credential signal exists", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-muse-home-" });
        const musePath = yield* writeVersionBinary(home);
        const snapshot = yield* checkMuseProviderStatus(
          decodeMuseSettings({ enabled: true, binaryPath: musePath }),
          { HOME: home },
        );
        expect(snapshot.status).toBe("ready");
        expect(snapshot.auth.status).toBe("unknown");
      }),
    ),
  );

  it.effect("includes discovered skills in the ready snapshot", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-muse-home-" });
        const musePath = path.join(home, "muse");
        yield* fs.writeFileString(
          musePath,
          [
            "#!/bin/sh",
            'if [ "$1" = "skills" ]; then',
            `  printf '%s\\n' '{"skills":[{"name":"review","scope":"user","path":"$HOME/.agents/skills/review/SKILL.md","activation":"on"}]}'`,
            "  exit 0",
            "fi",
            'printf "%s\\n" "Muse Code 1.3.0 (1.3.0-R3401.1)"',
            "exit 0",
            "",
          ].join("\n"),
        );
        yield* fs.chmod(musePath, 0o755);
        const snapshot = yield* checkMuseProviderStatus(
          decodeMuseSettings({ enabled: true, binaryPath: musePath }),
          { HOME: home, META_API_KEY: "test-key-value" },
        );
        expect(snapshot.status).toBe("ready");
        expect(snapshot.skills.map((skill) => skill.name)).toEqual(["review"]);
      }),
    ),
  );
});
