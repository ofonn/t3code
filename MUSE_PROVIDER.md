# Muse provider (unofficial, this branch only)

This branch adds the Muse CLI as a T3 Code provider. Nothing here exists
upstream. Upstream is not accepting contributions (see `CONTRIBUTING.md`),
so the integration stays on the `muse-provider` branch of this fork.
Tested on Linux only.

What you get:

- Muse as a provider pick, with per-thread sessions and resume
- `/` lists Muse skills (user, project, bundled, plugin)
- Reasoning-effort selector in the chat input
- Every thread starts trusted, so workspace skills and rules load
- Pinned model `muse-spark-1.3-contributor`
- Tests plus an MSP mock host

## Requirements

- Linux (only tested platform)
- Node.js (developed and tested with v26) and pnpm 11
  (`packageManager` in `package.json` pins it; bun is not needed)
- Muse CLI on `PATH`, authenticated (`muse login` or `META_API_KEY`)
- Contributor-tier access for the pinned model. Outside the program,
  change one line, `MUSE_PINNED_MODEL_ID` in
  `apps/server/src/provider/Layers/MuseAdapter.ts`.

## Setup

1. Clone the branch and install dependencies:

   ```sh
   git clone https://github.com/ofonn/t3code.git -b muse-provider
   cd t3code
   pnpm install
   ```

2. Start the dev stack with the repo-local runner on `PATH`:

   ```sh
   PATH="$PWD/node_modules/.bin:$PATH" node scripts/dev-runner.ts dev
   ```

   Defaults: server on `127.0.0.1:13773`, web on `127.0.0.1:5733`.
   The runner prints a pairing URL. Open the
   `http://localhost:5733/pair#token=...` link to connect.

If you also run a daily-driver T3, `t3 pair` discovers that server first
and mints for it, not for dev. To force a dev token, point discovery at a
shadow home holding only the dev state:

```sh
mkdir -p /tmp/t3pairhome
ln -sfn ~/.t3/dev /tmp/t3pairhome/dev
cd apps/server
T3CODE_HOME=/tmp/t3pairhome node src/bin.ts pair
```

Pairings persist in the dev database across server restarts.

## Verifying

From `apps/server`, run the focused suites with the repo-local runner:

```sh
../../node_modules/.bin/vp test run \
  src/provider/Drivers/MuseSkills.test.ts \
  src/provider/Layers/MuseProvider.test.ts \
  src/provider/Layers/MuseAdapter.test.ts \
  src/provider/muse/MuseProtocol.test.ts
```

Typecheck and lint from the repo root:

```sh
./node_modules/.bin/vp run --filter t3 typecheck
./node_modules/.bin/vp lint <changed files>
```

## Keeping it current

This branch sits behind upstream `main`. To update, rebase it onto a
fresh `main` and resolve the wiring touchpoints
(`builtInDrivers.ts`, `TextGeneration.ts`, `Icons.tsx`,
`providerIconUtils.ts`, `providerDriverMeta.ts`, `session-logic.ts`,
contracts settings). The Muse files themselves are additive and rebase
clean.

## Reverting

The whole integration is this branch. `git checkout main` (or delete the
branch) removes it; no upstream files are modified in place.
