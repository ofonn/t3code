import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { makeMuseProtocol } from "./MuseProtocol.ts";

it.layer(NodeServices.layer)("Muse protocol", (it) => {
  it.effect("exchanges JSON-RPC frames and reports process exit without hanging", () =>
    Effect.gen(function* () {
      const protocol = yield* makeMuseProtocol({
        command: process.execPath,
        args: [
          "-e",
          `const r=require('node:readline').createInterface({input:process.stdin});r.on('line',line=>{const m=JSON.parse(line);if(m.method==='exit')process.exit(2);else process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:m.id,result:{echo:m.params.text}})+'\\n')})`,
        ],
        cwd: process.cwd(),
        onMessage: () => Effect.void,
        onExit: () => Effect.void,
      });
      const result = yield* protocol.request(
        "echo",
        { text: "hello\nMuse" },
        Schema.Struct({ echo: Schema.String }),
      );
      assert.equal(result.echo, "hello\nMuse");
      const exit = yield* protocol.request("exit", {}, Schema.Void).pipe(Effect.flip);
      assert.include(exit.message, "closed");
    }),
  );
});
