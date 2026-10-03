// Use the installed local toolchain; never install dependencies during a build.
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
for (const [binary, args] of [
  ["node_modules/typescript/bin/tsc", ["--noEmit"]],
  ["node_modules/vite/bin/vite.js", ["build"]],
]) {
  const result = spawnSync(process.execPath, [resolve(root, binary), ...args], {
    cwd: root,
    stdio: "inherit",
  });
  if (result.error || result.status !== 0) {
    if (result.error) console.error(result.error.message);
    process.exit(result.status ?? 1);
  }
}
