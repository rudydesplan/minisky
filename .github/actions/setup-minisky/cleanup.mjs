import { rmSync } from "node:fs";
import { platform } from "node:process";
import { execFileSync } from "node:child_process";

const pid = Number(process.env.STATE_pid);
if (Number.isInteger(pid) && pid > 0) {
  try {
    if (platform === "win32") {
      execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
    } else {
      process.kill(-pid, "SIGTERM");
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        try {
          process.kill(pid, 0);
        } catch {
          break;
        }
        await new Promise((resolvePromise) => setTimeout(resolvePromise, 100));
      }
      try {
        process.kill(-pid, "SIGKILL");
      } catch {
        // Process group already stopped.
      }
    }
  } catch {
    // Cleanup is best-effort when the daemon already exited.
  }
}

const root = process.env.STATE_root;
if (root) {
  try {
    rmSync(root, { recursive: true, force: true });
  } catch (error) {
    process.stderr.write(`setup-minisky cleanup warning: ${error}\n`);
  }
}
