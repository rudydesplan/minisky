import { createHash } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fstatSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { arch, platform } from "node:process";
import { execFileSync, spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";

function input(name, fallback = "") {
  return (process.env[`INPUT_${name.replaceAll("-", "_").toUpperCase()}`] ?? fallback).trim();
}

function appendCommand(fileName, line) {
  const file = process.env[fileName];
  if (file) appendFileSync(file, `${line}\n`);
}

function fail(message) {
  process.stderr.write(`setup-minisky: ${message}\n`);
  process.exitCode = 1;
}

function releaseTarget() {
  const key = `${platform}-${arch}`;
  const targets = {
    "linux-x64": ["linux", "amd64", "tar.gz", "minisky"],
    "linux-arm64": ["linux", "arm64", "tar.gz", "minisky"],
    "darwin-arm64": ["darwin", "arm64", "tar.gz", "minisky"],
    "win32-x64": ["windows", "amd64", "zip", "minisky.exe"],
  };
  const target = targets[key];
  if (!target) throw new Error(`unsupported runner ${platform}/${arch}`);
  return target;
}

async function download(url, destination) {
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`download ${url} returned HTTP ${response.status}`);
  const payload = Buffer.from(await response.arrayBuffer());
  writeFileSync(destination, payload, { mode: 0o600 });
}

export function verifyChecksum(archivePath, checksumsPath, archiveName) {
  const matches = [];
  for (const line of readFileSync(checksumsPath, "utf8").split(/\r?\n/)) {
    if (line === "") continue;
    const match = /^([a-fA-F0-9]{64}) ([ *])(.+)$/.exec(line);
    if (!match) throw new Error("checksums.txt contains an invalid checksum entry");
    const [, digest, , fileName] = match;
    if (fileName === archiveName) matches.push(digest);
  }
  if (matches.length !== 1) {
    throw new Error(`checksums.txt must cover ${archiveName} exactly once`);
  }
  const [expected] = matches;
  const actual = createHash("sha256").update(readFileSync(archivePath)).digest("hex");
  if (actual.toLowerCase() !== expected.toLowerCase()) {
    throw new Error(`checksum mismatch for ${archiveName}`);
  }
}

export function safeArchiveEntry(entry) {
  const normalized = entry.replaceAll("\\", "/");
  return normalized !== "" &&
    !normalized.startsWith("/") &&
    !/^[A-Za-z]:\//.test(normalized) &&
    !normalized.split("/").includes("..");
}

function tarString(buffer, start, length) {
  const end = buffer.indexOf(0, start);
  return buffer.subarray(start, end >= start && end < start + length ? end : start + length).toString("utf8");
}

function tarSize(header) {
  const encoded = tarString(header, 124, 12).trim();
  if (!/^[0-7]+$/.test(encoded)) throw new Error("release archive has an invalid tar size");
  const size = Number.parseInt(encoded, 8);
  if (!Number.isSafeInteger(size)) throw new Error("release archive tar entry is too large");
  return size;
}

export function validateTarGzipArchive(archive) {
  let tar;
  try {
    tar = gunzipSync(archive);
  } catch {
    throw new Error("release archive is not a valid gzip stream");
  }
  let offset = 0;
  while (offset + 512 <= tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) return;
    const name = tarString(header, 0, 100);
    const prefix = tarString(header, 345, 155);
    const fullName = prefix ? `${prefix}/${name}` : name;
    if (!safeArchiveEntry(fullName)) throw new Error("release archive contains an unsafe path");
    const type = String.fromCharCode(header[156]);
    if (type !== "\0" && type !== "0" && type !== "5") {
      throw new Error(`release archive contains a non-regular tar entry: ${fullName}`);
    }
    const size = tarSize(header);
    if (type === "5" && size !== 0) {
      throw new Error(`release archive directory contains data: ${fullName}`);
    }
    offset += 512 + Math.ceil(size / 512) * 512;
    if (offset > tar.length) throw new Error("release archive contains a truncated tar entry");
  }
  throw new Error("release archive is missing the tar end marker");
}

function extractArchive(archivePath, format, destination) {
  mkdirSync(destination, { recursive: true, mode: 0o700 });
  if (format === "tar.gz") {
    const archive = readFileSync(archivePath);
    validateTarGzipArchive(archive);
    execFileSync("tar", ["-xzf", "-", "-C", destination], { input: archive });
    return;
  }
  const script = [
    "Add-Type -AssemblyName System.IO.Compression.FileSystem",
    `$zip=[System.IO.Compression.ZipFile]::OpenRead('${archivePath.replaceAll("'", "''")}')`,
    "$bad=$zip.Entries | Where-Object { $_.FullName -match '(^[\\\\/]|^[A-Za-z]:[\\\\/]|(^|[\\\\/])\\.\\.([\\\\/]|$))' }",
    "if ($bad) { $zip.Dispose(); throw 'release archive contains an unsafe path' }",
    "$zip.Dispose()",
    `[System.IO.Compression.ZipFile]::ExtractToDirectory('${archivePath.replaceAll("'", "''")}','${destination.replaceAll("'", "''")}')`,
  ].join("; ");
  execFileSync("powershell", ["-NoProfile", "-Command", script]);
}

export function validateExtractedBinary(path) {
  const info = lstatSync(path);
  if (!info.isFile()) throw new Error("extracted MiniSky binary must be a regular file");
  return path;
}

export function copyRegularFileNoFollow(source, destination, label, operations = {}) {
  const open = operations.open ?? openSync;
  const before = lstatSync(source);
  if (!before.isFile()) throw new Error(`${label} must be a regular file`);
  const descriptor = open(source, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const after = fstatSync(descriptor);
    if (!after.isFile() || before.dev !== after.dev || before.ino !== after.ino) {
      throw new Error(`${label} changed while opening`);
    }
    writeFileSync(destination, readFileSync(descriptor), { mode: 0o600, flag: "wx" });
  } finally {
    closeSync(descriptor);
  }
}

function findBinary(root, fileName) {
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      const nested = findBinary(path, fileName);
      if (nested) return nested;
    } else if (entry.name === fileName) {
      return validateExtractedBinary(path);
    }
  }
  return "";
}

function stopProcess(pid) {
  if (!pid) return;
  try {
    if (platform === "win32") execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"]);
    else process.kill(-pid, "SIGTERM");
  } catch {
    // The process may already have exited during startup.
  }
}

async function main() {
  const runnerTemp = process.env.RUNNER_TEMP || tmpdir();
  const root = mkdtempSync(join(runnerTemp, "setup-minisky-"));
  const binDir = join(root, "bin");
  const home = join(root, "home");
  const stateDir = join(root, "state");
  mkdirSync(binDir, { recursive: true, mode: 0o700 });
  mkdirSync(home, { recursive: true, mode: 0o700 });
  mkdirSync(stateDir, { recursive: true, mode: 0o700 });

  const [releaseOS, releaseArch, format, binaryName] = releaseTarget();
  const installedBinary = join(binDir, binaryName);
  const suppliedBinary = input("binary");
  if (suppliedBinary) {
    const source = resolve(suppliedBinary);
    if (!existsSync(source) || !lstatSync(source).isFile()) {
      throw new Error(`supplied binary does not exist: ${source}`);
    }
    copyRegularFileNoFollow(source, installedBinary, "supplied binary");
  } else {
    const repository = input("repository", "qamarudeenm/minisky");
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) {
      throw new Error(`invalid release repository ${repository}`);
    }
    const version = input("version");
    if (!/^v[0-9]+\.[0-9]+\.[0-9]+$/.test(version)) {
      throw new Error("version must be an explicit stable vMAJOR.MINOR.PATCH tag when binary is not supplied");
    }
    const archiveName = `minisky_${releaseOS}_${releaseArch}.${format}`;
    const releaseBase = `https://github.com/${repository}/releases/download/${version}`;
    const archivePath = join(root, archiveName);
    const checksumsPath = join(root, "checksums.txt");
    await Promise.all([
      download(`${releaseBase}/${archiveName}`, archivePath),
      download(`${releaseBase}/checksums.txt`, checksumsPath),
    ]);
    verifyChecksum(archivePath, checksumsPath, archiveName);
    const extracted = join(root, "release");
    extractArchive(archivePath, format, extracted);
    const source = findBinary(extracted, binaryName);
    if (!source) throw new Error(`verified release archive does not contain ${binaryName}`);
    copyRegularFileNoFollow(source, installedBinary, "extracted MiniSky binary");
  }
  if (platform !== "win32") chmodSync(installedBinary, 0o700);

  const apiPort = input("api-port", "18080");
  const uiPort = input("ui-port", "18081");
  const profile = input("profile", "github-action");
  const services = input("services");
  const timeoutSeconds = Number(input("timeout-seconds", "60"));
  if (
    !/^[0-9]{1,5}$/.test(apiPort) ||
    !/^[0-9]{1,5}$/.test(uiPort) ||
    Number(apiPort) < 1 ||
    Number(apiPort) > 65535 ||
    Number(uiPort) < 1 ||
    Number(uiPort) > 65535
  ) {
    throw new Error("api-port and ui-port must be TCP ports from 1 to 65535");
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(profile)) {
    throw new Error("profile contains unsupported characters");
  }
  if (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 600) {
    throw new Error("timeout-seconds must be between 1 and 600");
  }

  const args = ["start", "--port", apiPort, "--ui-port", uiPort];
  if (services) args.push("--services", services);
  const logPath = join(root, "minisky.log");
  const log = openSync(logPath, "a", 0o600);
  const child = spawn(installedBinary, args, {
    detached: platform !== "win32",
    env: {
      ...process.env,
      HOME: home,
      MINISKY_STATE_DIR: stateDir,
      MINISKY_PROFILE: profile,
    },
    stdio: ["ignore", log, log],
  });
  closeSync(log);
  child.unref();
  appendCommand("GITHUB_STATE", `pid=${child.pid}`);
  appendCommand("GITHUB_STATE", `root=${root}`);
  appendCommand("GITHUB_STATE", `log=${logPath}`);

  const endpoint = `http://127.0.0.1:${apiPort}`;
  const deadline = Date.now() + timeoutSeconds * 1000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${endpoint}/healthz`);
      if (response.ok) {
        appendCommand("GITHUB_OUTPUT", `endpoint=${endpoint}`);
        appendCommand("GITHUB_ENV", `MINISKY_ENDPOINT=${endpoint}`);
        appendCommand("GITHUB_PATH", binDir);
        process.stdout.write(`MiniSky ready at ${endpoint}; logs: ${logPath}\n`);
        return;
      }
    } catch {
      // Retry until the bounded deadline.
    }
    try {
      process.kill(child.pid, 0);
    } catch {
      throw new Error(`MiniSky exited before readiness; logs:\n${readFileSync(logPath, "utf8")}`);
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 500));
  }
  stopProcess(child.pid);
  throw new Error(`MiniSky readiness timed out; logs:\n${readFileSync(logPath, "utf8")}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => fail(error instanceof Error ? error.message : String(error)));
}
