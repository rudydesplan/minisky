import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import {
  constants,
  existsSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  mkdtempSync,
  mkdirSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { platform } from "node:process";
import test from "node:test";

import {
  copyRegularFileNoFollow,
  safeArchiveEntry,
  validateExtractedBinary,
  validateTarGzipArchive,
  verifyChecksum,
} from "./index.mjs";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "setup-minisky-test-"));
  t.after(() => rmSync(root, { recursive: true, force: true, maxRetries: 3 }));
  return root;
}

function tarEntry(name, type = "0", payload = Buffer.alloc(0), linkName = "") {
  const header = Buffer.alloc(512);
  header.write(name, 0, 100, "utf8");
  header.write("0000700\0", 100, 8, "ascii");
  header.write("0000000\0", 108, 8, "ascii");
  header.write("0000000\0", 116, 8, "ascii");
  header.write(`${payload.length.toString(8).padStart(11, "0")}\0`, 124, 12, "ascii");
  header.write("00000000000\0", 136, 12, "ascii");
  header.fill(" ", 148, 156);
  header.write(type, 156, 1, "ascii");
  header.write(linkName, 157, 100, "utf8");
  header.write("ustar\0", 257, 6, "ascii");
  header.write("00", 263, 2, "ascii");
  const checksum = header.reduce((sum, byte) => sum + byte, 0);
  header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
  const padding = Buffer.alloc((512 - (payload.length % 512)) % 512);
  return Buffer.concat([header, payload, padding]);
}

function tarGzip(...entries) {
  return gzipSync(Buffer.concat([...entries, Buffer.alloc(1024)]));
}

test("safeArchiveEntry rejects paths outside the extraction root", () => {
  for (const entry of ["../minisky", "nested/../../minisky", "/tmp/minisky", "C:\\tmp\\minisky"]) {
    assert.equal(safeArchiveEntry(entry), false, entry);
  }
  assert.equal(safeArchiveEntry("release/minisky"), true);
});

test("verifyChecksum rejects duplicate archive entries", (t) => {
  const root = fixture(t);
  const archiveName = "minisky_linux_amd64.tar.gz";
  const archivePath = join(root, archiveName);
  const checksumsPath = join(root, "checksums.txt");
  writeFileSync(archivePath, "trusted archive");
  const digest = createHash("sha256").update("trusted archive").digest("hex");
  writeFileSync(checksumsPath, `${digest}  ${archiveName}\n${"0".repeat(64)}  ${archiveName}\n`);

  assert.throws(
    () => verifyChecksum(archivePath, checksumsPath, archiveName),
    /exactly once/,
  );
});

test("verifyChecksum uses anchored grammar and exact filenames", (t) => {
  const root = fixture(t);
  const archiveName = "minisky_linux_amd64.tar.gz";
  const archivePath = join(root, archiveName);
  const checksumsPath = join(root, "checksums.txt");
  writeFileSync(archivePath, "trusted archive");
  const digest = createHash("sha256").update("trusted archive").digest("hex");

  for (const line of [
    `${digest}  prefix  ${archiveName}\n`,
    `${digest}   ${archiveName}\n`,
    ` ${digest}  ${archiveName}\n`,
  ]) {
    writeFileSync(checksumsPath, line);
    assert.throws(() => verifyChecksum(archivePath, checksumsPath, archiveName), /invalid checksum entry|exactly once/);
  }
});

test("tar validation rejects links, FIFOs, and devices before extraction", () => {
  const unsafe = [
    ["1", "hard link"],
    ["2", "symbolic link"],
    ["3", "character device"],
    ["4", "block device"],
    ["6", "FIFO"],
  ];
  for (const [type, label] of unsafe) {
    assert.throws(
      () => validateTarGzipArchive(tarGzip(tarEntry("minisky", type, Buffer.alloc(0), "target"))),
      /non-regular/,
      label,
    );
  }
  assert.doesNotThrow(() => validateTarGzipArchive(tarGzip(
    tarEntry("release/", "5"),
    tarEntry("release/minisky", "0", Buffer.from("binary")),
  )));
});

test("extracted binary validation uses lstat and rejects symlinks", (t) => {
  const root = fixture(t);
  const release = join(root, "release");
  mkdirSync(release);
  const regular = join(release, "regular");
  writeFileSync(regular, "binary");
  assert.equal(validateExtractedBinary(regular), regular);

  const linked = join(release, "minisky");
  try {
    symlinkSync(regular, linked, "file");
  } catch (error) {
    t.skip(`runner cannot create a file symlink: ${error}`);
    return;
  }
  assert.throws(() => validateExtractedBinary(linked), /regular file/);
});

test("copyRegularFileNoFollow copies only the opened regular file", (t) => {
  const root = fixture(t);
  const source = join(root, "source");
  const destination = join(root, "destination");
  writeFileSync(source, "trusted binary");

  copyRegularFileNoFollow(source, destination, "test binary");

  assert.equal(readFileSync(destination, "utf8"), "trusted binary");
});

test("copyRegularFileNoFollow rejects symlinks and special files", (t) => {
  const root = fixture(t);
  const regular = join(root, "regular");
  const linked = join(root, "linked");
  writeFileSync(regular, "binary");
  try {
    symlinkSync(regular, linked, "file");
  } catch (error) {
    t.skip(`runner cannot create a file symlink: ${error}`);
    return;
  }
  assert.throws(
    () => copyRegularFileNoFollow(linked, join(root, "linked-copy"), "test binary"),
    /regular file/,
  );
  assert.equal(existsSync(join(root, "linked-copy")), false);

  if (platform !== "win32") {
    assert.throws(
      () => copyRegularFileNoFollow("/dev/null", join(root, "device-copy"), "test binary"),
      /regular file/,
    );
  }
});

test("copyRegularFileNoFollow passes O_NOFOLLOW to open", (t) => {
  const root = fixture(t);
  const source = join(root, "source");
  const target = join(root, "target");
  const destination = join(root, "destination");
  writeFileSync(source, "original");
  writeFileSync(target, "replacement");
  let observedFlags = 0;

  assert.throws(() => copyRegularFileNoFollow(source, destination, "test binary", {
    open(path, flags) {
      observedFlags = flags;
      rmSync(path);
      symlinkSync(target, path, "file");
      return openSync(path, flags);
    },
  }));
  if (constants.O_NOFOLLOW) {
    assert.equal(observedFlags & constants.O_NOFOLLOW, constants.O_NOFOLLOW);
  }
  assert.equal(existsSync(destination), false);
});

test("copyRegularFileNoFollow detects an lstat/open inode swap", (t) => {
  const root = fixture(t);
  const source = join(root, "source");
  const moved = join(root, "source-before-swap");
  const destination = join(root, "destination");
  writeFileSync(source, "original");

  assert.throws(() => copyRegularFileNoFollow(source, destination, "test binary", {
    open(path, flags) {
      renameSync(path, moved);
      writeFileSync(path, "replacement");
      return openSync(path, flags);
    },
  }), /changed while opening/);
  assert.equal(existsSync(destination), false);
});
