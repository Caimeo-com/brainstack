import { constants, createReadStream, existsSync } from "node:fs";
import { appendFile, chmod, lstat, mkdir, open, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { Buffer } from "node:buffer";
import { abs, shellSingleQuote } from "./paths";

export function run(args: string[], options: { cwd?: string; env?: Record<string, string>; check?: boolean; timeoutMs?: number } = {}) {
  let proc: ReturnType<typeof Bun.spawnSync>;
  try {
    proc = Bun.spawnSync(args, {
      cwd: options.cwd || process.cwd(),
      env: { ...process.env, ...(options.env || {}) },
      stdout: "pipe",
      stderr: "pipe",
      timeout: options.timeoutMs
    });
  } catch (error) {
    const stderr = error instanceof Error ? error.message : String(error);
    if (options.check !== false) {
      throw new Error(`${args.join(" ")} failed\n${stderr}`);
    }
    return { code: 127, stdout: "", stderr, timedOut: false };
  }
  const stdout = proc.stdout.toString();
  const timedOut = proc.exitCode === null;
  const stderr = proc.stderr.toString() || (timedOut && options.timeoutMs ? `timed out after ${options.timeoutMs}ms` : "");
  const code = timedOut ? 124 : proc.exitCode;
  if (options.check !== false && code !== 0) {
    throw new Error(`${args.join(" ")} failed\n${stderr || stdout}`);
  }
  return { code, stdout, stderr, timedOut };
}

function isLocalGitRemote(remote: string): boolean {
  const value = remote.trim();
  return value.startsWith("/") || value.startsWith("~/") || value.startsWith("file://");
}

export function safeGitProtocolArgs(remote: string): string[] {
  return [
    "-c",
    "protocol.ext.allow=never",
    "-c",
    `protocol.file.allow=${isLocalGitRemote(remote) ? "user" : "never"}`
  ];
}

export function safeGitProtocolEnv(remote: string): Record<string, string> {
  return {
    GIT_ALLOW_PROTOCOL: isLocalGitRemote(remote) ? "ssh:https:http:git:file" : "ssh:https:http:git"
  };
}

export async function runWithStdinFile(
  args: string[],
  filePath: string,
  options: { cwd?: string; env?: Record<string, string>; maxBytes?: number; timeoutMs?: number } = {}
) {
  const proc = Bun.spawn(args, {
    cwd: options.cwd || process.cwd(),
    env: { ...process.env, ...(options.env || {}) },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe"
  });
  const stdoutPromise = new Response(proc.stdout).text();
  const stderrPromise = new Response(proc.stderr).text();
  let inputError: unknown = null;
  let timedOut = false;
  let timeout: ReturnType<typeof setTimeout> | null = null;
  if (options.timeoutMs !== undefined && options.timeoutMs > 0) {
    timeout = setTimeout(() => {
      timedOut = true;
      proc.kill();
    }, options.timeoutMs);
    timeout.unref?.();
  }

  try {
    // Open once with O_NOFOLLOW and validate the same descriptor we stream from, so
    // the path cannot be swapped for a symlink or different file between the earlier
    // lstat-based validation and the actual read (TOCTOU).
    const handle = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = await handle.stat();
      if (!info.isFile()) {
        throw new Error(`not a regular file: ${filePath}`);
      }
      if (options.maxBytes !== undefined && info.size > options.maxBytes) {
        throw new Error(`file too large: ${info.size} bytes > ${options.maxBytes}`);
      }
      const buffer = Buffer.alloc(64 * 1024);
      let totalBytes = 0;
      while (true) {
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, -1);
        if (!bytesRead) {
          break;
        }
        totalBytes += bytesRead;
        if (options.maxBytes !== undefined && totalBytes > options.maxBytes) {
          throw new Error(`file grew beyond max bytes while streaming: ${totalBytes} bytes > ${options.maxBytes}`);
        }
        proc.stdin.write(Buffer.from(buffer.subarray(0, bytesRead)));
      }
      await proc.stdin.flush();
    } finally {
      await handle.close().catch(() => undefined);
    }
  } catch (error) {
    inputError = error;
    proc.kill();
  } finally {
    try {
      proc.stdin.end();
    } catch {
      // Process may have exited before stdin was closed.
    }
  }

  const [rawCode, stdout, stderr] = await Promise.all([proc.exited, stdoutPromise, stderrPromise]);
  if (timeout) {
    clearTimeout(timeout);
  }
  const timeoutMessage = timedOut && options.timeoutMs ? `timed out after ${options.timeoutMs}ms` : "";
  return {
    code: timedOut ? 124 : rawCode,
    stdout,
    stderr: [stderr, inputError ? (inputError instanceof Error ? inputError.message : String(inputError)) : "", timeoutMessage].filter(Boolean).join("\n"),
    timedOut
  };
}

export async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

export async function writeText(path: string, text: string, mode?: number): Promise<void> {
  const dir = dirname(path);
  await ensureDir(dir);
  if (mode === undefined) {
    await writeFile(path, text, "utf8");
    return;
  }
  if (existsSync(path)) {
    const existing = await lstat(path);
    if (existing.isSymbolicLink() || !existing.isFile()) {
      throw new Error(`refusing to overwrite non-regular file: ${path}`);
    }
  }
  const tempPath = join(dir, `.${basename(path)}.${process.pid}.${Date.now()}.tmp`);
  const handle = await open(tempPath, "wx", mode);
  try {
    await handle.writeFile(text, "utf8");
    await handle.close();
    await chmod(tempPath, mode);
    await rename(tempPath, path);
  } catch (error) {
    try {
      await handle.close();
    } catch {
      // ignore close failures while preserving the original write error
    }
    await rm(tempPath, { force: true });
    throw error;
  }
}

export async function readExistingPrivateText(path: string): Promise<string> {
  if (!existsSync(path)) {
    return "";
  }
  const info = await lstat(path);
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new Error(`refusing to read non-regular private file: ${path}`);
  }
  if ((info.mode & 0o077) !== 0) {
    await chmod(path, 0o600);
  }
  return await readFile(path, "utf8");
}

export async function writePrivateText(path: string, text: string): Promise<void> {
  await writeText(path, text, 0o600);
}

export async function writeIfMissing(path: string, text: string, mode?: number): Promise<boolean> {
  if (existsSync(path)) {
    return false;
  }
  if (mode === 0o600) {
    await writePrivateText(path, text);
    return true;
  }
  await writeText(path, text, mode);
  return true;
}

export async function readEnvSecretOrFile(envName: string, fileEnvName: string, filePathOverride?: string): Promise<string> {
  const filePath = filePathOverride?.trim() || process.env[fileEnvName]?.trim();
  if (filePath) {
    const absolute = abs(filePath);
    const info = await lstat(absolute);
    if (info.isSymbolicLink() || !info.isFile()) {
      throw new Error(`${fileEnvName} must point to a regular non-symlink file: ${absolute}`);
    }
    if ((info.mode & 0o077) !== 0) {
      throw new Error(`${fileEnvName} must not be group/world accessible; run chmod 600 ${shellSingleQuote(absolute)}`);
    }
    return (await readFile(absolute, "utf8")).split(/\r?\n/)[0]?.trim() || "";
  }
  return process.env[envName]?.trim() || "";
}

export async function setEnvIfBlank(path: string, key: string, value: string): Promise<boolean> {
  if (!value.trim()) {
    return false;
  }
  const existing = await readExistingPrivateText(path);
  const lines = existing.split(/\r?\n/);
  let changed = false;
  let found = false;
  const next = lines.map((line) => {
    if (!line.startsWith(`${key}=`)) {
      return line;
    }
    found = true;
    if (line.slice(key.length + 1).trim()) {
      return line;
    }
    changed = true;
    return `${key}=${value}`;
  });
  if (!found) {
    if (next.length && next[next.length - 1] !== "") {
      next.push(`${key}=${value}`);
    } else if (next.length) {
      next[next.length - 1] = `${key}=${value}`;
    } else {
      next.push(`${key}=${value}`);
    }
    changed = true;
  }
  if (changed) {
    await writePrivateText(path, `${next.join("\n").replace(/\n+$/, "")}\n`);
  }
  return changed;
}
