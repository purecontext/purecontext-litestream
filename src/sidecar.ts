import { execFileSync, type ChildProcess, spawn } from 'node:child_process'
import { writeFileSync, appendFileSync, unlinkSync, mkdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { loadLitestreamEnv } from './env.js'

export interface LitestreamOptions {
  name?: string
  configDir?: string
  dbPath: string
  replicaPath: string
  bucket?: string
  endpoint?: string
  region?: string
  accessKeyId?: string
  secretAccessKey?: string
  syncInterval?: string
  snapshotInterval?: string
  retention?: string
  l0Retention?: string
  l0RetentionCheckInterval?: string
  envFile?: string
  logFile?: string
}

interface LitestreamState {
  child: ChildProcess
  configPath: string
}

let state: LitestreamState | null = null

function resolveEnv(opts: LitestreamOptions) {
  return {
    bucket: opts.bucket ?? process.env.LITESTREAM_BUCKET,
    endpoint: opts.endpoint ?? process.env.LITESTREAM_ENDPOINT,
    region: opts.region ?? process.env.LITESTREAM_REGION ?? 'auto',
    accessKeyId: opts.accessKeyId ?? process.env.LITESTREAM_ACCESS_KEY_ID,
    secretAccessKey:
      opts.secretAccessKey ?? process.env.LITESTREAM_SECRET_ACCESS_KEY,
  }
}

// Litestream parses config with non-strict YAML: unknown keys are silently
// ignored. Misplacing a key yields a running daemon with defaults and no error,
// so placement below is load-bearing and verified against the 0.5.x binary.
//
// Top level: l0-retention, l0-retention-check-interval, snapshot.{interval,retention}
// Replica level: bucket, path, endpoint, region, credentials, force-path-style, sync-interval
//
// The L0 retention monitor issues 2 LIST calls per check per database regardless
// of whether the database changed (upstream issue #1171). At the 15s default that
// is ~480 requests/hour per idle database; a 24h interval reduces it to ~2/day.
// A value of 0 disables the monitor outright but is rejected by 0.5.16 and
// earlier, so a long interval is used instead.
function assertPositiveDuration(value: string, key: string): void {
  if (/^0+(s|m|h|ms|us|ns)?$/.test(value.trim())) {
    throw new Error(
      `[litestream] ${key} must be greater than 0 (got "${value}"). ` +
        'Litestream refuses to start with a zero duration, which would silently disable backups.',
    )
  }
}

function generateConfigMulti(dbs: LitestreamOptions[]): string {
  const first = dbs[0]
  const l0Retention = first.l0Retention ?? '8760h'
  const l0CheckInterval = first.l0RetentionCheckInterval ?? '24h'
  const snapshotInterval = first.snapshotInterval ?? '24h'
  const snapshotRetention = first.retention ?? '720h'

  assertPositiveDuration(l0Retention, 'l0-retention')
  assertPositiveDuration(l0CheckInterval, 'l0-retention-check-interval')

  const sections = dbs.map((opts) => {
    const env = resolveEnv(opts)
    return `  - path: ${opts.dbPath}
    replicas:
      - type: s3
        bucket: ${env.bucket}
        path: ${opts.replicaPath}
        endpoint: ${env.endpoint}
        region: ${env.region}
        access-key-id: ${env.accessKeyId}
        secret-access-key: ${env.secretAccessKey}
        force-path-style: true
        sync-interval: ${opts.syncInterval ?? '1s'}`
  })

  return `l0-retention: ${l0Retention}
l0-retention-check-interval: ${l0CheckInterval}

snapshot:
  interval: ${snapshotInterval}
  retention: ${snapshotRetention}

dbs:
${sections.join('\n')}
`
}

function configFileName(name: string): string {
  return `litestream-${name}.yml`
}

function killOrphans(configName: string): void {
  let pids: string
  try {
    pids = execFileSync('pgrep', ['-f', configName], { encoding: 'utf-8' }).trim()
  } catch {
    return
  }
  if (!pids) return

  for (const line of pids.split('\n')) {
    const pid = parseInt(line.trim(), 10)
    if (isNaN(pid)) continue

    console.error(`[litestream] killing orphaned replicator (pid ${pid})`)
    try {
      process.kill(pid, 'SIGTERM')
    } catch {}
  }

  const deadline = Date.now() + 2000
  for (const line of pids.split('\n')) {
    const pid = parseInt(line.trim(), 10)
    if (isNaN(pid)) continue

    while (Date.now() < deadline) {
      try {
        process.kill(pid, 0)
      } catch {
        break
      }
      execFileSync('sleep', ['0.1'])
    }

    try {
      process.kill(pid, 'SIGKILL')
    } catch {}
  }
}

function stopPreviousChild(): void {
  if (!state) return

  const { child } = state
  if (child.exitCode !== null || child.killed) return

  console.error(`[litestream] stopping previous replicator (pid ${child.pid})`)
  child.kill('SIGTERM')

  const deadline = Date.now() + 2000
  while (Date.now() < deadline) {
    try {
      process.kill(child.pid!, 0)
    } catch {
      return
    }
    execFileSync('sleep', ['0.1'])
  }

  try {
    child.kill('SIGKILL')
  } catch {
    // already gone
  }
}

export function startLitestream(opts: LitestreamOptions): boolean {
  return startLitestreamAll([opts])
}

export function startLitestreamAll(dbs: LitestreamOptions[]): boolean {
  if (dbs.length === 0) return false

  loadLitestreamEnv(dbs[0].envFile)
  const env = resolveEnv(dbs[0])
  const missing = (['bucket', 'endpoint', 'accessKeyId', 'secretAccessKey'] as const).filter(
    (k) => !env[k],
  )

  if (missing.length > 0) {
    console.error(
      `[litestream] not started — missing: ${missing.join(', ')}. Backups are OFF.`,
    )
    return false
  }

  const name = dbs[0].name ?? `pid-${process.pid}`
  const cfgName = configFileName(name)
  const dir = dbs[0].configDir ?? tmpdir()

  stopPreviousChild()
  killOrphans(cfgName)

  mkdirSync(dir, { recursive: true })
  const configPath = join(dir, cfgName)
  writeFileSync(configPath, generateConfigMulti(dbs))

  const child = spawn('litestream', ['replicate', '-config', configPath], {
    stdio: ['ignore', 'ignore', 'pipe'],
  })

  const logFile = dbs.find((d) => d.logFile)?.logFile
  child.stderr?.on('data', (data: Buffer) => {
    const msg = data.toString().trim()
    if (!msg) return
    const line = `[${new Date().toISOString()}] ${msg}\n`
    console.error(`[litestream] ${msg}`)
    if (logFile) {
      try {
        appendFileSync(logFile, line)
      } catch {}
    }
  })

  child.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'ENOENT') {
      console.error(
        '[litestream] binary not found. Install: brew install benbjohnson/litestream/litestream',
      )
    } else {
      console.error('[litestream] failed to start:', err.message)
    }
    cleanup()
  })

  child.on('exit', () => {
    cleanup()
  })

  state = { child, configPath }

  process.on('SIGINT', stopLitestream)
  process.on('SIGTERM', stopLitestream)

  for (const db of dbs) {
    console.error(
      `[litestream] replicating ${db.dbPath} → s3://${env.bucket}/${db.replicaPath}`,
    )
  }
  return true
}

function cleanup(): void {
  if (!state) return
  try {
    unlinkSync(state.configPath)
  } catch {}
  state = null
}

export function stopLitestream(): void {
  if (!state) return
  state.child.kill('SIGTERM')
  cleanup()
}
