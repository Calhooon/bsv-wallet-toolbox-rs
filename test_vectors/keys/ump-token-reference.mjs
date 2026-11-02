// The reference's UMP token both ways (#560), run on the reference's own code
// (ts-stack edf6e03, packages/wallet/wallet-toolbox/src/CWIStyleWalletManager.ts)
// under Node, with no network: the UMPTokenInteractor is a local stand-in that
// answers the lookup with the token under test and records buildAndSend at a
// fixed outpoint (a locator only; nothing is published).
//
//   REF_ROOT=<export> node ump-token-reference.mjs record   writes ump-token-reference.json
//   REF_ROOT=<export> node ump-token-reference.mjs verify   reads ump-token-rust.json,
//                                                            writes ump-token-rust.result.json
//
// REF_ROOT: `git archive edf6e03 packages/sdk/src packages/sdk/mod.ts
// packages/wallet/wallet-toolbox/src | tar -x -C $REF_ROOT`, then
// `npm install hash-wasm@^4.12.0 typescript@5` there (ump-ref-loader.mjs).
import { registerHooks } from 'node:module'
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const { resolve, load } = await import(pathToFileURL(join(here, 'ump-ref-loader.mjs')).href)
registerHooks({ resolve, load })

const root = process.env.REF_ROOT
const ref = await import(pathToFileURL(join(root, 'packages/wallet/wallet-toolbox/src/CWIStyleWalletManager.ts')).href)
const { Random, Utils, Hash } = await import(pathToFileURL(join(root, 'packages/sdk/mod.ts')).href)
const { CWIStyleWalletManager, PBKDF2_NUM_ROUNDS } = ref
const hex = a => Utils.toHex(a)
const unhex = s => Utils.toArray(s, 'hex')
const OUTPOINT = `${'ab'.repeat(32)}.0`
const PIN = 'edf6e03'

/** A manager on the reference's code with a local interactor and recorders. */
function manager({ token, password, mode }) {
  const seen = { keys: [], published: [] }
  const interactor = {
    findByPresentationKeyHash: async h => (token && hex(h) === hex(token.presentationHash) ? token : undefined),
    findByRecoveryKeyHash: async h => (token && hex(h) === hex(token.recoveryHash) ? token : undefined),
    buildAndSend: async (_wallet, _admin, t) => {
      seen.published.push(t)
      return OUTPOINT
    }
  }
  const m = new CWIStyleWalletManager(
    'admin.test',
    async (primaryKey, _pkm, profileId) => {
      seen.keys.push({ primaryKey, profileId })
      return {}
    },
    interactor,
    async key => {
      seen.recoveryKey = key
      return true
    },
    async (_reason, test) => {
      if (!(await test(password))) throw new Error('password retriever: test failed')
      return password
    },
    undefined,
    undefined,
    { algorithm: 'pbkdf2-sha512', iterations: PBKDF2_NUM_ROUNDS }
  )
  if (mode) m.authenticationMode = mode
  return { m, seen }
}

async function record() {
  const password = 'reference vector password #560'
  const presentationKey = Random(32)
  const { m, seen } = manager({ password })
  await m.providePresentationKey(presentationKey)
  await m.providePassword(password)
  const rootPrimaryKey = seen.keys[0].primaryKey
  const profileId = await m.addProfile('Work')
  await m.switchProfile(profileId)
  const profileKey = seen.keys.at(-1).primaryKey
  const rootPrivilegedKey = await m.getFactor('privilegedKey')
  const recoveryKey = await m.getRecoveryKey()
  if (hex(recoveryKey) !== hex(seen.recoveryKey)) throw new Error('recovery key mismatch')
  const token = m.currentUMPToken
  const v3 = m.serializeUMPToken(token)
  const legacyToken = { ...token, umpVersion: undefined, passwordKdf: undefined }
  const legacy = m.serializeUMPToken(legacyToken)

  // The reference opens its own legacy serialization, at PBKDF2_NUM_ROUNDS.
  const back = m.deserializeUMPToken(legacy)
  if (back.passwordKdf !== undefined) throw new Error('legacy token kept KDF metadata')
  const { m: l, seen: ls } = manager({ token: back, password })
  await l.providePresentationKey(presentationKey)
  await l.providePassword(password)
  if (hex(ls.keys[0].primaryKey) !== hex(rootPrimaryKey)) throw new Error('legacy open mismatch')

  const vector = {
    description:
      'A UMP token written by the reference (ts-stack edf6e03 CWIStyleWalletManager.ts) under Node: createNewUser (providePresentationKey + providePassword) with kdfConfig pbkdf2-sha512 at PBKDF2_NUM_ROUNDS, then addProfile("Work"); serialized by its serializeUMPToken. tokenV3Hex has KDF metadata (version byte 3); tokenLegacyHex is the same token with umpVersion and passwordKdf removed (version byte 2), which the reference opens at 7777 rounds. The outpoint is the local interactor\'s fixed locator.',
    reference: `ts-stack ${PIN}`,
    node: process.version,
    inputs: { password, presentationKeyHex: hex(presentationKey) },
    drawnByReference: { recoveryKeyHex: hex(recoveryKey) },
    expected: {
      rootPrimaryKeyHex: hex(rootPrimaryKey),
      rootPrivilegedKeyHex: hex(rootPrivilegedKey),
      profileIdHex: hex(profileId),
      profileName: 'Work',
      profilePrimaryKeyHex: hex(profileKey),
      passwordSaltHex: hex(token.passwordSalt),
      passwordKdf: token.passwordKdf,
      currentOutpoint: OUTPOINT
    },
    tokenV3Hex: hex(v3),
    tokenLegacyHex: hex(legacy)
  }
  writeFileSync(join(here, 'ump-token-reference.json'), JSON.stringify(vector, null, 2) + '\n')
  console.log('recorded', { v3Bytes: v3.length, legacyBytes: legacy.length, salt: hex(token.passwordSalt) })
}

async function verify() {
  const v = JSON.parse(readFileSync(join(here, 'ump-token-rust.json'), 'utf8'))
  const password = v.inputs.password
  const presentationKey = unhex(v.inputs.presentationKeyHex)
  const probe = manager({ password }).m
  const token = probe.deserializeUMPToken(unhex(v.tokenHex)) // the reference's reader
  const result = { reference: `ts-stack ${PIN}`, node: process.version, checks: {} }
  const check = (name, got, want) => {
    result.checks[name] = { got, want, ok: got === want }
  }

  // presentation key + password (unlockExisting), then the profile (switchProfile).
  const { m, seen } = manager({ token, password })
  await m.providePresentationKey(presentationKey)
  check('authenticationFlow', m.authenticationFlow, 'existing-user')
  await m.providePassword(password)
  check('rootPrimaryKey', hex(seen.keys[0].primaryKey), v.expected.rootPrimaryKeyHex)
  const profiles = m.listProfiles()
  check('profileName', profiles[1]?.name, v.expected.profileName)
  await m.switchProfile(unhex(v.expected.profileIdHex))
  check('profilePrimaryKey', hex(seen.keys.at(-1).primaryKey), v.expected.profilePrimaryKeyHex)
  check('recoveryKey (getRecoveryKey)', hex(await m.getRecoveryKey()), v.expected.recoveryKeyHex)
  check('rootPrivilegedKey (password)', hex(await m.getFactor('privilegedKey')), v.expected.rootPrivilegedKeyHex)
  check('passwordKdf', JSON.stringify(token.passwordKdf), JSON.stringify(v.expected.passwordKdf))
  check('umpVersion', token.umpVersion, 3)

  // presentation key + recovery key (fields 3 and 5).
  const r = manager({ token, password, mode: 'presentation-key-and-recovery-key' })
  await r.m.providePresentationKey(presentationKey)
  await r.m.provideRecoveryKey(unhex(v.expected.recoveryKeyHex))
  check('rootPrimaryKey (recovery)', hex(r.seen.keys[0].primaryKey), v.expected.rootPrimaryKeyHex)

  // A wrong password does not open it.
  const w = manager({ token, password })
  await w.m.providePresentationKey(presentationKey)
  let refused = false
  try {
    await w.m.providePassword('not the password')
  } catch {
    refused = true
  }
  check('wrong password refused', refused, true)

  result.allOk = Object.values(result.checks).every(c => c.ok)
  writeFileSync(join(here, 'ump-token-rust.result.json'), JSON.stringify(result, null, 2) + '\n')
  console.log(JSON.stringify(result.checks, null, 1))
  console.log('allOk', result.allOk)
  if (!result.allOk) process.exitCode = 1
}

await (process.argv[2] === 'verify' ? verify() : record())
process.exit(process.exitCode ?? 0)
