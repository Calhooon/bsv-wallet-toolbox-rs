'use strict'

// Offline witness for toolbox #103. Only public, throwaway fixture keys are used.
const assert = require('node:assert/strict')
const { execFileSync } = require('node:child_process')
const { createRequire, Module } = require('node:module')
const { resolve, join } = require('node:path')

const PIN = 'edf6e03dad39bd7bc4fc6e73e64de3a2b6f805c9'
// tests/fixtures/storage/seed.rs and test_vectors/keys/test_users.json (Alice).
const TEST_KEY = '143ab18a84d3b25e1a13cefa90038411e5d2014590a2a4a57263d1593c8dee1c'
const REMITTANCE = {
  derivationPrefix: 'Pg==',
  derivationSuffix: 'Sg==',
  senderIdentityKey: '0320bbfb879bbd6761ecd2962badbb41ba9d60ca88327d78b07ae7141af6b6c810'
}
const PAYMENT_LOCK = '76a914d4251d021420e279795dd3e75773f283658ea19d88ac'
const FAILING_TXID = '691cadf6c182241b0f32b0547ac702f74e19691e8cdc33d2881f6116b5341a6d'

function git(reference, ...args) {
  return execFileSync('git', ['-C', reference, ...args], { encoding: 'utf8' }).trim()
}

async function loadReference(reference) {
  assert.equal(git(reference, 'rev-parse', 'HEAD'), PIN, 'reference must be at the exact pin')
  assert.equal(git(reference, 'status', '--porcelain'), '', 'reference must be clean')
  const rootRequire = createRequire(join(reference, 'package.json'))
  const walletRequire = createRequire(
    join(reference, 'packages/wallet/wallet-toolbox/package.json')
  )
  const esbuild = rootRequire('esbuild')
  // Use the reference's own toolchain. Compile in memory; leave its files alone.
  const built = await esbuild.build({
    absWorkingDir: reference,
    stdin: {
      contents: `
        export { Wallet } from './packages/wallet/wallet-toolbox/src/Wallet.ts';
        export { StorageKnex } from './packages/wallet/wallet-toolbox/src/storage/StorageKnex.ts';
        export { WalletStorageManager } from './packages/wallet/wallet-toolbox/src/storage/WalletStorageManager.ts';
        export { Beef, KeyDeriver, LockingScript, MerklePath, P2PKH, PrivateKey, Spend,
          Transaction, UnlockingScript } from './packages/sdk/mod.ts';
      `,
      resolveDir: reference,
      loader: 'ts'
    },
    bundle: true,
    platform: 'node',
    format: 'cjs',
    target: 'node24',
    packages: 'external',
    write: false,
    logLevel: 'silent',
    plugins: [
      {
        name: 'pinned-sdk-source',
        setup(build) {
          build.onResolve({ filter: /^@bsv\/sdk(?:\/|$)/ }, ({ path }) => ({
            path:
              path === '@bsv/sdk'
                ? join(reference, 'packages/sdk/mod.ts')
                : join(
                    reference,
                    'packages/sdk/src',
                    path.slice('@bsv/sdk/'.length).replace(/\.ts$/, '') + '.ts'
                  )
          }))
        }
      }
    ]
  })
  const runtime = new Module(join(reference, 'upstream-103-runtime.cjs'), module)
  runtime.filename = runtime.id
  runtime.paths = walletRequire.resolve.paths('knex')
  runtime._compile(built.outputFiles[0].text, runtime.filename)
  return { ...runtime.exports, knex: walletRequire('knex') }
}

function forbidNetwork() {
  const blocked = () => {
    throw new Error('Network access is forbidden in this vector')
  }
  globalThis.fetch = blocked
  require('node:net').Socket.prototype.connect = blocked
  for (const protocol of ['node:http', 'node:https']) {
    require(protocol).request = blocked
    require(protocol).get = blocked
  }
}

function makePayment(api, keyDeriver, unlocking) {
  const { Beef, LockingScript, MerklePath, P2PKH, Transaction, UnlockingScript } = api
  const key = keyDeriver.derivePrivateKey(
    [2, '3241645161d8'],
    'Pg== Sg==',
    REMITTANCE.senderIdentityKey
  )
  const paymentLock = new P2PKH().lock(key.toAddress())
  assert.equal(paymentLock.toHex(), PAYMENT_LOCK, 'BRC-29 script must match the fixture')
  const input = (sourceTXID, hex) => ({
    sourceTXID,
    sourceOutputIndex: 0,
    unlockingScript: UnlockingScript.fromHex(hex),
    sequence: 0xffffffff
  })
  // Reproduce the fixture seeder's version, sequence and index-based lock time.
  const funding = new Transaction(
    1,
    [input('11'.repeat(32), '00')],
    [
      {
        satoshis: 20000,
        lockingScript: LockingScript.fromHex('7551')
      }
    ],
    0
  )
  const parent = new Transaction(
    1,
    [input(funding.id('hex'), '00')],
    [
      {
        satoshis: 20000,
        lockingScript: LockingScript.fromHex('02abcd87')
      }
    ],
    1
  )
  const payment = new Transaction(
    1,
    [input(parent.id('hex'), unlocking)],
    [
      {
        satoshis: 9999,
        lockingScript: paymentLock
      }
    ],
    2
  )
  const proof = MerklePath.fromCoinbaseTxidAndHeight(parent.id('hex'), 100001)
  const beef = new Beef()
  const bumpIndex = beef.mergeBump(proof)
  beef.mergeRawTx(parent.toBinary(), bumpIndex)
  beef.mergeRawTx(payment.toBinary())
  return { parent, payment, proof, bytes: beef.toBinaryAtomic(payment.id('hex')) }
}

function scriptValid(api, parent, payment) {
  const spend = new api.Spend({
    sourceTXID: parent.id('hex'),
    sourceOutputIndex: 0,
    sourceSatoshis: parent.outputs[0].satoshis,
    lockingScript: parent.outputs[0].lockingScript,
    transactionVersion: payment.version,
    otherInputs: [],
    outputs: payment.outputs,
    inputIndex: 0,
    unlockingScript: payment.inputs[0].unlockingScript,
    inputSequence: payment.inputs[0].sequence,
    lockTime: payment.lockTime
  })
  try {
    return spend.validate()
  } catch (error) {
    assert.match(error.message, /top stack element must be truthy/)
    return false
  }
}

async function runCase(api, name, unlocking, expectedScriptValid) {
  const keyDeriver = new api.KeyDeriver(api.PrivateKey.fromHex(TEST_KEY))
  assert.equal(keyDeriver.identityKey, REMITTANCE.senderIdentityKey)
  const { parent, payment, proof, bytes } = makePayment(api, keyDeriver, unlocking)
  const txid = payment.id('hex')
  if (!expectedScriptValid)
    assert.equal(txid, FAILING_TXID, 'payment must match the failing fixture')
  const evaluated = scriptValid(api, parent, payment)
  assert.equal(evaluated, expectedScriptValid, 'independent script evaluation')
  const root = proof.computeRoot(parent.id('hex'))
  let rootChecks = 0
  let mockPosts = 0
  const tracker = {
    async isValidRootForHeight(candidate, height) {
      rootChecks++
      return candidate === root && height === 100001
    },
    async currentHeight() {
      return 880000
    }
  }
  const services = new Proxy(
    {
      async getChainTracker() {
        return tracker
      },
      async getHeight() {
        return 880000
      },
      async postBeef(beef, txids) {
        mockPosts++
        assert.deepEqual(txids, [txid])
        assert.equal(beef.findTxid(txid).tx.toHex(), payment.toHex())
        return [
          {
            name: 'fixture',
            status: 'success',
            notes: [],
            txidResults: txids.map((id) => ({ txid: id, status: 'success' }))
          }
        ]
      }
    },
    {
      get(target, property) {
        assert.ok(Object.hasOwn(target, property), `Unexpected service call: ${String(property)}`)
        return target[property]
      }
    }
  )
  const database = api.knex({
    client: 'better-sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
    pool: { min: 1, max: 1 }
  })
  const active = new api.StorageKnex({ chain: 'test', knex: database })
  let wallet
  try {
    await active.migrate(
      'upstream-103',
      api.PrivateKey.fromHex('1'.repeat(64)).toPublicKey().toString()
    )
    await active.makeAvailable()
    const storage = new api.WalletStorageManager(keyDeriver.identityKey, active)
    await storage.makeAvailable()
    wallet = new api.Wallet({
      chain: 'test',
      keyDeriver,
      storage,
      services,
      actionBatchMode: 'legacy'
    })
    console.log(`${name}.unlockingScript: ${unlocking}`)
    console.log(`${name}.scriptValid: ${evaluated}`)
    console.log(`${name}.txid: ${txid}`)
    const result = await wallet.internalizeAction({
      tx: bytes,
      description: expectedScriptValid ? 'A valid unproven payment' : 'A failing unproven payment',
      outputs: [
        { outputIndex: 0, protocol: 'wallet payment', paymentRemittance: { ...REMITTANCE } }
      ]
    })
    console.log(`${name}.accepted: ${result.accepted}`)
    const listed = await wallet.listOutputs({ basket: 'default' })
    console.log(`${name}.outputs: ${JSON.stringify(listed.outputs)}`)
    const { userId } = await storage.getAuth()
    const rows = await active.findOutputs({ partial: { userId, txid } })
    assert.equal(rows.length, 1, 'one received coin in storage')
    const coin = rows[0]
    const transactions = await active.findTransactions({ partial: { userId, txid } })
    assert.equal(transactions.length, 1)
    const transaction = transactions[0]
    console.log(`${name}.coin.spendable: ${coin.spendable}`)
    console.log(`${name}.coin.satoshis: ${coin.satoshis}`)
    console.log(`${name}.transaction.status: ${transaction.status}`)
    console.log(`${name}.transaction.isOutgoing: ${transaction.isOutgoing}`)
    console.log(`${name}.rootChecks: ${rootChecks}`)
    console.log(`${name}.mockPosts: ${mockPosts}`)
    assert.equal(result.accepted, true)
    assert.equal(listed.totalOutputs, 1)
    assert.equal(listed.outputs[0].outpoint, `${txid}.0`)
    assert.equal(coin.spendable, true)
    assert.equal(coin.satoshis, 9999)
    assert.equal(coin.change, true)
    assert.equal(coin.spentBy, undefined)
    assert.equal(transaction.isOutgoing, false)
    assert.equal(transaction.status, 'unproven')
    assert.ok(rootChecks >= 2, 'both wallet and storage check the root')
  } finally {
    if (wallet) await wallet.destroy()
    else await active.destroy()
  }
}

async function main() {
  assert.equal(process.argv.length, 3, 'usage: node vector.cjs /path/to/ts-stack-edf6e03')
  const reference = resolve(process.argv[2])
  const api = await loadReference(reference)
  forbidNetwork()
  console.log(`reference: ${PIN}`)
  console.log(`node: ${process.version}`)
  console.log(`date: ${new Date().toISOString().slice(0, 10)}`)
  await runCase(api, 'failing', '02abce', false)
  await runCase(api, 'valid', '02abcd', true)
  console.log('checks: passed')
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
