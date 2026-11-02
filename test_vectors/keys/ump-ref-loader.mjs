// Synchronous module hooks (module.registerHooks) that run the reference's
// TypeScript sources under Node (#560).
//
// REF_ROOT names a directory holding `git archive edf6e03 packages/sdk/src
// packages/sdk/mod.ts packages/wallet/wallet-toolbox/src` of ts-stack, with
// `hash-wasm` and `typescript` installed in its node_modules. `@bsv/sdk` and
// `@bsv/sdk/<path>` resolve to the sdk's sources; an extensionless or `.js`
// relative specifier resolves to its `.ts` file; `.ts` files are transpiled
// with the TypeScript compiler, nothing else is changed.
import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, resolve as resolvePath } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const root = process.env.REF_ROOT
const require = createRequire(join(root, 'package.json'))
const ts = require('typescript')
const sdk = join(root, 'packages/sdk')

function tsFile(base) {
  for (const candidate of [base, `${base}.ts`, base.replace(/\.js$/, '.ts'), join(base, 'index.ts')]) {
    if (candidate.endsWith('.ts') && existsSync(candidate)) return candidate
  }
  return undefined
}

export function resolve(specifier, context, next) {
  let base
  if (specifier === '@bsv/sdk') base = join(sdk, 'mod.ts')
  else if (specifier.startsWith('@bsv/sdk/')) base = join(sdk, 'src', specifier.slice('@bsv/sdk/'.length))
  else if (specifier.startsWith('.') && context.parentURL?.endsWith('.ts')) {
    base = resolvePath(dirname(fileURLToPath(context.parentURL)), specifier)
  } else if (specifier.startsWith('hash-wasm')) {
    return { url: pathToFileURL(join(root, 'node_modules', specifier)).href, shortCircuit: true }
  }
  const file = base === undefined ? undefined : tsFile(base)
  if (file !== undefined) return { url: pathToFileURL(file).href, shortCircuit: true }
  return next(specifier, context)
}

export function load(url, context, next) {
  if (!url.endsWith('.ts')) return next(url, context)
  const source = readFileSync(fileURLToPath(url), 'utf8')
  const out = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2022,
      verbatimModuleSyntax: false,
      isolatedModules: true
    },
    fileName: fileURLToPath(url)
  })
  return { format: 'module', source: out.outputText, shortCircuit: true }
}
