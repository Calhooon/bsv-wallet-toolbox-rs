# Storage JSON-RPC wire audit (#202)

This audits every literal `rpc_call` in `src/storage/client/storage_client.rs`
against `ts-stack@edf6e03dad39bd7bc4fc6e73e64de3a2b6f805c9`. There are 41 call
sites and 40 distinct method names. `insertCertificateAuth` has two callers.
The request witnesses are `tests/fixtures/data/storage-wire/<method>.json`,
plus `insertCertificateAuth_with_fields.json`, read by `tests/storage_wire.rs`.
They exercise the real client against loopback mockito without authentication,
signing, payment, broadcasting, or explorer calls. A null mock response is only
a terminator for request capture; it makes no claim about result compatibility.

The table records the beta.1 dependency (`bsv-rs =0.9.0-beta.1`). Fields owned by
bsv-rs are not rewritten here. Its #105 / PR #271 is the coordinated beta.2
change. The ordinary storage fixtures under `tests/fixtures/data/storage/`
are local storage arguments, including `inputBeef`, and are not wire fixtures.

## Sources and notation

All TS paths below are at the pin above. `WT` means
`packages/wallet/wallet-toolbox/src/`; `SDK` means `packages/sdk/src/wallet/`.

* `C`: WT `storage/remoting/StorageClientBase.ts`, which owns the methods;
  `StorageClient.ts:114-194` serializes their parameters and reads results.
* `S`: WT `storage/remoting/StorageServer.ts:71-111` is the allowlist;
  `:629-670` decodes and dispatches; `:891-945` invokes the storage method
  with positional parameters. Its auth binding is at `:1033-1163`.
* `I`: WT `sdk/WalletStorage.interfaces.ts` supplies storage argument/result
  names to both sides. `V`: SDK `validationHelpers.ts` supplies validated args.
* `T/X`: WT `storage/schema/tables/TableX.ts`; `W`: SDK `Wallet.interfaces.ts`.
* BRC-100: `BRCs@5119a9b`, `wallet/0100.md:676` names `inputBEEF`.
  Issue #202 and every comment were read; there were no comments.
* Stack context: `bsv-stack-lean@91d7b9839a828ee2be38fed28c9b515eeaf5d3a0`,
  `docs/charters/{wallet,storage,broadcaster,tracker}.md`, `lean/Wallet.lean`,
  `docs/lanes/l2-README.md`. CLI rules: `origin/beta:docs/lanes/README.md`.

`H` is a hex string, `B` a base64 string, `A` a JSON array of byte numbers,
`N` a JSON number, `S` a string, `F` a boolean, `D` an RFC3339 date string,
`O` an object, and `[]` an array. `?` means optional. Rust options without
`skip_serializing_if` produce explicit null; TS undefined properties are absent.
Both are absence at these reference readers unless a row says otherwise.
Named groups below expand every field in a method cell; fields sharing a row
share its verdict. Counts count these auditable field groups once, not once per
reuse. Method-only differences are counted as one group per method.

Counts for the two field tables plus the 17 toolbox-only method rows: same 42;
differs and fixed 16; moves with bsv-rs #105 6; owner's word 10;
reference-only 13; toolbox-only 20. The 16 reference-only allowlist names below
are a separate capability inventory, not additional field groups.

The reference also negotiates optional binary JSON: C `:888-890`,
`StorageClient.ts:130-155`, `BinaryJson.ts:234-260`, S `:630-635`. Plain JSON
encodes binary as A. Opted-in binary JSON uses `{$bsvBinary:"base64",data:B}`.
The toolbox does not request that protocol. Plain A remains accepted, so this
is a reference-only capability, not a reason to convert the toolbox to base64.

## Method inventory

Each row's toolbox, reference client, and server cells describe positional
`params`, not an enclosing named argument. Group verdicts are below. All
requests also carry `jsonrpc:"2.0"`, `method:S`, `params:[]`, `id:N`.

| Method (witness filename stem) | Toolbox request | Reference client sends | Reference server reads | Result and verdict groups |
|---|---|---|---|---|
| `makeAvailable` | `[]` | `[]` C398-410 | `[]` S99 | Settings; same |
| `migrate` | `[storageName:S]` | same C434-435 | ignored S1040-1043 | Reference returns no result; Rust expects S; owner M1 |
| `destroy` | `[]` | same C423-424 | ignored S1040-1043 | Rust discards Value; same |
| `findOrInsertUser` | `[identityKey:H]` | same C632-633 | same S1046-1050 | `{user:User,isNew:F}`; same |
| `findCertificatesAuth` | `[Auth, FindCertificates]` | `[Auth,{Base,partial:{userId:N?},certifiers:H[]?,types:B[]?,includeFields:F?}]` C730-737 | same I264-269 | `Certificate[]`; Q1 and entity groups |
| `findOutputBaskets` | `[Auth, FindBaskets]` | method `findOutputBasketsAuth`, `[Auth,{Base,partial:{userId:N?,name:S?}}]` C751-754 | both method spellings accepted S87-88,900-902 | `Basket[]`; Q2; method alias same |
| `findOutputsAuth` | `[Auth,{Base,partial:{userId:N?,basketId:N?,txid:H?,vout:N?},noScript:F?,txStatus:S[]?}]` | same C767-771, I275-279 | same | `Output[]`; Q3 and entity groups |
| `findProvenTxReqs` | `[{Base,partial:{},status:S[]?,txids:H[]?}]` | same C783-787, I505-509 | same; server supplies Auth S1054-1060 | `ProvenTxReq[]`; same and E4 |
| `findTransactions` | `[{Base,partial:{},status:S[]?,noRawTx:F?}]` | absent | absent S71-111 | `Transaction[]`; toolbox-only |
| `listActions` | `[Auth,ListActions]` | `[Auth,ValidListActions]` C690-693 | same V1656-1670 | bsv-rs `ListActionsResult`; L1, D7 and wallet result fields below |
| `listOutputs` | `[Auth,ListOutputs]` | `[Auth,ValidListOutputs]` C702-705 | same V1589-1603 | bsv-rs `ListOutputsResult`; L2, D8 |
| `listCertificates` | `[Auth,ListCertificates]` | same fields C714-717 | V1166-1180 | bsv-rs `ListCertificatesResult`; L3, D6 |
| `abortAction` | `[Auth,{reference:B}]` | same C622-623 | same | `{aborted:F}`; same |
| `createAction` | `[Auth,Create]` | `[Auth,ValidCreateActionArgs]` C478-508 | I211, V714-733 | `CreateResult`; C1-C6, D1-D4, R1 |
| `processAction` | `[Auth,Process]` | same C521-522 | I352-362 | `ProcessResult`; same |
| `internalizeAction` | `[Auth,Internalize]` | same names C467-468 | W601-607, I364-377 | `InternalizeResult`; D5 |
| `markInternalizedTxFailed` | `[txid:H]` | absent | absent | null; toolbox-only; method-not-found ignored |
| `insertCertificateAuth` | `[Auth,Certificate]`; second caller adds `fields:CertificateField[]`, removes `certificateFieldId` | `[Auth,CertificateX]` C678-680 | I230, T/Certificate | `N`; E1-E3 and reference-only deletion field |
| `insertCertificateFieldAuth` | `[Auth,CertificateField]` | absent | absent | N; toolbox-only, entity naming still audited |
| `relinquishCertificate` | `[Auth,{certificateType:B,serialNumber:B,certifier:H}]` | `[Auth,{type:B,serialNumber:B,certifier:H}]` C799-800 | same W936-940 | N; D6 |
| `relinquishOutput` | `[Auth,{basket:S,output:S}]` | same C812-813 | W648-651 | N; same (output is `H.N`) |
| `updateTransactionStatusAfterBroadcast` | `[txid:H,success:F]` | absent | absent | null; toolbox-only; method-not-found ignored |
| `reviewStatus` | `[Auth,agedLimit:D]` | absent | absent | `{log:S}`; toolbox-only |
| `purgeData` | `[Auth,{maxAgeDays:N,purgeCompleted:F,purgeFailed:F}]` | absent | absent | `{count:N,log:S}`; toolbox-only; I414-434 uses separate millisecond ages and `purgeSpent` locally |
| `beginStorageTransaction` | `[]` | absent | absent | Value discarded; toolbox-only |
| `commitStorageTransaction` | `[]` | absent | absent | Value discarded; toolbox-only |
| `rollbackStorageTransaction` | `[]` | absent | absent | Value discarded; toolbox-only |
| `findOrInsertSyncStateAuth` | `[Auth,storageIdentityKey:H,storageName:S]` | same C657-668 | same | `{syncState:SyncState,isNew:F}`; E12 |
| `setActive` | `[Auth,newActiveStorageIdentityKey:H]` | same C1056-1057 | same | N; same |
| `getSyncChunk` | `[SyncArgs]` | same core C849-884, optional transfer fields | same I596-660 | `SyncChunk`; Y1, entity groups |
| `processSyncChunk` | `[SyncArgs,SyncChunk]` | same C825-840, binary conversion per `syncChunkBinary.ts` | same I687-723, S1061-1065 | `{done:F,maxUpdated_at:D?,updates:N,inserts:N,error:O?}`; Y2-Y3 and entity groups |
| `synchronizeTransactionStatuses` | `[]` | absent | absent | `{txid:H,status:S,blockHeight:N?,blockHash:H?,merkleRoot:H?,merklePath:A?}[]`; toolbox-only |
| `sendWaitingTransactions` | `[minTransactionAge:N milliseconds]` | absent | absent | `ProcessResult?`; toolbox-only |
| `abortAbandoned` | `[timeout:N milliseconds]` | absent | absent | null; toolbox-only |
| `unFail` | `[]` | absent | absent | null; toolbox-only |
| `monitorReviewStatus` | `[]` | absent | absent | `{log:S}`; toolbox-only |
| `monitorPurgeData` | `[{maxAgeDays:N,purgeCompleted:F,purgeFailed:F}]` | absent | absent | `{count:N,log:S}`; toolbox-only |
| `tryAcquireTaskLock` | `[taskName:S,instanceId:S,ttl:N milliseconds]` | absent | absent | F; toolbox-only |
| `releaseTaskLock` | `[taskName:S,instanceId:S]` | absent | absent | null; toolbox-only |
| `updateProvenTxReqWithNewProvenTx` | `[{provenTxReqId:N,provenTx:ProvenTx}]` | flat object C1040-1044, I552-563 | flat object; server supplies Auth S1054-1060 | M2-M3; existing shape cannot supply the reference's arguments |

The reference-only method names in the storage allowlist are `abortActionBatch`,
`activateNoSendExpiry`, `adminStats`, `armNoSendExpiry`, `beginActionBatch`,
`commitActionBatch`, `commitActionBatchByDigest`, `extendActionBatch`,
`findOutputBasketsAuth` (the accepted alias of our spelling), `getCapabilities`,
`getSettings`, `getSyncCheckpoint`, `prepareActionBatchCommit`,
`prepareNoSendExpiry`, `renewActionBatch`, and `resumeActionBatch`. The reference
also dispatches optional sync-transfer methods outside that allowlist. These
are reference-only capabilities, not calls missing a toolbox witness.

## Shared arguments and results, field by field

Status `differs and fixed` marks a toolbox-owned wire correction committed
with its changed witness. `moves with bsv-rs #105` is deliberately left at beta.1.
`owner's word` requires information or a shape the current public type does not
hold, or a validation/encoding decision beyond renaming this storage wire.

| Group | Toolbox names and encoding | Reference client sends / reads | Reference server reads / returns | Verdict |
|---|---|---|---|---|
| Auth | `identityKey:H,userId:N?,isActive:F?` | same I239-243 | binds identity and replaces user fields S1121-1147 | same |
| Base | `since:D?,paged:{offset:N?,limit:N?}?,orderDescending:F?` | same I245-254 | same; normalizes pagination S761-771 | same |
| Base extra | absent | `trx:O?` I248 | local transaction handle, optional | reference-only |
| Q1 | `userId:N?` at root | `partial:{userId:N?}` I264-269 | `partial` object | differs and fixed |
| Q1 common | `certifiers:H[]?,types:B[]?,includeFields:F?` | same | same | same |
| Q2 | `userId:N?,name:S?` at root | `partial:{userId:N?,name:S?}` I271-273 | `partial` object | differs and fixed |
| Q1/Q2 extra filters | absent | `partial:Partial<Certificate/Basket>` | all table columns can filter | reference-only |
| Q3 | `txStatus:["noSend","nonFinal"]` for these Rust variants | `["nosend","nonfinal"]` I279, WT sdk/types.ts | same | differs and fixed |
| C1 | `inputBeef:A?` | `inputBEEF:A?` V716,C479-505 | `inputBEEF:A?` V756-757 | differs and fixed |
| C2 | `description:S,lockTime:N?,version:N?,labels:S[]?` | same names, validated values populated V714-725 | same | same |
| C3 | `isNewTx:F,isNoSend:F,isDelayed:F,isSendWith:F,isRemixChange:F,isSignAction:F,includeAllSourceTransactions:F` | same names V699-733 | same | same |
| C4 | `inputs[].inputDescription:S,unlockingScript:H?,unlockingScriptLength:N?,sequenceNumber:N?` | same names V569-576 | same | same |
| C4 outpoint | `inputs[].outpoint:S (H.N)` | `{txid:H,vout:N}` V570,606 | validated OutPoint object | owner's word; bsv-rs public Outpoint encoding is not changed |
| C5 | `outputs[].lockingScript:H,satoshis:N,outputDescription:S,basket:S?,customInstructions:S?,tags:S[]?` | same names V615-622 | same | same |
| C6 | `options.signAndProcess:F?,acceptDelayedBroadcast:F?,trustSelf:"known"?,noSend:F?,randomizeOutputs:F?` | same names V658-688 | same | same |
| D1 | `options.knownTxids:A[]?` | `H[]` V662-664 | H[] | moves with bsv-rs #105 |
| D2 | `options.sendWith:A[]?` | `H[]` V670 | H[] | moves with bsv-rs #105 |
| D3 | `options.returnTxidOnly:F?` | `returnTXIDOnly:F` V665 | same | moves with bsv-rs #105 |
| D4 | `options.noSendChange:S[]? (H.N)` | `{txid:H,vout:N}[]` V667-669 | validated OutPoint objects | owner's word; distinct from #105's public BRC form |
| C missing defaults | optional arrays, options and numbers may be absent; own derived defaults | fully validated V751-806, default delayed true, flags derived from validated inputs | uses validated values | owner's word; validation/default behavior is not a wire rename |
| Create extra | absent | `isTestWerrReviewActions:F,randomVals:N[]?,logger:O?` V710,728,565 | optional behavior/test hooks | reference-only |
| Process | `isNewTx:F,isSendWith:F,isNoSend:F,isDelayed:F,reference:B?,txid:H?,rawTx:A?,sendWith:H[]` | same I352-361 | same | same |
| Process extra | absent | `logger:O?` I361 | remote logging object | reference-only |
| ProcessResult | `sendWithResults:{txid:H,status:S}[]?,notDelayedResults:{txid:H,status:S,competingTxs:H[]?,competingBeef:A?}[]?,log:S?` | same I384-406 | same; statuses `success,doubleSpend,serviceError,invalidTx` | same |
| Process logger | `log:S?` | configured remote logger may return `{logs:O[]}` StorageClient.ts25-72 | S1110-1119 can replace log with object | owner's word; String slot cannot preserve structured logs |
| D5 | `Internalize.tx:H` | `tx:A` W602, C467-468 | A | moves with bsv-rs #105 |
| Internalize | `description:S,labels:S[]?,seekPermission:F?,outputs[].{outputIndex:N,protocol:S,paymentRemittance:{derivationPrefix:B,derivationSuffix:B,senderIdentityKey:H}?,insertionRemittance:{basket:S,customInstructions:S?,tags:S[]?}?}` | same W564-607 | same | same |
| InternalizeResult | `accepted:F,isMerge:F,txid:H,satoshis:N,sendWithResults:[]?,notDelayedResults:[]?` | same I364-377 | same | same |
| L1 | `labels:S[],labelQueryMode:S?,includeLabels:F?,includeInputs:F?,includeInputSourceLockingScripts:F?,includeInputUnlockingScripts:F?,includeOutputs:F?,includeOutputLockingScripts:F?,limit:N?,offset:N?,seekPermission:F?` | same names, defaults populated V1656-1670 | same | same |
| L2 common | `basket:S,tags:S[]?,tagQueryMode:S?,includeCustomInstructions:F?,includeTags:F?,includeLabels:F?,limit:N?,offset:N?,seekPermission:F?` | same names V1589-1603 | same | same |
| L2 include | `include:S?` | `includeLockingScripts:F,includeTransactions:F,knownTxids:H[]` V1640-1650 | those validated flags | owner's word; bsv-rs public args are not validated storage args |
| L3 | `certifiers:H[],types:B[],limit:N?,offset:N?,privileged:F?,privilegedReason:S?` | same names V1166-1180 | same | same |
| L3 partial | absent | `partial:{type:B?,serialNumber:B?,certifier:H?,subject:H?,revocationOutpoint:S?,signature:H?}?` V1167-1174 | same | reference-only |
| D6 | `relinquishCertificate.certificateType:B`; `listCertificates.certificates[].certificate:O` with `certificateType:B` | `type:B`; flat certificate entries | W936-940, W905-908 | moves with bsv-rs #105 |
| D7 | `ListActionsResult.totalActions:N,actions:[]` with `actions[].txid:H` | same W540-557; beta.1 already uses `hex_txid` | same | same |
| D8 | `ListOutputsResult.totalOutputs:N,outputs:[],beef:H?` | `totalOutputs:N,outputs:[],BEEF:A?` W644 | same | moves with bsv-rs #105 |
| WalletAction fields | `txid:H,satoshis:N,status:S,isOutgoing:F,description:S,labels:S[]?,version:N,lockTime:N,inputs:[]?,outputs:[]?` | same W540-552 | same | same |
| WalletActionInput fields | `sourceOutpoint:S,sourceSatoshis:N,sourceLockingScript:H?,unlockingScript:H?,inputDescription:S,sequenceNumber:N` | same W499-507 | same | same |
| WalletActionOutput fields | `satoshis:N,lockingScript:H?,spendable:F,customInstructions:S?,tags:S[],outputIndex:N,outputDescription:S,basket:S` | same W509-524 | same | same |
| WalletOutput fields | `satoshis:N,lockingScript:H?,spendable:F,customInstructions:S?,tags:S[]?,outpoint:S,labels:S[]?` | same W526-538 | same | same |
| Certificate result fields | `subject:H,serialNumber:B,certifier:H,revocationOutpoint:S,signature:H,fields:{fieldName:S},keyring:{fieldName:B}?,verifier:S?` under the certificate wrapper except keyring/verifier | same names, flat W851-868,900-908; wrapper/type difference is D6 | same | same |
| R1 | `CreateResult.inputs[].inputType:S` | `type:S` I295 | `type:S` | differs and fixed, accept reference alias |
| CreateResult common | `inputBeef:A?,inputs:[],outputs:[],noSendChangeOutputVouts:N[]?,derivationPrefix:B,version:N,lockTime:N,reference:B` | same I309-318; result really is `inputBeef` | same | same |
| CreateResult input | `vin:N,sourceTxid:H,sourceVout:N,sourceSatoshis:N,sourceLockingScript:H,sourceTransaction:A?,unlockingScriptLength:N,providedBy:S,spendingDescription:S?,derivationPrefix:B?,derivationSuffix:B?,senderIdentityKey:H?` | same I283-300 | same | same |
| CreateResult output | `vout:N,satoshis:N,lockingScript:H,providedBy:S,purpose:S?,derivationSuffix:B?,basket:S?,tags:S[],outputDescription:S?,customInstructions:S?` | same I302-307,V615-622 | same | same |
| Y1 | `fromStorageIdentityKey:H,toStorageIdentityKey:H,identityKey:H,since:D?,maxRoughSize:N,maxItems:N,offsets:{name:S,offset:N}[]` | same I606-660 | same | same |
| Y1 extras | absent | `includeNextCheckpoint:F?,syncStateId:N?,requireMatchingCheckpoint:F?,includeTotals:F?,syncTransferVersion:N?` I596-660,C851-852 | supported as optional | reference-only |
| SyncChunk | `fromStorageIdentityKey:H,toStorageIdentityKey:H,userIdentityKey:H,user:User?,provenTxs:ProvenTx[]?,provenTxReqs:ProvenTxReq[]?,outputBaskets:Basket[]?,txLabels:Label[]?,outputTags:Tag[]?,transactions:Transaction[]?,txLabelMaps:LabelMap[]?,commissions:Commission[]?,outputs:Output[]?,outputTagMaps:TagMap[]?,certificates:Certificate[]?,certificateFields:CertificateField[]?` | same containers I687-708 | same, entity fields below | same |
| SyncChunk extras | absent | `totals:{totalRecords:N,records:O}?` I662-680 | same | reference-only |
| Y2 | `ProcessSyncChunkResult.maxUpdatedAt:D?` | `maxUpdated_at:D?` I710-719 | `maxUpdated_at:D?` | differs and fixed, accept reference alias |
| Y2 common | `done:F,updates:N,inserts:N` | same | same | same |
| Y3 | `error:S?` | `error:WalletError?` I718 | error object | owner's word; cannot preserve error object in current String slot |
| Y2 extra | absent | `nextCheckpoint:O?` I712 | same | reference-only |
| M1 | `migrate` expects S | void from ignored migration | S1040-1043 | owner's word; lifecycle behavior, not field name |
| M2 | `{provenTxReqId:N,provenTx:ProvenTx}` | `{provenTxReqId:N,txid:H,attempts:N,status:S,history:S,height:N,index:N,blockHash:H,merkleRoot:H,merklePath:A}` I552-563 | same | owner's word; missing attempts/status/history, no inferred values |
| M3 | `{txsUpdated:N,reqsUpdated:N,provenTxId:N}` | `{status:S,history:S,provenTxId:N,notified:F?,notify:S?,log:S?}` I565-576 | same | owner's word; counts are not status/history aliases |

## Entity fields (requests and results)

These are toolbox-owned types. The response aliases already present in
`entities/mod.rs` remain accepted. Wire-only adapters translate requests
without changing those types or their local serialization.

| Group | Toolbox names and encoding | Reference client sends / reads | Reference server reads / returns | Verdict |
|---|---|---|---|---|
| E1 all entities | `createdAt:D,updatedAt:D` | `created_at:D,updated_at:D` T/* | same; old Rust already accepts those aliases | differs and fixed, outbound only |
| E2 Certificate | `certType:B` | `type:B` T/Certificate:10 | same; incoming alias already present | differs and fixed, outbound only |
| Certificate common | `certificateId:N,userId:N,serialNumber:B,certifier:H,subject:H,verifier:H?,revocationOutpoint:S,signature:H` | same T/Certificate:8-17 | same | same |
| Certificate result fields | no field collection on the returned `TableCertificate` | `fields:CertificateField[]?` on `TableCertificateX`, T/Certificate:21; C730-736 reads them for `includeFields` | same | reference-only; preserving requested fields needs an owner's public type decision |
| E3 CertificateField | `userId:N,certificateId:N,fieldName:S,fieldValue:S,masterKey:B` | same T/CertificateField:8-12 | same | same |
| E4 Transaction, ProvenTxReq | `inputBeef:A?` | `inputBEEF:A?` T/Transaction:35,T/ProvenTxReq:33 | same | differs and fixed, outbound rename and inbound alias |
| E5 Transaction | status `"noSend","nonFinal"` | `"nosend","nonfinal"` WT sdk/types.ts:54-84 | same; incoming aliases already present | differs and fixed, outbound only |
| E6 Output | `outputType:S` | `type:S` T/Output:20 | same | differs and fixed, both directions |
| E7 Basket | `numberOfDesiredUtxos:N,minimumDesiredUtxoValue:N` | `numberOfDesiredUTXOs:N,minimumDesiredUTXOValue:N` T/OutputBasket:9-10 | same; incoming aliases already present | differs and fixed, outbound only |
| E8 Label, LabelMap | `labelId:N` | `txLabelId:N` T/TxLabel:6,T/TxLabelMap:6 | same; incoming aliases already present | differs and fixed, outbound only |
| E9 Tag, TagMap | `tagId:N` | `outputTagId:N` T/OutputTag:6,T/OutputTagMap:6 | same; incoming aliases already present | differs and fixed, outbound only |
| E10 Commission | `payerLockingScript:A` | `lockingScript:A` T/Commission:12 | same; incoming alias already present | differs and fixed, outbound only |
| E11 User | `userId:N,identityKey:H,activeStorage:H?` | same T/User | same | same |
| E12 SyncState | `whenLastSyncStarted:D?` | `when:D?` T/SyncState:14,C667 | same | differs and fixed, inbound alias |
| SyncState common | `syncStateId:N,userId:N,storageIdentityKey:H,storageName:S,status:S,init:F,refNum:S,syncMap:S,satoshis:N?,errorLocal:S?,errorOther:S?` | same T/SyncState | same | same |
| Settings | `storageIdentityKey:H,storageName:S,chain:S,dbtype:S?,maxOutputScript:N` | same T/Settings | same | same |
| Settings extension | `settingsId:N` | absent | absent | toolbox-only |
| Settings capabilities | absent | `syncCheckpointVersion:N?,syncTransfer:O?` S927-936 | advertised | reference-only |
| Transaction common | `transactionId:N,userId:N,txid:H?,reference:B,description:S,satoshis:N,version:N,lockTime:N,rawTx:A?,isOutgoing:F` | same T/Transaction | same | same |
| Transaction proof | `proofTxid:H?` | `provenTxId:N?` T/Transaction:10 | same | owner's word; different value and relationship, not a rename |
| Transaction expiry | absent | `noSendExpiryMode:S?,noSendExpiryValue:N?,noSendExpiryDeadline:N?,noSendExpiryState:S?,noSendExpiryAnchorTxid:H?,noSendExpiryAnchorVout:N?,noSendExpiryReleasedAt:N?,noSendExpiryObservedAt:N?,noSendExpiryReclaimTxid:H?,noSendExpiryReclaimRawTx:A?,noSendExpiryReclaimDerivationPrefix:B?,noSendExpiryReclaimDerivationSuffix:B?,noSendExpiryReclaimSatoshis:N?` T/Transaction:40-54 | same | reference-only |
| ProvenTx | `provenTxId:N,txid:H,height:N,index:N,blockHash:H,merkleRoot:H,merklePath:A,rawTx:A` | same T/ProvenTx | same | same |
| ProvenTxReq common | `provenTxReqId:N,provenTxId:N?,txid:H,status:S,attempts:N,history:S,notified:F,notify:S,rawTx:A?,batch:S?` | same T/ProvenTxReq | same | same |
| ProvenTxReq status extras | `pending,inProgress,failed,notFound` | absent from WT sdk/types.ts:54-67 | absent | toolbox-only |
| ProvenTxReq extras | absent | `wasBroadcast:F?,rebroadcastAttempts:N?` T/ProvenTxReq:43-48 | same | reference-only |
| Output common | `outputId:N,userId:N,transactionId:N,basketId:N?,txid:H,vout:N,satoshis:N,lockingScript:A?,scriptLength:N,scriptOffset:N,providedBy:S,purpose:S?,outputDescription:S?,spentBy:N?,sequenceNumber:N?,spendingDescription:S?,spendable:F,change:F,derivationPrefix:B?,derivationSuffix:B?,senderIdentityKey:H?,customInstructions:S?` | same T/Output | same | same |
| Basket common | `basketId:N,userId:N,name:S` | same T/OutputBasket | same | same |
| Label common | `userId:N,label:S` | same T/TxLabel | same | same |
| Tag common | `userId:N,tag:S` | same T/OutputTag | same | same |
| LabelMap common | `transactionId:N` | same T/TxLabelMap | same | same |
| TagMap common | `outputId:N` | same T/OutputTagMap | same | same |
| Commission common | `commissionId:N,userId:N,transactionId:N,satoshis:N,keyOffset:S,isRedeemed:F` | same T/Commission | same | same |
| Deletion markers | absent on Basket, Label, Tag, LabelMap, TagMap, Certificate | `isDeleted:F` respective tables | needed to replicate deletion | reference-only; owner must provide storage/schema support |
| Synthetic row IDs | `certificateFieldId:N,txLabelMapId:N,outputTagMapId:N` | absent on composite-key tables | ignored unknown fields | toolbox-only; fields caller already omits certificateFieldId |

## Boundaries and follow-up

The request audit covers the forty methods, including unsupported extensions.
Fixed scenes contain synthetic metadata and byte sentinels; they pin JSON
serialization and do not claim the supplied transaction, certificate, or key
offset would pass the storage provider's semantic validation.
It is not a claim that every remote operation works against the reference:
owner rows, unvalidated storage arguments, deletion markers, and beta.1's
embedded SDK types remain material interoperability limits. No public type,
schema, pin, SQL implementation, or local storage fixture is changed.

The captain should file separate findings for the owner groups: validated
storage args and default semantics (including parsed outpoints and listOutputs
flags); proof-update request/result shape; sync deletion/proof/expiry metadata;
structured sync errors and logs; lifecycle and unsupported remote methods.
The CLI uses a toolbox wallet against toolbox storage, so these wire renames
are invisible unless it points at a reference server. Tell the bsv-rs captain
that D1-D3, D5-D6 and D8 move with #105; D4 and the storage outpoint row remain a
separate validated-storage question. Re-run and review the witnesses when
the dependency pin changes; do not auto-bless a mismatch.

T1: work is limited to a request/result's fields, without a chain query.
T2/T4: no proof or coin verdict is changed. T3: no extra network round trip.
T5: the server's pinned field contract replaces an undocumented assumption.
T6: every called method has an executable request witness; alias tests exercise
reference-shaped responses. No TS server deployment is exercised by this audit.

## CI landing requirement

Use the existing required `Test Suite (ubuntu-latest, stable)` and `Clippy`
contexts. At landing, the captain changes the unit-test, integration-test,
doc-test and Clippy commands in `.github/workflows/ci.yml:130-139,164-165` from
`--features sqlite` to `--features sqlite,remote`, keeping the required context
set unchanged. Enabling `remote` runs all 42 tests in `tests/storage_wire.rs`
and the library's `storage::client::wire_tests` on every PR, and includes their
code in Clippy's checks. Workflow edits belong to the captain; this lane's local
full-feature gates are recorded in its evidence.
