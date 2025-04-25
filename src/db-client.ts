#!/bin/env node
import neo4j, { Session, Config, SessionConfig, ManagedTransaction } from "neo4j-driver"
import { Driver, QueryResult, RecordShape, ResultSummary, TransactionConfig } from "neo4j-driver-core"

/** DB Client for Neo4j DB - implemented as functions (not classes) 
  Raw Usage:
    const db = newReadonlyDbContext({dbParms: { dbName:'neo4j', dbUrl:'neo4j://localhost:7687'}})
    const results = await executeCypher(db, `return {name: 'foo', value:'this is a test'} as obj`)
    const writeDb = newWritableDbContext(db)  // Talk to leader to allow writes in a separate session to the same database
    for (const res of results) {
      const obj = res.obj
      await executeCypher(writeDb, `merge (n:Value{name:$nameParm}) set n.value=$valParm return $valParm as value`, {nameParm: obj.name, valParm: obj.value})
    }
    await dbClose(db)  // Note that they both are closed since they are both opened on the same database 
  
  See main() method for detailed example that also acts as a CLI
  handleCliArgs() or getDbParmsFrom may also be useful for other implementations, but 
  it may be easier to just extend the list of PredefinedDbFunctions - see [db-funcs.ts](./db-funcs.ts)
  
  Driver docs are at https://neo4j.com/docs/api/javascript-driver/current/
*/
export type DbParmsType = Partial<{
  dbUrl: string;
  dbName: string;
  dbUser: string;
  dbPass: string;
  readonly: boolean;
  allowwrite: boolean;
  ignoremarks: boolean;
  quiet: boolean;
  log: boolean;
  logresults: boolean;
  help: boolean;
  positionals: string[];
}>

export let GlobalExports = {
  dbLogFn: null as unknown as Function, // set to console.log, console.error or timestampedLog to enable logging
  dbConsoleLogStdout: console.log // used for stdout to echo final output or not if --quiet option specified 
}

export function isValueTrue(valOrStr) {
  return valOrStr === true || '1yYtT+'.includes(valOrStr)
}

/** log function adds timestamp */
export function timestampedLog(msg: string, ...rest) {
  const dbLog = { [msg]: rest }
  console.error(new Date().toISOString(), dbLog)
}

/** Note that this aligns with Neo4j Query type used by run() methods */
export type DbQueryWithParametersType = {
  text: string,
  parameters?: { [parm: string]: any },
}

export type DbResultRaw = QueryResult<RecordShape>

export type DbContextType = {
  dbParms: DbParmsType,
  dbDriverParms: Config,
  dbSessionParms: SessionConfig,
  dbTxnParms: TransactionConfig,
  dbDriver: Driver,
  dbSession: Session | null,
  dbTxn: ManagedTransaction | null,
  dbMarks: string[],  // lastBookmarks from session not dbDriver.BookmarkManager
  dbSummary: any, // flat JSON summary from last results
}

export type DbContextOrParmsType = Partial<DbContextType & DbParmsType>

// 2022 trick to expose members rather than type name: type Resolve<T> = T extends Function ? T : {[K in keyof T]: T[K]}

export type DbFunction<T = any> = (db: DbContextType, parameters?: { [key: string]: any }) => T

/** Provide CLI and programmatic access to Neo4j DB in nodejs */
export function newDbDriver(dbContext: DbContextType) {
  dbContext.dbDriverParms = { useBigInt: true, ...dbContext.dbDriverParms }
  const dbParms = dbContext.dbParms
  if (dbParms.dbUrl == null || dbParms.dbUrl.length === 0) {
    throw new Error("dbParms.dbUrl must be specified")
  }
  return neo4j.driver(dbParms.dbUrl, neo4j.auth.basic(dbParms.dbUser, dbParms.dbPass), dbContext.dbDriverParms)
}

/** Shut down entire DB driver */
export async function dbClose(db: DbContextType) {
  await dbCloseSession(db)   // May not be explicitly needed, but let's be nice
  const res = await db?.dbDriver?.close()
  db.dbDriver = null
  return res
}

/** Close just the active session to the DB (so next request will get a fresh one) */
export async function dbCloseSession(db: DbContextType) {
  const res = await db?.dbSession?.close()
  db.dbSession = null
  return res
}

/** Note that DB is actually not contacted until first cypher query goes through, but driver may check url
 * Parms can be a DbParmsType the result of getParms() call but can also be pulled from another DbContextType
 * This can allow setting or overriding just certain dbSession or dbTxn parms as needed
 */
export function newDbContext(dbContext: DbContextOrParmsType, overrides?: DbContextOrParmsType): DbContextType {
  const dbParmsOverrides = overrides?.dbParms ?? overrides as DbParmsType
  const dbParms: DbParmsType = { ...(dbContext.dbParms ?? dbContext), ...dbParmsOverrides }
  dbParms[Symbol.for('nodejs.util.inspect.custom')] = (depth, options) => {  // Avoid logging password
    const { dbPass, ...rest } = { ...dbParms }
    delete rest[Symbol.for('nodejs.util.inspect.custom')]   //ugh
    return {
      ...rest,
      dbPass: dbPass == null ? null : '****',
    }
  }
  GlobalExports.dbLogFn?.('newDbContext', dbParms)
  let db: DbContextType = {
    dbParms: dbParms,
    dbDriverParms: { ...dbContext.dbDriverParms, ...overrides?.dbDriverParms },
    dbSessionParms: { database: dbParms.dbName, ...dbContext.dbSessionParms, ...overrides?.dbSessionParms },
    dbTxnParms: { ...dbContext.dbTxnParms, ...overrides?.dbTxnParms },
    dbDriver: overrides?.dbDriver ?? dbContext.dbDriver,
    dbSession: null,
    dbTxn: null,
    dbMarks: overrides?.dbMarks ?? dbContext.dbMarks,
    dbSummary: null, // flat JSON summary from last results
  }
  if (db.dbDriver == null) {
    db.dbDriver = newDbDriver(db)
    // db.dbSessionParms.bookmarkManager = db.dbDriver.executeQueryBookmarkManager // Could be used to ensure dbDriver.executeQuery is in sync with session.execute
  }
  setDbSessionParmsDefaultAccessMode(db)
  return db
}

/** Ensure session default access mode respects our dbParms settings */
export function setDbSessionParmsDefaultAccessMode(db: DbContextType) {
  if (db.dbParms.allowwrite) {
    db.dbSessionParms.defaultAccessMode = 'WRITE'
  } else if (db.dbParms.readonly) {
    db.dbSessionParms.defaultAccessMode = 'READ'
  } // else let it use db driver default (write)
}

/** Answer a new read-only db context */
export function newReadonlyDbContext(db: DbContextOrParmsType, overrides?: DbContextOrParmsType): DbContextType {
  return newDbContext(db, { ...overrides, ...{ allowwrite: false } })
}

/** Answer a new writable db context */
export function newWritableDbContext(db: DbContextOrParmsType, overrides?: DbContextOrParmsType): DbContextType {
  return newDbContext(db, { ...overrides, ...{ allowwrite: true } })
}

/** Handle causal cluster bookmarks so multiple sessions can remain in synch - at end of transaction or when closing a session */
export function dbHandleLastBookmarks(db: DbContextType) {
  const marks = db.dbSession?.lastBookmarks()
  db.dbMarks = marks
  db.dbSessionParms.bookmarks = db.dbParms.ignoremarks ? null : marks
  // Note that if dbDriver.executeQuery is done, should share it's bookmarkmanager: https://neo4j.com/docs/javascript-manual/current/bookmarks/#_mix_executequery_and_sessions
  return db
}

/** Forget causal cluster bookmarks we may have saved up */
export function dbForgetBookmarks(db: DbContextType) {
  db.dbMarks = null
  db.dbSessionParms.bookmarks = null
  return db
}

/** Flatten DB query statistics with name/number pairs - avoid 0 values */
export function flatQueryStatistics(dbQueryStats) {
  return Object.fromEntries(Object.entries(dbQueryStats?._stats ?? {}).filter(([stat, num]) => +num > 0).map(e => e))
}

export function cleanupDbSummary(dbSummary) {
  // Note that plan and profile fields return deeperr objects if EXPLAIN or PROFILE is done - seem to log ok"
  dbSummary.counters = flatQueryStatistics(dbSummary.counters)
  dbSummary.updateStatistics = flatQueryStatistics(dbSummary.updateStatistics)
  return dbSummary
}

export function objectFromDbResultRecord<T = any>(rec, onlyFields?: PropertyKey[]): T {
  return onlyFields == null ? rec.toObject() : Object.fromEntries(onlyFields.map(e => [e, rec.get(e)])) as T
}

export async function dbResultsAsObjects<T>(db: DbContextType, dbResults: DbResultRaw, onlyFields?: string[]): Promise<T[]> {
  let recs: T[] = []
  const records = dbResults?.records
  if (records == null || dbResults.summary?.counters == null) {  // already converted
    return dbResults as any as T[]
  }
  records.forEach(rec => {
    recs.push(objectFromDbResultRecord<T>(rec, onlyFields))
  })
  db.dbSummary = cleanupDbSummary(dbResults.summary)
  // callers can do dbForgetBookmarks(db) if they really have no need for cross-session causal synchronization
  return recs
}

/** Execute a Cypher query or function ensuring results are converted to objects */
export async function executeCypher<T = any>(db: DbContextType, dbQuery: string | DbFunction | DbQueryWithParametersType, parameters?: { [key: string]: any }): Promise<T[]> {
  const results = await dbResultsAsObjects<T>(db, await executeCypherRawResults<T>(db, dbQuery, parameters))
  if (db.dbParms.logresults) {
    timestampedLog('dbResult', dbQuery, results)
  }
  return results
}

/** Execute a Cypher query or function as a read/write transaction without converting results to objects */
export async function executeCypherRawResults<T = any>(db: DbContextType, dbQuery: string | DbFunction | DbQueryWithParametersType, parameters?: { [key: string]: any }): Promise<DbResultRaw> {
  const isQueryFunction = typeof dbQuery === 'function'
  let dbQueryWithParms = dbQuery as DbQueryWithParametersType
  if (typeof dbQuery === 'string') {
    dbQueryWithParms = { text: dbQuery, parameters: parameters }
  } else if (parameters != null && !isQueryFunction) {
    dbQueryWithParms.parameters = { ...dbQueryWithParms.parameters, ...parameters }
  }
  GlobalExports.dbLogFn?.('executeCypher', dbQueryWithParms ?? dbQuery, parameters ?? '')
  if (db.dbSession == null) {
    db.dbSession = db.dbDriver.session(db.dbSessionParms)
    if (!isQueryFunction) {  // Perform using single-transaction session.run call
      const dbResult = await db.dbSession.run(dbQueryWithParms.text, dbQueryWithParms.parameters, db.dbTxnParms)
      dbHandleLastBookmarks(db)
      return dbResult
    }
    // else fall through since isQueryFunction is true
  }
  const dbQueryFn = isQueryFunction ? dbQuery : async (db: DbContextType) => {
    // causal consistency bookmarks are maintained within a transaction
    return await db.dbTxn.run<T>(dbQueryWithParms.text, dbQueryWithParms.parameters)
  }
  if (db.dbTxn == null) {
    let dbResult = null
    try {
      if (db.dbSessionParms?.defaultAccessMode === 'READ') {
        dbResult = await db.dbSession.executeRead<T>(async (dbTxn) => {
          db.dbTxn = dbTxn
          return await dbQueryFn(db, parameters)
        }, db.dbTxnParms)
      } else {
        dbResult = await db.dbSession.executeWrite<T>(async (dbTxn) => {
          db.dbTxn = dbTxn
          return await dbQueryFn(db, parameters)
        }, db.dbTxnParms)
      }
    } finally {
      dbHandleLastBookmarks(db)
      db.dbTxn = null
    }
    return dbResult
  }
  return await dbQueryFn(db, parameters)  // Execute within existing transaction
}

export type DbObserver = { onNext: (obj) => void, onCompleted?: (summary) => void, onError?: (error) => void, onKeys?: (keys: string[]) => void }
/** Execute Cypher statement to allow streaming results back - can handle large amounts data
 *  https://neo4j.com/docs/api/javascript-driver/current/#consuming-records-with-streaming-api
 *  Note that if you are already in a transaction, you should create a fresh session for the stream (and close it when done)
  const streamDb = newDbContext(db)
  await streamDb.executeAndStreamCypherResults(`match (n:Value) return properties(n) as props`)
   .subscribe({
    onNext: obj => {
      console.log(obj)
    }
  })
*/
export async function executeAndStreamCypherResults<T = any>(db: DbContextType, dbQueryWithParms: DbQueryWithParametersType, observer: DbObserver) {
  // If already in a transaction, need to get fresh session to stream
  const ourDb = db.dbTxn ? newDbContext(db) : db
  GlobalExports.dbLogFn?.('executeAndStreamCypherResults', dbQueryWithParms)
  if (ourDb.dbSession == null) {
    ourDb.dbSession = db.dbDriver.session(db.dbSessionParms)
  }
  return new Promise<T>((resolve, reject) => {
    ourDb.dbSession.run(dbQueryWithParms).subscribe({
      onKeys: (keys: string[]) => {
        observer.onKeys?.(keys)
      },
      onNext: record => {
        observer.onNext(record?.toObject())
      },
      onCompleted: (summary: ResultSummary) => {
        dbHandleLastBookmarks(ourDb)
        const dbSummary = cleanupDbSummary(summary)
        ourDb.dbSummary = dbSummary
        if (ourDb !== db) {
          db.dbMarks = ourDb.dbMarks
          db.dbSummary = ourDb.dbSummary
          ourDb.dbSession.close()
        }
        const retVal = observer.onCompleted?.(dbSummary)
        resolve(dbSummary)
      },
      onError: error => {
        if (ourDb !== db) {
          ourDb.dbSession.close()
        }
        observer.onError?.(error)
        reject(error)
      }
    })
  })
}

/** Allow bigint to convert to JSON numeric and uint64 precisely (use string value if out of range)
*/
const MaxSafeInt_bigint = BigInt(Number.MAX_SAFE_INTEGER) // 2^53 - 1 = 9007199254740991
const MinSafeInt_bigint = BigInt(Number.MIN_SAFE_INTEGER) // - (2^53 - 1) = -900719925474099

/** Allow JSON.stringify() to work by default for BigInt - output precise number or string to avoid floating-point issues
 * This allows int64 types from DB driver to work (avoiding their Integer{low,high} objects workaround
 * setBigintHelperJson() must be called for this to take effect
 * Note that consumers need to use BigInt(xxx) on the field to get a legit bigint out of it
 * See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt#use_within_json
 * for more details and other approaches.
 */
export function setBigintHelperJson() {
  if (BigInt.prototype['toJSON'] == null) {
    BigInt.prototype['toJSON'] = function toJSON() {
      if (this > MaxSafeInt_bigint || this < MinSafeInt_bigint) {
        return this.toString() // Answers number in quoted string, or if target supports bigints can return raw digits via JSON.rawJSON(this.toString())
      } else {
        return Number(this) // Within precise range for floating-point integers
      }
    }
  }
}