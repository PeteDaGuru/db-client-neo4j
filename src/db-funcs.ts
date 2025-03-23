import { cleanupDbSummary, dbCloseSession, dbLogFn, executeAndStreamCypherResults, executeCypher, executeCypherRawResults, main, newDbContext, newReadonlyDbContext, newWritableDbContext, objectFromDbResultRecord, PredefinedDbFunctions } from './db-client'
import { open, writeFile } from 'node:fs/promises'

/** Samples of adding DB functions to db-client and using it as an enhanced CLI
 *  With correct packages.json run script entry, can invoke via:
 *   npm run db-funcs -- --help
*/
function addMoreDbFunctions() {
  const p = PredefinedDbFunctions

  /** Use to run the specified functions as a series of tests - used by npm run-script test:write */
  p.runTestFuncs = async function runTestFuncs(db, parms) {
    let testCount=0
    for (const [testName, val] of Object.entries(parms ?? {})) {
      const test = PredefinedDbFunctions[testName]
      if (val === '0') {
        console.log(`ignoring ${testName}`)
      } else if (test == null) {
        throw new Error(`Cannot find test ${testName}`)
      } else {
        testCount++
        let result = []
        const dbWrite = newWritableDbContext(db)
        try {
        // Parameters are only for setValue
         result = await executeCypher(dbWrite, test, {key:'testKey', value:'testValue'})
        } catch (ex) {
          console.log(`${testCount} ${testName} testResult`, ex.message)
          throw ex
        } finally {
          console.log(`${testCount} ${testName}`, result)
        }
      }
    }
    return `ran ${testCount} tests`
  }

  /** sample1 test of isolating reads and writes and creating new sessions to the same DB
   * Note that sessions can be made to other DBs as well if desired.
   *  npm run --silent -- db-funcs sample1
   *  
   */
  p.sample1 = async function sample1(db, parms) {
    const readDb = newReadonlyDbContext(db)
    const results = await executeCypher(readDb, `return {name: $name, value:$value} as obj`, { name: 'sample1', value: new Date().toISOString() })
    const writeDb = newWritableDbContext(readDb)  // Talk to leader and allow writes in a separate session
    let obj = {} as any
    for (const res of results) {
      obj = res.obj
      await executeCypher(writeDb, `merge (n:Value{name:$nameParm}) set n.value=$valParm return $valParm as value`, { nameParm: obj.name, valParm: obj.value })
    }
    await dbCloseSession(writeDb)
    await dbCloseSession(readDb)
    // Note that we don't await dbClose(db) the entire DB here since this function may be invoked alongside others - main() does the final db close   
    return obj
  }

  /** Test of nesting functions and handling parameters */
  p.testNestedParms = async function testNestedParms(db, parms) {
    const echoResult = await executeCypher(db, PredefinedDbFunctions.echo, parms)
    const countResult = await executeCypher(db, PredefinedDbFunctions.nodeCount)
    const r2 = await executeCypher(db, async (db, p1) => {
      const r3 = await executeCypher(db, PredefinedDbFunctions.echo, { p1: p1 })
      return r3
    }, { parms: echoResult[0].parms, nodeCount: countResult[0].nodeCount })
    return r2
  }

  /** Test handling implicit transactions (IN TRANSACTIONS OF) 
   * npm run --silent -- db-funcs testImplictInTransactions 
   */
  p.testImplictInTransactions = async function testImplictInTransactions(db, parms) {
    // Since we are inside an active transaction, cannot execute expicit IN TRANSACTIONS using the given db session
    // Note that when not inside one of these PredefinedDbFunctions, this would not be an issue
    let error = null
    try {
      await executeCypher(db, `with $obj as obj call (obj) { finish } in transactions of 2 rows return obj`, { obj: 'foo' })
    } catch (ex) {
      error = ex
    }
    if (error == null) {
      throw new Error('testInTransactions should fail since we are already in a transaction')
    } else {
      const expectedErrorCode = 'Neo.DatabaseError.Transaction.TransactionStartFailed'
      if (error.code != expectedErrorCode) {
        throw new Error(`testInTransactions error.code=${error.code} instead of ${expectedErrorCode}`)
      }
    }
    // But we can open our own independent session
    const dbSess2 = newDbContext(db)
    const r4 = await executeCypher(dbSess2, `with $obj as obj call (obj) { finish } in transactions of 2 rows return obj`, { obj: 'foo' })
    return r4
  }

  /** exportToCsv using executeAndStreamCypherResults to handle results in batches
   *  Uses APOC function export-cypher-query-csv https://neo4j.com/docs/apoc/current/export/csv/#export-cypher-query-csv
   *  https://github.com/neo4j/apoc  
   * 
   * Note that without specifying a file, it streams everything in memory to the 'data' field.
  * Parms are: node label string (default Value) and number of results to limit it to (default 12)
  * Example invocation: 
  *   npm run --silent -- db-funcs exportValuesToCsv label=Value limit=200
  * 
  * If you specify a file parameter, it will write just the data to the given local file:
  *   npm run --silent -- db-funcs exportValuesToCsv label=Value limit=200 file=values.csv
  * Not that right now all that data will be streamed to memory first.  Future work is to stream it directly.
  * 
  * Optionally, can pass a server filename in to the Neo4j export procedure, but that places it in the Neo4j server filesystem,
  * which can handle large exports quickly.
  * This requires additional DB server settings in apoc.conf:
  *  apoc.export.file.enabled=true
  *  server.directories.import  is the directory that will be used (default is `import`)
  * https://neo4j.com/docs/apoc/5/export/csv/*
  * https://neo4j.com/docs/apoc/5/overview/apoc.export/apoc.export.csv.query/
  * 
  * Note that we can set up appropriate rights in a local Neo4j instance and use container volume-mapping to export to host filesystem)
  * See running-neo4j.md  for how to set that up in a local neo4j DB container
  * 
  * Example invocation: 
  *   npm run --silent -- db-funcs exportValuesToCsv label=Value limit=200 serverfile=values.csv
  */
  p.exportToCsv = async function exportToCsv(db, parms) {
    // Note that we use some javacript template values via ${xxx} substitution, and internal cypher parameter ones via $xxx
    const qp = {
      batchSize: 20000n,
      delim: ',',
      quotes: 'ifNeeded',
      timeoutSeconds: 300n,
      stream: true,
      limit: 12n,
      serverfile: null,
      file: null,
      fields: 'name,value'
    }
    const label = `\`${parms.label ?? 'Value'}\``
    for (const parmKey of Object.keys(qp)) {
      const val = parms[parmKey]
      if (val != null) {
        qp[parmKey] = val
      }
    }
    const fieldNames = qp.fields.split(',').map(e => e.trim()).filter(e => e !== '')
    if (fieldNames.length === 0) {
      throw new Error("fields must be comma-separated names")
    }
    const cypherFieldStmt = fieldNames.map(e => `n.\`${e}\` as \`${e}\``).join(', ')
    if (qp.serverfile != null) {
      qp.stream = false
    }
    if (qp.serverfile != null && qp.file != null) {
      throw new Error("Can specify only a local file or a serverfile name, not both")
    }
    try {
      qp.limit = BigInt(parms.limit ?? 12)
    } catch (ex) {
      dbLogFn?.('exportValuesToCsv', `limit ${parms.limit} not a number, default ${qp.limit} used`)
    }
    qp.timeoutSeconds = BigInt(qp.timeoutSeconds)
    qp.batchSize = BigInt(qp.batchSize)
    let outStream = process.stdout as any
    if (qp.file != null) {
      outStream = (await open(qp.file, 'w')).createWriteStream()
    }
    let dbSummary = null
    try {
      dbSummary = await executeAndStreamCypherResults(db, {
        text: `
call apoc.export.csv.query("match (n:${label}) return ${cypherFieldStmt} limit $limit",
$serverfile,
{stream:$stream, timeoutSeconds:$timeoutSeconds, quotes:$quotes, delim:$delim, batchSize:$batchSize,
 params:{limit: $limit, serverfile: $serverfile}});
`, parameters: qp
      },
        {
          onNext: (obj) => {
            const str = obj.data
            if (str != null) {
              outStream.write(str)
            }
          }
        })
    } catch (ex) {
      console.error(ex)
    } finally {
      if (qp.file != null) {
        outStream.close()
      }
    }
    return qp.file ?? qp.serverfile ?? 'use --quiet option to avoid this line'

    /* The following holds everything in memory at once, so can't handle large exports even if batchSize is small
       const rawDbResult = await executeCypherRawResults(db, `
   call apoc.export.csv.query("match (n:${label}) return ${cypherFieldStmt} limit $limit",
   $serverfile,
   {stream:$stream, timeoutSeconds:$timeoutSeconds, quotes:$quotes, delim:$delim, batchSize:$batchSize,
    params:{limit: $limit, serverfile: $serverfile}});
   `, qp)
       // Write to local file if specified
       db.dbSummary = cleanupDbSummary(rawDbResult.summary)
       for (const rec of rawDbResult.records) {
         if (qp.file != null) {
           const dataString = rec.get('data')
           if (dataString != null && dataString.length > 0) {
             const otherKeys = rec.keys.filter(e => e != 'data')
             await writeFile(qp.file, rec.get('data'))  // This reads it all into memory at once
             const obj = objectFromDbResultRecord(rec, otherKeys)
             obj.file = qp.file
             return obj
           }
         }
         // return rec.toObject()
       }
       return rawDbResult  // Shouldn't really get here
     */
  }



} // end addMoreDbFunctions



async function index(parms?) {
  addMoreDbFunctions()
  return await main(parms ?? process.argv.slice(2)) // get only user-provided arguments
}

if (require.main === module) {
  // Run via CLI not require() or import {}
  index()
}
