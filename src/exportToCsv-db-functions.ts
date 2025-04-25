import { DbFunction, executeAndStreamCypherResults, executeCypher, GlobalExports } from "./db-client"
import { open, writeFile } from 'node:fs/promises'

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
  *   npm run --silent -- db-cypher exportValuesToCsv label=Value limit=200 serverfile=values.csv
  */
export async function exportToCsv(db, parms) {
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
    GlobalExports.dbLogFn?.('exportValuesToCsv', `limit ${parms.limit} not a number, default ${qp.limit} used`)
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

/** Set a DB function to exportToCsv with various options for invocation from CLI
 * Answer the ones we've added (or if the passed parameter is null, just return them)
 */
export function setExportToCsvDbFunctions(dbFuncs: { [functionName: string]: DbFunction }): { [functionName: string]: DbFunction } {
  const addFuncs = {
    exportToCsv: exportToCsv
  }
  if (dbFuncs != null) {
    for (const [fnName, fn] of Object.entries(addFuncs)) {
      dbFuncs[fnName] = fn
    }
  }
  return addFuncs
}