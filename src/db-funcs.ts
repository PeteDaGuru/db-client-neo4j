import { cleanupDbSummary, dbLogFn, executeCypher, executeCypherRawResults, main, objectFromDbResultRecord, PredefinedDbFunctions } from './db-client'
import { writeFile } from 'node:fs/promises'

/** Samples of adding DB functions to db-client and using it as an enhanced CLI
 *  With correct packages.json run script entry, can invoke via:
 *   yarn db-funcs --help
*/
function addMoreDbFunctions() {
  const p = PredefinedDbFunctions

  /** exportValuesToCsv predefined function - export name,value fields from Values nodes
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
  p.exportValuesToCsv = async (db, parms) => {
    // Note that we use some javacript template values via ${xxx} substitution, and internal cypher parameter ones via $xxx
    const label = parms.label ?? 'Value'
    const localfile = parms.file
    let serverfile = parms.serverfile
    let stream = true
    if (serverfile != null || localfile == null) {
      stream = false
    }
    let limit = 12n
    if (serverfile != null && localfile != null) {
      throw new Error("Can specify only a local file or a serverfile name, not both")
    }
    try {
      limit = BigInt(parms.limit ?? 12)
    } catch (ex) {
      dbLogFn?.('exportToCsv', `limit ${parms.limit} not a number, default ${limit} used`)
    }
    const rawDbResult = await executeCypherRawResults(db, `
call apoc.export.csv.query("match (n:${label}) return n.name as name, n.value as value limit $limit",
$serverfile,
{stream:$stream, timeoutSeconds:300, quotes:'ifNeeded', params:{limit: $limit, serverfile: $serverfile}});
`, { limit: limit, stream: stream, serverfile: serverfile ?? null})

    // Write to local file if specified
    db.dbSummary = cleanupDbSummary(rawDbResult.summary)
    for (const rec of rawDbResult.records) {
      if (localfile != null) {
        const dataString = rec.get('data')
        if (dataString != null && dataString.length > 0) {
          const otherKeys = rec.keys.filter(e => e != 'data')
          await writeFile(localfile, rec.get('data'))  // This reads it all into memory at once
          const obj = objectFromDbResultRecord(rec, otherKeys)
          obj.localfile = localfile
          return obj
        }
      }
      return rec.toObject()
    }
    return rawDbResult  // Shouldn't really get here
  }



}



async function index(parms?) {
  addMoreDbFunctions()
  return await main(parms ?? process.argv.slice(2)) // get only user-provided arguments
}

if (require.main === module) {
  // Run via CLI not require() or import {}
  index()
}
