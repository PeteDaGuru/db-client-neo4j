#!/bin/env node

import { parseArgs, inspect } from 'node:util'
import { dbClose, DbFunction, DbParmsType, DbQueryWithParametersType, executeCypher, GlobalExports, isValueTrue, newDbContext, setBigintHelperJson, timestampedLog } from './db-client'
import { setBasicDbFunctions } from './set-basic-db-functions'
import { setExportToCsvDbFunctions } from './exportToCsv-db-functions'
import { setTestDbFunctions } from './set-test-db-functions'
import { setSolographDbFunctions } from './solograph/db-solo-cli'

/** CLI for db-client that provides an interface to Neo4j graph database 
 * Some predefined functions are included to provide useful examples
*/

/** Predefined functions for invocation from CLI - try not to use a Cypher keyword like match, return, profile, explain, with */
export const PredefinedDbFunctions: { [functionName: string]: DbFunction } = {}
setBasicDbFunctions(PredefinedDbFunctions)
setTestDbFunctions(PredefinedDbFunctions)
setExportToCsvDbFunctions(PredefinedDbFunctions)
setSolographDbFunctions(PredefinedDbFunctions)

/** Handle simple args from CLI as name=value */
export function queryParmsFromCLI(args: string[]) {
  return Object.fromEntries(args.map(e => e.split('=', 2)))
}

/** Answer parms from CLI or enviroment using node parseArgs */
export function getDbParmsFrom(args: string[], env?: { [key: string]: string }): DbParmsType {
  if (env == null) {
    env = process?.env ?? {}
  }
  const pa = parseArgs({
    args: args, // process.argv.slice(2) used if null
    options: {
      dbUrl: {
        type: 'string',
        short: 'd',
      },
      dbName: {
        type: 'string',
        short: 'n'
      },
      dbUser: {
        type: 'string',
        short: 'u'
      },
      dbPass: {
        type: 'string',
        short: 'p'
      },
      readonly: {
        type: 'boolean',
        short: 'r',
      },
      allowwrite: {
        type: 'boolean',
        short: 'w',
      },
      ignoremarks: {
        type: 'boolean',
        short: 'i',
      },
      log: {
        type: 'boolean',
        short: 'l',
      },
      logresults: {
        type: 'boolean',
      },
      quiet: {
        type: 'boolean',
        short: 'q',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
    allowPositionals: true,
    strict: true,
  })
  // Fixup with default values
  pa.values.dbName = pa.values.dbName ?? env.NEO4J_DBNAME  // default if null is neo4j
  pa.values.dbUrl = pa.values.dbUrl ?? env.NEO4J_DBURL ?? env.NEO4J_URI ?? 'neo4j://localhost:7687'
  pa.values.dbUser = pa.values.dbUser ?? env.NEO4J_USERNAME ?? 'neo4j'
  pa.values.dbPass = pa.values.dbPass ?? env.NEO4J_PASSWORD
  pa.values.allowwrite = pa.values.allowwrite ?? isValueTrue(env.NEO4J_ALLOWWRITE)
  if (pa.values.readonly) {
    pa.values.allowwrite = false
  }
  const firstArg = pa.positionals[0]
  pa.values.help = pa.values.help || firstArg === 'help' || firstArg === '?'
  const { values, positionals } = pa
  return { ...values, positionals: positionals }  // Flatten parseArgs result
}

export let helpText = {
  fullText: `yarn db-client [settings] "cypher query" [key1=val1] [key2=val2]
Execute a Neo4j cypher DB query or predefined functions, optionally passing in string key=value parameters 
that can be accessed in the query via neo4j $key references.

Default is to allow only read operations - no writing allowed.

Optional settings - some may be set by environment variables too:
 --dbUrl      | -d : Database URL - default from NEO4J_DBURL or NEO4J_URI
 --dbName     | -n : DB name - default from NEO4J_DBNAME or if not specified usually defaults to neo4j
 --dbUser     | -u : username - default from NEO4J_USERNAME
 --dbPass     | -p : password - default from NEO4J_PASSWORD
 --allowwrite | -w : allow writing to DB - can be set a default via NEO4J_ALLOWRITE=1
 --readonly   | -r : override allowwrite flag for this invocation (in case env variable is set)
 --ignoremarks| -i : ignore DB bookmark (avoid causal clustering for independent transactions)
 --log        | -l : log internal flows and DB response details to stderr
 --logresults      : log query and results to stderr (independent of --log)
 --quiet      | -q : Do not display result to stdout console.log (does not affect --log* output)
 --help  | ?  | -h : Display this help
`
}

export function replaceHelpCommandNameWith(str) {
  helpText.fullText = helpText.fullText.replace('db-client', str)
}

export function help() {
  console.log(helpText.fullText)
  console.log(`PredefinedDbFunctions: ${Object.keys(PredefinedDbFunctions).join(' ')}`)
}


/** Answer parms from CLI or enviroment using node parseArgs */
export function handleCliArgs(args, env?: { [key: string]: string }) {
  const dbParms = getDbParmsFrom(args, env ?? process?.env)
  if (dbParms.help || dbParms.positionals.length === 0) {
    help()
    return { dbParms: dbParms, query: null, queryParms: null }
  }
  let query: string | DbFunction | DbQueryWithParametersType = dbParms.positionals.join(' ')
  let queryParms = null
  if (query.length === 0) {
    query = `return 'ok' as ok`
  } else {
    const firstArg = dbParms.positionals[0]
    const dbFn = PredefinedDbFunctions[firstArg]
    if (dbFn != null) {
      query = dbFn
      queryParms = queryParmsFromCLI(dbParms.positionals.slice(1))
    } else if (firstArg.includes(' ')) { // First parameter is query with embedded spaces, rest are parms
      query = firstArg
      queryParms = queryParmsFromCLI(dbParms.positionals.slice(1))
    }
  }
  inspect.defaultOptions.depth = 18
  inspect.defaultOptions.breakLength = 240
  if (dbParms.log) {
    inspect.defaultOptions.depth = 18
    inspect.defaultOptions.compact = 18
    inspect.defaultOptions.breakLength = 512
    GlobalExports.dbLogFn = timestampedLog
  }
  if (dbParms.logresults) {
    inspect.defaultOptions.depth = 42
    inspect.defaultOptions.compact = 18
    inspect.defaultOptions.breakLength = 512
  }
  return { dbParms: dbParms, query: query, queryParms: queryParms }
}



/** DB Client for Neo4j DB 
 * If processResultFn is set, run that on the result before returning it.
 * Note that this provides a CLI but is also an example of using the API.
 * Driver docs are at https://neo4j.com/docs/api/javascript-driver/current/
*/
export async function main(args, processResultFn?:(any)=>any) {
  const { dbParms, query, queryParms } = handleCliArgs(args)
  if (query == null) {
    return {}
  }
  const db = newDbContext(dbParms)
  let data = {}
  if (processResultFn == null) {
    processResultFn = (res) => {
      return {result: res}
    }
  }
  try {
    data = processResultFn(await executeCypher(db, query, queryParms))
  } finally {
    await dbClose(db)
  }
  GlobalExports.dbLogFn?.('main', db)
  if (!dbParms.quiet) {
    GlobalExports.dbConsoleLogStdout(JSON.stringify(data))
  }
  return data
}

/** Generic way of doing C-like  main(argv) from Nodejs command-line invocation */
export async function index(parms?) {
  return await main(parms ?? process.argv.slice(2)) // get only user-provided arguments
}

/** Trick to allow direct invocation from CLI or also use as a module if desired */
if (require.main === module) {
  // Run via CLI not require() or import {}
  setBigintHelperJson()
  index()
}

/** Ensure BigInt JSON support is available when loaded as a module */
setBigintHelperJson() 
