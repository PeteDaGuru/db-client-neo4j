import { dbCloseSession, DbFunction, executeCypher, GlobalExports, newDbContext, newReadonlyDbContext, newWritableDbContext } from "./db-client"

/** Set some predefined DB functions for tests invocation from CLI - try not to use a Cypher keyword like match, return, profile, explain, with
 * Answer the ones we've added (or if the passed parameter is null, just return them)
 */
export function setTestDbFunctions(dbFuncs: { [functionName: string]: DbFunction }): { [functionName: string]: DbFunction } {
  const addFuncs = {
    /** runTestFuncs - Use to run the specified functions as a series of tests - used by npm run-script test:write */
    runTestFuncs: async function runTestFuncs(db, parms) {
      let testCount = 0
      for (const [testName, val] of Object.entries(parms ?? {})) {
        const test = dbFuncs?.[testName]
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
            result = await executeCypher(dbWrite, test, { key: 'testKey', value: 'testValue' })
          } catch (ex) {
            console.log(`${testCount} ${testName} testResult`, ex.message)
            throw ex
          } finally {
            console.log(`${testCount} ${testName}`, result)
          }
        }
      }
      return `ran ${testCount} tests`
    },

    /** sample1 test of isolating reads and writes and creating new sessions to the same DB
     * Note that sessions can be made to other DBs as well if desired. */
    sample1: async function sample1(db, parms) {
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
    },

    /** Test of nesting functions and handling parameters */
    testNestedParms: async function testNestedParms(db, parms) {
      const echoResult = await executeCypher(db, dbFuncs.echo, parms)
      const countResult = await executeCypher(db, dbFuncs.nodeCount)
      const r2 = await executeCypher(db, async (db, p1) => {
        const r3 = await executeCypher(db, dbFuncs.echo, { p1: p1 })
        return r3
      }, { parms: echoResult[0].parms, nodeCount: countResult[0].nodeCount })
      return r2
    },

    /** Test handling implicit transactions (IN TRANSACTIONS OF) 
    */
    testImplictInTransactions: async function testImplictInTransactions(db, parms) {
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
    },

  }
  if (dbFuncs != null) {
    for (const [fnName, fn] of Object.entries(addFuncs)) {
      dbFuncs[fnName] = fn
    }
  }
  return addFuncs
}