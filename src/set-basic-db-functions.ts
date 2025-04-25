import { DbFunction, executeCypher } from "./db-client"

/** Set some basic predefined DB functions for invocation from CLI - try not to use a Cypher keyword like match, return, profile, explain, with
 * Answer the ones we've added (or if the passed parameter is null, just return them)
 */
export function setBasicDbFunctions(dbFuncs: { [functionName: string]: DbFunction }): { [functionName: string]: DbFunction } {
  const addFuncs = {
    ok: async (db) => { return await executeCypher(db, `return 'ok' as ok`) },
    echo: async (db, parms) => { return await executeCypher(db, `return $parms as parms`, { parms: parms }) },
    nodeCount: async (db) => { return await executeCypher(db, `match (n) return count(n) as nodeCount`) },
    initValueConstraint: async (db) => { return await executeCypher(db, `create constraint Value_name if not exists for (n:Value) require n.name is unique ;`) },
    setValue: async (db, parms) => { return await executeCypher(db, `merge (n:Value{name:$key}) set n.value=$value return n.value as value`, parms) },
    getAllValues: async (db) => { return await executeCypher(db, `match (n:Value) with properties(n) as props return apoc.map.fromPairs(collect([props.name,props.value])) as dict`) },
    getValue: async (db, parms) => { return await executeCypher(db, `match (n:Value) where n.name=$key return n.value as value`, parms) },
  }
  if (dbFuncs != null) {
    for (const [fnName, fn] of Object.entries(addFuncs)) {
      dbFuncs[fnName] = fn
    }
  }
  return addFuncs
}