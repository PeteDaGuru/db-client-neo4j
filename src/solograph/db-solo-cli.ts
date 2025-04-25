import { DbFunction } from "../db-client"
import { main, PredefinedDbFunctions, replaceHelpCommandNameWith } from "../db-cli"
import { createSoloGraphFromCypherQuery, createSoloGraphFromNeo4jResults } from "./db-solograph"

/** Set some test DB functions for solograph with various options for invocation from CLI
 * Answer the ones we've added (or if the passed parameter is null, just return them)
 */
export function setSolographDbFunctions(dbFuncs: { [functionName: string]: DbFunction }): { [functionName: string]: DbFunction } {
  const addFuncs = {
    testSoloGraph: async function testSoloGraph(db, parms) {
      const graph = await createSoloGraphFromCypherQuery(db, `MATCH g = ()-[]->() limit 5 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output`, parms)
      return graph
    },

    /** Test production of empty SoloGraph from result that are not nodes and relationships  */
    testSoloGraph1: async function testSoloGraph1(db, parms) {
      const graph = await createSoloGraphFromCypherQuery(db, `MATCH (n)-[]->(p) limit 5 return properties(n) as n, properties(p) as p`, parms)
      return graph
    },

    /** Test production of SoloGraph objects from unstructured result 2 - Neo4j Path */
    testSoloGraph2: async function testSoloGraph2(db, parms) {
      const graph = await createSoloGraphFromCypherQuery(db, `MATCH g = ()-[]->() limit 5 return g`, parms)
      return graph
    },

    /** Test production of SoloGraph objects from unstructured result 3 - explicit returns */
    testSoloGraph3: async function testSoloGraph3(db, parms) {
      const graph = await createSoloGraphFromCypherQuery(db, `MATCH (n)-[r]->(p) limit 5 return n,r,p`, parms)
      return graph
    },
  }
  if (dbFuncs != null) {
    for (const [fnName, fn] of Object.entries(addFuncs)) {
      dbFuncs[fnName] = fn
    }
  }
  return addFuncs
}

/** Ensure response is in Solograph format, not native Neo4j format */
async function index(parms?) {
  setSolographDbFunctions(PredefinedDbFunctions)
  replaceHelpCommandNameWith('db-solo')
  return await main(parms ?? process.argv.slice(2),
    (data) => {
      return { result: createSoloGraphFromNeo4jResults(data) }
    })
}

if (require.main === module) {
  // Run via CLI not require() or import {}
  index()
}