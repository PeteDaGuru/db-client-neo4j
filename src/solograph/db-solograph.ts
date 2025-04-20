import { DbContextType, executeCypher, main, PredefinedDbFunctions, replaceHelpCommandNameWith } from '../db-client'
import { addMoreDbFunctions } from '../db-funcs'

/** Functions to handle Solo graphs from https://github.com/WardCunningham/graph
 *  With correct packages.json run script entry, can invoke via:
 *   npm run db-solograph -- --help
 *   node -r ts-node/register/transpile-only src/solograph/db-solograph.ts
 *
 *  Note: Current code takes any Cypher result and tries to parse out all Neo4j nodes/edges
 * 
 *  Solo collaborator wants {name: "foo", graph: {nodes:[], rels:[]}} and wants a name property in props of nodes
 *  solo collaborator may also desire things as .jsonl so JSON must be in one line (not pretty-printed).  jq -c option
 *  http://ward.dojo.fed.wiki/view/welcome-visitors/view/solo-graph
 *
*/

/** Neo4j Edge */
export type EdgeResultNeo4j = {
  type: string,
  properties: { [key: string]: any },
  identity: bigint,
  start: bigint,
  end: bigint,
  elementId: string,
  startNodeElementId: string,
  endNodeElementId: string,
}

/** Neo4j Node */
export type NodeResultNeo4j = {
  labels: string[],
  properties: { [key: string]: any },
  identity: bigint,
  elementId: string,
}

/** GraphSolo Node */
export type SoloGraphNode = {
  type: string,
  in: number[],
  out: number[],
  props: { [key: string]: any },
}

/** GraphSolo Edge */
export type SoloGraphEdge = {
  type: string,
  from: number,
  to: number,
  props: { [key: string]: any },
}

/** Answer true if the given object is indeed a simple object or instance of a class */
export function isObject(e) {
  return e != null && typeof e === 'object'
}

/** Answer true if object seems to be a Neo4j node object
*  In the Neo4j driver, there is a isNode() that looks for __isNode__ property defined in node_modules/neo4j-driver-core/lib/graph-types.js
*  But we may have converted to plain objects by the tie we get here, so that field does not exist
*/
export function isNeo4jNode(e) {
  return e?.identity != null && Array.isArray(e.labels)
}

/** Answer true if this is a Neo4j edge
 *  In the Neo4j driver, there is a isRelationship() that looks for __isRelationship__ property defined in node_modules/neo4j-driver-core/lib/graph-types.js
 *  But we may have converted to plain objects by the time we get here, so that field does not exist
*/
export function isNeo4jEdge(e) {
  return e?.identity != null && e.type != null && e.start != null && e.end != null
}

/** Answer true if this is a Neo4j path
 *  In the Neo4j driver, there is a isRelationship() that looks for __isRelationship__ property defined in node_modules/neo4j-driver-core/lib/graph-types.js
 *  But we may have converted to plain objects by the time we get here, so that field does not exist
*/
export function isNeo4jPath(e) {
  return Array.isArray(e.segments) && isNeo4jNode(e.start)
}

/** Answer true if the given result already been conerted to a SoloGraph */
function hasSoloGraph(e) {
  if (isObject(e)) {
    if (hasSoloGraph(e.result?.solo)) { // Usual case
      return true
    }
    if (Array.isArray(e) && e.length > 0) {
      return hasSoloGraph(e[0])
    }
    if (Array.isArray(e.nodes) && Array.isArray(e.edges)) {
      return true
    }
    if (e.type != null && isObject(e.props)) {
      return true
    }
    return hasSoloGraph(e.solo) || hasSoloGraph(e.result)
  }
  return false
}

/** Create a SoloGraph from the nodes and relationships from a Neo4j result. Uses first label of the node only.
 *  MATCH   g = ()-[]->() limit 100 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output
 */
export function createSoloGraphFromNeo4jNodesAndEdges(nodes: NodeResultNeo4j[], edges: EdgeResultNeo4j[]) {
  const nodeMap = new Map<string, number>() // Map from elementId to Graph node index
  const gNodes: SoloGraphNode[] = []
  for (const node of nodes) {
    const id = node.elementId ?? String(node.identity)
    const nodeIdx = nodeMap.get(id)
    if (nodeIdx == null) {
      const idx = gNodes.length
      const gNode: SoloGraphNode = { type: node.labels[0], in: [], out: [], props: node.properties }
      gNodes.push(gNode)
      nodeMap.set(id, idx)
    } else { // Merge properties (only happens if DISTINCT not used in Cypher query)
      const gNode = gNodes[nodeIdx]
      gNode.props = { ...gNode.props, ...node.properties }
    }
  }
  const edgeMap = new Map<string, number>() // Map from elementId to Graph edge index
  const gEdges: SoloGraphEdge[] = []
  for (const edge of edges) {
    const id = edge.elementId ?? String(edge.identity)
    const edgeIdx = edgeMap.get(id)
    if (edgeIdx == null) {
      const idx = gEdges.length
      const fromIdx = nodeMap.get(edge.startNodeElementId ?? String(edge.start))
      const toIdx = nodeMap.get(edge.endNodeElementId ?? String(edge.end))
      const gEdge: SoloGraphEdge = { type: edge.type, props: edge.properties, from: fromIdx, to: toIdx }
      gEdges.push(gEdge)
      edgeMap.set(id, idx)
      gNodes[fromIdx].out.push(idx)
      gNodes[toIdx].in.push(idx)
    } else { // Merge properties (only happens if DISTINCT not used in Cypher query)
      const gEdge = gEdges[edgeIdx]
      gEdge.props = { ...gEdge.props, ...edge.properties }
    }
  }
  return { nodes: gNodes, edges: gEdges }
}

export type CreateSoloGraphResponseType = { solo: { nodes: SoloGraphNode[], edges: SoloGraphEdge[] }, rest: any[] }

/** Parse Neo4j nodes/rels from the given Neo4j query results into SoloGraph nodes and edges (and put anything else in the rest collection)
*/
export function createSoloGraphFromNeo4jResults(results: any): CreateSoloGraphResponseType {
  const nodes: NodeResultNeo4j[] = []
  const edges: EdgeResultNeo4j[] = []
  const rest: any[] = []
  if (hasSoloGraph(results)) {
    return results  // Already converted
  }
  /** Answer true if the object has array of nodes and edges {nodes:[], edges:[]} */
  function isNodesAndEdges(e) {
    return (Array.isArray(e?.nodes) && Array.isArray(e?.edges))
  }
  function addAllTo(ar, sourceArray) {
    for (const e of sourceArray) {
      ar.push(e)
    }
  }
  /** Add all Neo4j nodes and edges from the given row - answer true if done
   *  Simple case is {nodes:[], edges:[]} object
   *  Otherwise traverse and check all objects
   * Accepts a result row object or array
   */
  function addAllNodesAndEdgesFrom(row) {
    if (!isObject(row)) {
      return false
    }
    let foundSome = false
    if (isNodesAndEdges(row)) {
      addAllTo(nodes, row.nodes)
      addAllTo(edges, row.edges)
      foundSome = true
    }
    if (!foundSome) { // Perform deep traversal
      for (const [name, e] of Object.entries<any>(row)) {
        if (isObject(e)) {
          if (isNodesAndEdges(e)) {
            addAllTo(nodes, e.nodes)
            addAllTo(edges, e.edges)
            foundSome = true
          } else if (Array.isArray(e)) {
            if (addAllNodesAndEdgesFrom(e)) {
              foundSome = true
            }
          } else if (isNeo4jNode(e)) {
            nodes.push(e)
            foundSome = true
          } else if (isNeo4jEdge(e)) {
            edges.push(e)
            foundSome = true
          } else if (isNeo4jPath(e)) {
            for (const segment of e.segments) {
              const start = segment.start
              const end = segment.end
              const edge = segment.relationship
              if (isNeo4jNode(start) && isNeo4jNode(end) && isNeo4jEdge(edge)) {
                nodes.push(start)
                nodes.push(end)
                edges.push(edge)
              }
            }
          } else {
            rest.push({ [name]: e })  // Not a Neo4j node, edge, or path
          }
        } else {
          rest.push({ [name]: e })
        }
      }
    }
    return foundSome
  }
  if (!Array.isArray(results)) {
    results = [results]
  }
  for (const row of results) {
    addAllNodesAndEdgesFrom(row)
  }
  return { solo: createSoloGraphFromNeo4jNodesAndEdges(nodes, edges), rest: rest }
}

/** Execute the Cypher query and parse nodes/rels into SoloGraph nodes and edges (and put anything else in the rest collection)
 * Can do it explicitly via:
 * MATCH g = ()-[]->() limit 100 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output
 * or we will take a graph returned and flatten into nodes and edges for a Graph
*/
export async function createSoloGraphFromCypherQuery(db: DbContextType, cypherQuery: string, parms?): Promise<CreateSoloGraphResponseType> {
  return createSoloGraphFromNeo4jResults(await executeCypher(db, cypherQuery, parms))
}

/** Extend db-client main CLI to test SoloGraph support */
export function addSoloDbFunctions() {
  const p = PredefinedDbFunctions

  /** Test production of SoloGraph from structured result 
   *  MATCH g = ()-[]->() limit 100 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output
   */
  p.testSoloGraph = async function testSoloGraph(db, parms) {
    const graph = await createSoloGraphFromCypherQuery(db, `MATCH g = ()-[]->() limit 5 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output`, parms)
    return graph
  }

  /** Test production of empty SoloGraph from result that are not nodes and relationships  */
  p.testSoloGraph1 = async function testSoloGraph1(db, parms) {
    const graph = await createSoloGraphFromCypherQuery(db, `MATCH (n)-[]->(p) limit 5 return properties(n) as n, properties(p) as p`, parms)
    return graph
  }

  /** Test production of SoloGraph objects from unstructured result 2 - Neo4j Path */
  p.testSoloGraph2 = async function testSoloGraph2(db, parms) {
    const graph = await createSoloGraphFromCypherQuery(db, `MATCH g = ()-[]->() limit 5 return g`, parms)
    return graph
  }

  /** Test production of SoloGraph objects from unstructured result 3 - explicit returns */
  p.testSoloGraph3 = async function testSoloGraph3(db, parms) {
    const graph = await createSoloGraphFromCypherQuery(db, `MATCH (n)-[r]->(p) limit 5 return n,r,p`, parms)
    return graph
  }

} // end addMoreDbFunctions

async function index(parms?) {
  addMoreDbFunctions()
  addSoloDbFunctions()
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
