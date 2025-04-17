import {  DbContextType, executeCypher, main,  PredefinedDbFunctions } from '../db-client'
import { Graph } from './graph'

/** Functions to handle Solo graphs from https://github.com/WardCunningham/graph
 *  With correct packages.json run script entry, can invoke via:
 *   npm run db-solograph -- --help
 *   node -r ts-node/register/transpile-only src/solograph/db-solograph.ts
*/

export type EdgeResultNeo4j = {
  type: string,
  properties: {[key:string]: any},
  identity: bigint,
  start: bigint,
  end: bigint,
  elementId: string,
  startNodeElementId: string,
  endNodeElementId: string,
}

export type NodeResultNeo4j = {
  labels: string[],
  properties: {[key:string]: any},
  identity: bigint,
  elementId: string,
}

export type SoloGraphNode = {
  type:string,
  in:number[],
  out:number[],
  props:{[key:string]: any},
}

export type SoloGraphEdge = {
  type:string,
  from:number,
  to:number,
  props:{[key:string]: any},
}

/** Create a SoloGraph from the nodes and relationships from a Neo4j result. Uses first label of the node only.
 *  MATCH   g = ()-[]->() limit 100 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output
 */
export function createSoloGraphFromNeo4jNodesAndEdges(nodes:NodeResultNeo4j[], edges:EdgeResultNeo4j[]) {
  const nodeMap = new Map<string,number>() // Map from elementId to Graph node index
  const gNodes:SoloGraphNode[] = []
  for (const node of nodes) {
    const id = node.elementId ?? String(node.identity)
    const nodeIdx = nodeMap.get(id)
    if (nodeIdx == null) {
      const idx = gNodes.length
      const gNode:SoloGraphNode = {type:node.labels[0], in:[], out:[], props: node.properties}
      gNodes.push(gNode)
      nodeMap.set(id, idx)
    } else { // Merge properties (only happens if DISTINCT not used in Cypher query)
      const gNode = gNodes[nodeIdx]
      gNode.props = {...gNode.props, ...node.properties}
    }
  }
  const edgeMap = new Map<string,number>() // Map from elementId to Graph edge index
  const gEdges:SoloGraphEdge[] = []
  for (const edge of edges) {
    const id = edge.elementId ?? String(edge.identity)
    const edgeIdx = edgeMap.get(id)
    if (edgeIdx == null) {
      const idx = gEdges.length
      const fromIdx = nodeMap.get(edge.startNodeElementId ?? String(edge.start))
      const toIdx = nodeMap.get(edge.endNodeElementId ?? String(edge.end))
      const gEdge:SoloGraphEdge = {type:edge.type, props: edge.properties, from:fromIdx, to:toIdx}
      gEdges.push(gEdge)
      edgeMap.set(id, idx)
      gNodes[fromIdx].out.push(idx)
      gNodes[toIdx].in.push(idx)
    } else { // Merge properties (only happens if DISTINCT not used in Cypher query)
      const gEdge = gEdges[edgeIdx]
      gEdge.props = {...gEdge.props, ...edge.properties}
    }    
  }
  return new Graph(gNodes, gEdges)
}


/** Execute the Cypher query and parse nodes/rels into a SoloGraph 
 * Can do it explicitly via:
 * MATCH g = ()-[]->() limit 100 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output
 * or we will take a graph returned and flatten into nodes and edges for a Graph
*/
export  async function createSoloGraphFromCypherQuery(db:DbContextType, cypherQuery:string, parms?): Promise<Graph> {
  const results = await executeCypher(db, cypherQuery, parms)
  const graph = new Graph()
  for (const row of results) {
    if (row.output?.nodes && row.output?.edges) {
      return createSoloGraphFromNeo4jNodesAndEdges(row.output.nodes, row.output.edges)
    }
  // TODO: pull out nodes and edges from results
  }
  return graph
}


export function addMoreDbFunctions() {
  const p = PredefinedDbFunctions

  /** Test production of SoloGraph objects */
  p.testSoloGraph = async function testSoloGraph(db, parms) {
    const graph = await createSoloGraphFromCypherQuery(db, `MATCH g = ()-[]->() limit 5 return {nodes: apoc.coll.flatten(collect(distinct nodes(g))), edges: apoc.coll.flatten(collect(distinct relationships(g))) } as output`, parms)
    return graph
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
