import { newDbContext, executeCypher, dbClose } from '../db-client'

/** Sample of how to import a Solograph object to Neo4j
 * Determine key to use besides identifier index and make it a constraint
 */
const graph = {"nodes":[
  {"type":"Station","in":[],"out":[0,1,2,3],"props":{"name":"Washington","zip":"143"}},
  {"type":"Station","in":[0],"out":[],"props":{"name":"Baltimore","zip":"142"}},
  {"type":"Station","in":[1],"out":[],"props":{"name":"Richmond","zip":"144"}},
  {"type":"Station","in":[2],"out":[],"props":{"name":"Harrisburg","zip":"126"}},
  {"type":"Station","in":[3],"out":[],"props":{"name":"Norfolk","zip":"141"}},
  {"type":"Station","in":[],"out":[4,5,6,7,8],"props":{"name":"Baltimore","zip":"142"}},
  {"type":"Station","in":[4],"out":[],"props":{"name":"Washington","zip":"143"}},
  {"type":"Station","in":[5],"out":[],"props":{"name":"Harrisburg","zip":"126"}},
  {"type":"Station","in":[6],"out":[],"props":{"name":"Reading","zip":"125"}},
  {"type":"Station","in":[7],"out":[],"props":{"name":"Philadelphia","zip":"124"}},
  {"type":"Station","in":[8],"out":[],"props":{"name":"Richmond","zip":"144"}},
  {"type":"Station","in":[],"out":[9,10,11],"props":{"name":"Norfolk","zip":"141"}},
  {"type":"Station","in":[9],"out":[],"props":{"name":"Richmond","zip":"144"}},
  {"type":"Station","in":[10],"out":[],"props":{"name":"Washington","zip":"143"}},
  {"type":"Station","in":[11],"out":[],"props":{"name":"Raleigh","zip":"212"}},
  {"type":"Station","in":[],"out":[12,13,14],"props":{"name":"Richmond","zip":"144"}},
  {"type":"Station","in":[12],"out":[],"props":{"name":"Norfolk","zip":"141"}},
  {"type":"Station","in":[13],"out":[],"props":{"name":"Washington","zip":"143"}},
  {"type":"Station","in":[14],"out":[],"props":{"name":"Baltimore","zip":"142"}}],
  "rels":[
  {"type":"Path","from":0,"to":1,"props":{}},
  {"type":"Path","from":0,"to":2,"props":{}},
  {"type":"Path","from":0,"to":3,"props":{}},
  {"type":"Path","from":0,"to":4,"props":{}},
  {"type":"Path","from":5,"to":6,"props":{}},
  {"type":"Path","from":5,"to":7,"props":{}},
  {"type":"Path","from":5,"to":8,"props":{}},
  {"type":"Path","from":5,"to":9,"props":{}},
  {"type":"Path","from":5,"to":10,"props":{}},
  {"type":"Path","from":11,"to":12,"props":{}},
  {"type":"Path","from":11,"to":13,"props":{}},
  {"type":"Path","from":11,"to":14,"props":{}},
  {"type":"Path","from":15,"to":16,"props":{}},
  {"type":"Path","from":15,"to":17,"props":{}},
  {"type":"Path","from":15,"to":18,"props":{}}]
}

// (Note that I'm using backticks for the cypher strings to allow javascript variable substitution ${x} here vs.  Cypher variable substitution via $x)
async function update(db) {
  await executeCypher( db, `create constraint Station_name if not exists for (n:Station) require n.name is unique ;` );
  for (const node of graph.nodes) {
    await executeCypher( db, `merge (n:${node.type}{name:$name}) SET props+=$props`, { name: node.props.name, props: node.props } );
  }
  for (const rel of graph.rels) {
    const fromName = graph.nodes[rel.from]?.props?.name
    const toName = graph.nodes[rel.to]?.props?.name
    if (fromName == null || toName == null) {
      throw new Error( `missing node name - fromName ${fromName} toName ${toName}` );
    }
    const fromLabel=graph.nodes[rel.from].type
    const toLabel=graph.nodes[rel.to].type
    await executeCypher(db, `merge (n:${fromLabel}{name:fromName})-[r:${rel.type}]->(p:${toLabel}{name:toName}) SET r+=relProps`,
       {fromName: fromName, toName: toName, relProps: rel.props }
    );
  }
}

async function main() {
  const db = newDbContext({}); // Set env vars right so no parms needed here
  try {
    await update(db);
  } finally {
    dbClose(db);
  }
}
