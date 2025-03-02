# Neo4j Typescript Client
 
 DB Client for Neo4j DB - implemented as functions (not classes)

A thin wrapper for Neo4j Graph DB Javascript driver.

Simple usage:
```
const db = newDbContext({dbName:'neo4j', dbUrl:'neo4j://localhost:7687'})
const arrayOfObjectResults = executeCypher(db, `return 'this is a test'`)
dbClose(db)
```

See main() method for detailed example that also acts as a CLI:
```
npm run --silent -- cypher --help
npm run --silent --silent -- cypher --dbName neo4j --dbUrl "neo4j://localhost:7687" "match (n) return count(n) as numNodes;"
```

The list of PredefinedDbFunctions can be extended in external modules - see [db-funcs.ts](./src/db-funcs.ts) and run `npm run db-funcs`
 
Neo4j DB driver docs are at https://neo4j.com/docs/api/javascript-driver/current/

## Installation
From command-line, this is the initial setup to select correct version of nodejs and pull in libraries:
```
nvm use 
node --version
npm install
npm run build

npm run --silent -- cypher --help
```
Tell the npm command to pass all additional parameters to the  `cypher` script listed in `package.json` by using `npm run --silent -- cypher "blah blah"`.   Without the `--`, npm will interpret any option (like `--dbUrl`) as it's own parameter rather than pass it on to the cypher script.

### Using yarn
The yarn command `yarn --silent` can be used run things instead of `npm run --`.
Here it is installed in the default node environment using the -g option so it is in the PATH for command-line:
```
npm install -g yarn
npm build
yarn cypher --help
yarn --silent cypher --help
```
Note that default yarn command reports which command is run and the timings, so the `--silent` parameter prevents that noise.

### Using nvm or setting up nodejs
The code is developed using a relatively new version of nodejs 22.13.1 but the code should be compatible with a wide variety of node versions.  A comprehensive list of ways to install nodejs is at https://nodejs.org/en/download/package-manager/all 

We use the `nvm` tool (https://github.com/nvm-sh/nvm) with an `.nvmcrc` file that specifies a version of node to develop and test against. It can be used to setup that version of nodejs just by typing `nvm` in the project working directory.

A decent version of nodejs can be installed via an OS package manager - for instance `apk add nodejs npm`, `sudo dnf install nodejs`, `sudo yum install nodejs`, `brew install nodejs`, or `winget install OpenJS.NodeJS`.  

### Windows specific issues
Note that a standard version of nodejs can be installed from the Windows App store on the CLI via `winget install OpenJS.NodeJS` or `winget install OpenJS.NodeJS.LTS`.

Also for windows, `nvm-windows` can be used to manage nodejs versions: https://learn.microsoft.com/en-us/windows/dev-environment/javascript/nodejs-on-windows#install-nvm-windows-nodejs-and-npm (`winget install CoreyButler.NVMforWindows`).
It may need to be initially setup via `nvm install 22.13.1` and `nvm use 22.13.1` since it does not read the `.nvmrc` file.

To provide to provide linux-like CLI to run things there are several options on windows:
* WSL Windows Subsystem for Linux https://learn.microsoft.com/en-us/windows/wsl/install (`wsl --install` or `winget install Microsoft.WSL` ).
* git-bash https://gitforwindows.org/ (lighter, easy-to-install `winget install Git.Git`),
* Cygwin https://www.redhat.com/en/blog/hybrid-system-cygwin (more complex setup options `winget install Cygwin.Cygwin`).

 WSL allows for much better integration for doing further development and open-source work, but other tools work well too.  Probably `git-bash` may be the simplest and lightest way to get `git` and linux-like `bash` behavior on a windows system. 

## Usage
If there is a local server on neo4j://localhost:7687, one of the predefined functions can be used to check connectivity and node counts, or do a real query:
```
export NEO4J_PASSWORD=pickapassw0rd
export NEO4J_USERNAME=neo4j
npm run --silent -- cypher ok
npm run --silent -- cypher nodeCount
npm run --silent -- cypher "match ()-[r]->() return count(r) as numRelationships;"
```

Use the `--log` parameter to show more internal details.

A readonly connection is given by default (no writes allowed).  Pass the `--allowwrite` parameter to allow updates and deletes (be careful!).  This can also be set as a default by setting the environment variable `NEO4J_ALLOWWRITE=1`.
This can be overridden by forcing a read-only connection for a particular invocation via the `--readonly` parameter.

Connectivity parameters can be passed via environment variables or command-line:
```
export NEO4J_PASSWORD=pickapassw0rd
export NEO4J_USERNAME=neo4j
export NEO4J_DBURL=neo4j+s://abcd5678.databases.neo4j.io:7687
export NEO4J_DBNAME=neo4j
npm run --silent -- cypher echo foo=bar param2=2

NEO4J_PASSWORD=pickapassw0rd 
npm run --silent -- cypher --dbUrl 'neo4j://localhost:7687' echo foo=bar param2=2
```

The last syntax allows passing string parameters from the CLI.  Be careful about nesting single and double quotes correctly, and for most CLI shells characters like dollar-sign ($) need special handling when specifying queries:
```
npm run --silent -- cypher --allowwrite "merge (n:Value{name:'foo'}) SET n.value='bar' return n.value as value"
npm run --silent -- cypher "match (n:Value) where n.name=\$findName return properties(n) as props;" findName=foo
```

Here are some more variations, creating a Value node label with unique 'name' field as a key:
```
npm run --silent -- cypher --allowwrite "create constraint Value_name if not exists for (n:Value) require n.name is unique ;"
npm run --silent -- cypher --allowwrite  "merge (n:Value{name:'rightnow'}) set n.value=toString(datetime()) return n.value as value ;"
npm run --silent -- cypher --allowwrite "merge (n:Value{name:\$key}) set n.value=\$value return n.value as value ;" key=key1 "value=value for key1"
npm run --silent -- cypher "match (n:Value) with properties(n) as keyVal return keyVal"
npm run --silent -- cypher "match (n:Value) return collect(n {.*}) as keyVals"
npm run --silent -- cypher "match (n:Value) with properties(n) as props return apoc.map.fromPairs(collect([props.name,props.value])) as dict"
```
Note that spaces in parameters need to be handled by quoting the entire key=value pair `value=value for key`"


## Writing new DB functions
See the `PredefinedDbFunctions` table in [db-client.ts](./src/db-client.ts) for examples of how to use the API functions, and the `main()` function to see how `executeCypher()` is used to invoke a Cypher query by string or javascript function given a specific DB context setup via the `newDbContext()` function.   The `dbResultAsObjects()` function create javascript objects out of the raw result set.

An example of how to include `db-client` as a module and enhance the available db functions is in `db-func.ts`(./src/db-func.ts).

Note that various advanced parameters for sessions and transactions may be set.  The IDE / VSCode typescript plugin should be helpful and guide we to appropriate code comments, and the online docs for the Neo4j javascript driver are at https://neo4j.com/docs/api/javascript-driver/current/.

## Potential Enhancements
* use the Neo4j DbResult in a streaming mode to allow larger-than-memory results (can be redirected to a file) `dbResultAsStream()`
* Handle invocations from a browser web client
* Use browser typescript support via Module Shims 2.0 - support typescript type erasure https://github.com/guybedford/es-module-shims (Guy Bedford 2025-02-27): https://guybedford.com/es-module-shims-2.0

## Web client Development
Snippets for code for web development (WIP)
Issues with CORS and WebSockets may need to be addressed.

Note that we install yarn and http-server in the global path:
```
npm install -g yarn
npm install -g http-server
```

Start http server for web client (avoiding CORS issues with local files)
```
yarn build-web
yarn start-web
```

## Local Neo4j
For some tips on running a local Neo4j instance, see [running-neo4j.md](./running-neo4j.md)

 It may be easier to allocate a free Aura instance instead at https://console-preview.neo4j.io - make sure to collect the username and password when it is created.   The URL will be something like `neo4j+s://abcd5We678.databases.neo4j.io:7687` with `abcd5678` being the database instance id.
