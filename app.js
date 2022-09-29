const CosmosClient = require('@azure/cosmos').CosmosClient
var dotenv = require('dotenv');
dotenv_path = './env/.env';
dotenv.config({path: dotenv_path});

// Cliente

const endpoint = process.env.ENDPOINT
const key = process.env.DB_KEY
const options = {
    endpoint: endpoint,
    key: key,
    userAgentSuffix: 'Copa-CosmosDB'
};
const client = new CosmosClient(options)

// Identificadores y datos

const db_name = "copadataset"
const container_name_1 = "aeropuertos"
const container_name_2 = "distancias"
const aeropuertos = require(`./data/${container_name_1}.json`)
const distancias = require(`./data/${container_name_2}.json`)
const partitionKey_1 =  { kind: 'Hash', paths: ['/IATA'] }
const partitionKey_2 =  { kind: 'Hash', paths: ['/ID'] }

/** Crear base de datos **/

async function createDatabase(db_name) {
    const { promesa } = await client.databases.createIfNotExists({
        id: db_name
    })
    console.log(`Se creó la base de datos ${db_name}.\n`)
}

/** Crear contenedor **/

async function createContainer(container_name, db_name, partitionKey) {
    const { promesa } = await client
        .database(db_name)
        .containers.createIfNotExists(
            { id: container_name, partitionKey }
        )
    console.log(`Se creó el contenedor ${container_name}.\n`)
}

/** Insertar ítem **/

async function insertItem(item, container_name, db_name) {
    const { promesa } = await client
        .database(db_name)
        .container(container_name)
        .items.upsert(item)
    console.log(`Se insertó el item ${item}\n`)
}

/** Insertar varios ítems **/

// https://github.com/Azure/azure-sdk-for-js/issues/6494
async function bulkCreate(items, container_name, db_name) {
    const container = await client.database(db_name).container(container_name)
    const promesas = []
    for (const item of items) {
        promesas.push(container.items.create(item))
    }
    return Promise.all(promesas)
}

/** Exit the app with a prompt **/

function exit(message) {
    console.log(message)
    console.log('Presionar cualquier tecla...')
    process.stdin.setRawMode(true)
    process.stdin.resume()
    process.stdin.on('data', process.exit.bind(process, 0))
}

async function queryContainer() {
    console.log(`Querying container:\n Aeropuertos`)

    // query to return all children in a family
    // Including the partition key value of country in the WHERE filter results in a more efficient query
    const querySpec = {
        query: 'SELECT VALUE r.children FROM root r WHERE r.partitionKey = @country'
    }

    const { resources: results } = await client
        .database(databaseId)
        .container("Aeropuertos")
        .items.query(querySpec)
        .fetchAll()
    for (var queryResult of results) {
        let resultString = JSON.stringify(queryResult, null, "\t")
        console.log(`\tQuery returned ${resultString}\n`)
    }
}

createDatabase(db_name)
.then(() => createContainer(container_name_1, db_name, partitionKey_1))
.then(() => createContainer(container_name_2, db_name, partitionKey_2))
.then(() => bulkCreate(aeropuertos, container_name_1, db_name))
.then(() => bulkCreate(distancias, container_name_2, db_name))
.then(() => {
    exit(`Proceso exitoso...`)
})
.catch(error => {
    exit(`Terminó con errores: \n ${JSON.stringify(error, null, "\t")}`)
})
