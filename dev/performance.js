const { ServiceBroker } = require("moleculer");
const { Queue } = require("../index");
const { ACLMiddleware, user, ownerId } = require("../test/helper/acl");
const { Keys } = require("../test/helper/keys");
const { v4: uuid } = require("uuid");

process.env.CASSANDRA_CONTACTPOINTS = "192.168.2.124";
process.env.CASSANDRA_DATACENTER = "datacenter1";
process.env.CASSANDRA_KEYSPACE = "imicros_queue";
process.env.CASSANDRA_PORT = 31326;
process.env.CASSANDRA_USER = "cassandra";
process.env.CASSANDRA_PASSWORD = "cassandra";

const opts = { meta: { user, ownerId } };
const serviceId = uuid();
const n = 1000;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

const broker = new ServiceBroker({
    middlewares:  [ACLMiddleware],
    logger: console,
    logLevel: "info" //"debug"
});

const ts = new Date(); 

async function add() {
    const tasks = [];
    for (let i=0; i < n; i++) {
        tasks.push(broker.call("worker.queue.add", { serviceId, value: { i }}, opts));
        await delay(1);
    }
    await Promise.all(tasks);
    console.log("added", (new Date()) - ts);
};

async function fetch(workerId) {
    let task = await broker.call("worker.queue.fetch", { serviceId, workerId }, opts);
    if (task) {
        await broker.call("worker.queue.ack", { serviceId, workerId }, opts);
    }
    return task;
}

async function worker(tasks) {
    const workerId = uuid();
    let empty = 0;
    let errCount = 0;
    await delay(100);
    while(empty < 10) {
        try {
            let task = await fetch(workerId, tasks);
            // console.log(task);
            if (task) {
                tasks[task.i] = 1;
                empty = 0; 
            } else {
                // console.log("fetched first time null", { workerId, time: (new Date()) - ts });
                await delay(50);
                empty++;
            }
        } catch (err) {
            errCount++;
            await delay(10);
        }
    }
    console.log("fetched", { ownerId, serviceId, workerId, time: (new Date()) - ts, errCount });
    return Promise.resolve();
};

function received(tasks) {
    return tasks.reduce((n, val) => { return n + (val == 1) }, 0);    
};

async function run() {

    await broker.createService(Keys);
    await broker.createService(Queue, Object.assign({ 
        settings: { 
            cassandra: {
                contactPoints: process.env.CASSANDRA_CONTACTPOINTS || "127.0.0.1", 
                datacenter: process.env.CASSANDRA_DATACENTER || "datacenter1", 
                keyspace: process.env.CASSANDRA_KEYSPACE || "imicros_queue" 
            },
            services: {
                keys: "keys"
            }
        },
        dependencies: ["keys"]
    }));
    await broker.start();

    let tasks = [];
    // one worker
    // await Promise.all([add(), worker(tasks)]);
    // two workers
    await Promise.all([add(), worker(tasks), worker(tasks)]);
    // five workers
    // await Promise.all([add(), worker(tasks), worker(tasks), worker(tasks), worker(tasks), worker(tasks)]);

    // received tasks count
    console.log(received(tasks));

    await broker.stop();    

    process.exit(0);
}

run();
