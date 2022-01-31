"use strict";
const { ServiceBroker } = require("moleculer");
const { Queue } = require("../index");
const { v4: uuid } = require("uuid");

const tasks = {
    a: { msg: "first task" },
    b: { msg: "second task" },
    c: { msg: "third task" },
    d: { msg: "task with token" }
};

const workers = {
    A: uuid(),
    B: uuid(),
    C: uuid()
};

// helper & mocks
const { ACLMiddleware, user, ownerId } = require("./helper/acl");
const { Activity, activity } = require("./helper/activity");
const { Keys } = require("./helper/keys");

// collect events
const collect = [];
const Collect = {
    name: "collect",
    events: {
        "**"(payload, sender, event, ctx) {
            collect.push({ payload, sender, event, ctx });
        }
    }
};

describe("Test context service", () => {

    let broker, service, opts, keyService;
    beforeAll(() => {
    });
    
    afterAll(async () => {
    });
    
    describe("Test create service", () => {

        it("it should start the broker", async () => {
            broker = new ServiceBroker({
                middlewares:  [ACLMiddleware],
                logger: console,
                logLevel: "info" //"debug"
            });
            keyService = await broker.createService(Keys);
            service = await broker.createService(Queue, Object.assign({ 
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
            await broker.createService(Collect);
            await broker.createService(Activity);
            await broker.start();
            expect(service).toBeDefined();
            expect(keyService).toBeDefined();
        });

    });

    describe("Test queue - write to queue", () => {

        const serviceId = uuid();

        beforeEach(() => {
            opts = { meta: { user, ownerId } };
        });

        it("it should add a task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.a
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a second task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.b
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a third task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.c
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

    });

    describe("Test queue - fetch from queue", () => {

        const serviceId = uuid();

        beforeEach(() => {
            opts = { meta: { user, ownerId } };
        });

        it("it should add a task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.a
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a second task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.b
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a third task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.c
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        it("it should fetch the first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });

        it("it should fetch the first task again", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });

        it("it should fetch the second task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.B
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.b);
            });
        });

        it("it should fetch the second task again", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.B
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.b);
            });
        });

    });

    describe("Test queue - ack", () => {

        const serviceId = uuid();

        beforeEach(() => {
            opts = { meta: { user, ownerId } };
        });

        it("it should add a task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.a
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a second task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.b
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should fetch the first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });

        it("it should ack the first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.ack", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should fetch the second task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.b);
            });
        });

        it("it should fetch nothing ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.B
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(null);
            });
        });

    });

    describe("Test queue - recover", () => {

        const serviceId = uuid();

        beforeEach(() => {
            opts = { meta: { user, ownerId } };
        });

        it("it should add a task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.a
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should add a second task ", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.b
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });

        it("it should fetch the first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A,
                timeToRecover: 1
            };
            return broker.call("worker.queue.fetch", params, opts).delay(1000).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });
        
        it("it should recover the first task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.B
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });

        it("it should fetch the second task ", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.b);
            });
        });
        
        it("it should fetch the first task again", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.B
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.a);
            });
        });
       
    });

    describe("Test queue - task with token", () => {

        const serviceId = uuid();

        let token = {
            processId: uuid(),
            instanceId: uuid(),
            elementId: uuid(),
            type: "Service Task",
            status: "ACTIVITY.READY",
            user: {
                id: uuid()
            },
            ownerId: uuid(),
            attributes: { }
        };
        
        beforeEach(() => {
            opts = { meta: { user, ownerId } };
        });

        it("it should add a task with a token", () => {
            let params = {
                serviceId: serviceId,
                value: tasks.d,
                token
            };
            return broker.call("worker.queue.add", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
            });
        });
        
        it("it should fetch the task with token", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A,
            };
            return broker.call("worker.queue.fetch", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res.msg).toBeDefined();
                expect(res).toEqual(tasks.d);
            });
        });

        it("it should acknowledge the task with token and emit the token", () => {
            let params = {
                serviceId: serviceId,
                workerId: workers.A,
                result: 5
            };
            return broker.call("worker.queue.ack", params, opts).then(res => {
                expect(res).toBeDefined();
                expect(res).toEqual(true);
                expect(activity[0].params.token).toEqual(token);
                expect(activity[0].meta.ownerId).toEqual(ownerId);
            });
        });
        
    });
 
    describe("Test stop broker", () => {
        it("should stop the broker", async () => {
            expect.assertions(1);
            await broker.stop();
            expect(broker).toBeDefined();
        });
    });
    
});