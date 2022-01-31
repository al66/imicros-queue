# imicros-queue
[![Development Status](https://img.shields.io/badge/status-under_development-red)](https://img.shields.io/badge/status-under_development-red)

[Moleculer](https://github.com/moleculerjs/moleculer) service for persistent queue handling in the imicros backend

This service is part of the imicros-flow engine and provides a buffered work queue for pulling messages by external services.

For use by the external service the following actions are provided:

- fetch { serviceId, workerId, timeToRecover } => value
- ack { serviceId, workerId, ( result, error ) } => true|false

via the moleculer-web gateway these actions can be easily published for external access as named pathes - e.g.:
```
 api/queue/{serviceId}/fetch
 api/queue/{serviceId}/ack
```

Multiple external workers can work on the same service queue by an at least once guarantee. 

The tasks stored in cassandra are encrypted with a owner specific key (owner is set by the acl middleware).

Calling "fetch" for a worker returns the next task. Calling "fetch" again will return the same task - until with call of "ack" for this worker the task is confirmed. Then the next "fetch" returns the next task.
If a task has not been confirmed by the worker after "timeToRecover" (default: 60s) has expired, it is delivered to the next worker. A different "timeToRecover" can be specified for each "fetch".
With calling "ack", depending on the purpose of the service, a result can be returned or in case of errors also an error object.  
If the task has been added with a workflow token, the result and the error is returned to the workflow instance to continue the workflow.

All tasks are deleted after a fixed TTL ( default: 7 days ), the worker must therefore process his tasks within these 7 days.

## Installation
```
$ npm install imicros-flow-worker --save
```
## Dependencies
Requires a running [Cassandra](https://cassandra.apache.org/) node/cluster for storing the tasks in the queue.
Requires broker middleware AclMiddleware or similar: [imicros-acl](https://github.com/al66/imicros-acl)
Reuires a running key server for retrieving the owner encryption keys: [imicros-keys](https://github.com/al66/imicros-keys)

## Performance
As using Cassandra as a message queue with frequent deletion is an anti-pattern, we are using pointers for the consumer. But due to the required consistent updates in case of multiple workers the overhead when fetching slows down. As writes are possible within < 6 ms, fetching costs 4 accesses and at least about 24 ms.  

Performance tests on a real cassandra cluster haven't been done yet.

( Kafka unfortunatelly doesn't support varying topics and adding topics on the fly or creating consumers for a topic on the fly slows really down due to the necessary leadership selection ). 

# Usage

## Usage add (this is called by the workflow engine - not directly)
```js
let params = {
    serviceId: serviceId,
    value: { msg: "say hello to the world" }
};
let res = await broker.call("worker.queue.add", params, opts)
expect(res).toBeDefined();
expect(res).toEqual(true);

```
## Usage fetch
```js
let params = {
    serviceId: serviceId,
    workerId: "my external worker id"
};
let res = broker.call("worker.queue.fetch", params, opts)
expect(res).toBeDefined();
expect(res).toEqual({ msg: "say hello to the world" });

```
## Usage ack
```js
let params = {
    serviceId: serviceId,
    workerId: "my external worker id",
    result: "this is the result of the service, if available - can be any type: string, number, boolean or object"
    // error: { msg: "an error has occured" }
};
let res = broker.call("worker.queue.ack", params, opts)
expect(res).toBeDefined();
expect(res).toEqual(true);

```
