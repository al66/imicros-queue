/**
 * @license MIT, imicros.de (c) 2022 Andreas Leinen
 */
 "use strict";

const Cassandra = require("cassandra-driver");
const TimeUuid = require('cassandra-driver').types.TimeUuid;
const crypto = require("crypto");
const { Serializer } = require("./serializer/base");
 
 module.exports = {
     name: "worker.queue",
     
     /**
      * Service settings
      */
     settings: {
         /*
         cassandra: {
             contactPoints: ["192.168.2.124"],
             datacenter: "datacenter1",
             keyspace: "imicros_queue",
             queueTable: "queue",
             pointerTable: "pointer"
         },
         services: {
             keys: "keys"
         }
         */
     },
 
     /**
      * Service metadata
      */
     metadata: {},
 
     /**
      * Service dependencies
      */
     //dependencies: [],	
 
     /**
      * Actions
      */
     actions: {
         
         /**
          * Add entry to queue 
          * 
          * @param {String} serviceId - uuid
          * @param {String} value     - task to be queued  
          * @param {String} token     - instance token  
          * 
          * @returns {Boolean} result
          */
         add: {
             acl: "before",
             params: {
                 serviceId: { type: "uuid" },
                 value: { type: "any" },
                 token: { type: "object", optional: true }
             },
             async handler(ctx) {
                 let ownerId = ctx?.meta?.ownerId ?? null;
 
                 let oek;
                 // get owner's encryption key
                 try {
                     oek = await this.getKey({ ctx: ctx });
                 } catch (err) {
                     throw new Error("failed to receive encryption keys");
                 }
                 
                 let content = {
                    value: await this.serializer.serialize(ctx.params.value),
                    token: ctx.params.token ? await this.serializer.serialize(ctx.params.token) : null
                 };
                 let value = await this.serializer.serialize(content)
                 // encrypt value
                 let iv = crypto.randomBytes(this.encryption.ivlen);
                 try {
                     // hash encription key with iv
                     let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                     // encrypt value
                     value = this.encrypt({ value: value, secret: key, iv: iv });
                 } catch (err) {
                     this.logger.error("Failed to encrypt value", { 
                         error: err, 
                         iterations: this.encryption.iterations, 
                         keylen: this.encryption.keylen,
                         digest: this.encryption.digest
                     });
                     throw new Error("failed to encrypt");
                 }
                 
                 let query = "INSERT INTO " + this.queueTable + " (owner,service,ts,worker,value,oek,iv) VALUES (:owner,:service,now(),:worker,:value,:oek,:iv);";
                 let params = { 
                     owner: ownerId, 
                     service: ctx.params.serviceId, 
                     worker: "#",  // dummy for queue entries
                     value,
                     oek: oek.id,
                     iv: iv.toString("hex")
                 };
                 try {
                     await this.cassandra.execute(query, params, {prepare: true});
                     return true;
                 } catch (err) /* istanbul ignore next */ {
                     this.logger.error("Cassandra insert error", { error: err.message, query: query, params: params });
                     return false;
                }
             }
         },
         
         /**
          * Fetch next entry from the queue 
          * 
          * @param {String} serviceId        - uuid
          * @param {String} workerId         - external unique id of the worker, can be either uid or a string
          * @param {Number} timeToRecover    - time in ms after unacknowledged entries are reassigned
          * 
          * @returns {Object} value
          */
         fetch: {
            acl: "before",
            params: {
                serviceId: { type: "uuid", optional: true },
                workerId: { type: "string" },
                timeToRecover: { type: "number", optional: true }
            },
            async handler(ctx) {
                let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                let ownerId = ctx?.meta?.ownerId ?? null;
                if (!ownerId || !serviceId) throw new Error("not authenticated");

                //###  get all worker pointers
                let match;
                let recoverTime = new Date();
                let start = {
                    ts: null,
                    previous: null
                };
                let previous;
                let query = "SELECT owner, service, ts, worker, ttr, value, oek, iv, current FROM " + this.queueTable;
                query += " WHERE owner = :owner AND service = :service AND ts = minTimeuuid('1970-01-01 00:00:00');";
                let params = { 
                    owner: ownerId, 
                    service: serviceId
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    if (result.rows && Array.isArray(result.rows)) {
                        // check for unacknowledged items or recoverable items
                        for (const row of result.rows) {
                            // note start timeuuid for search
                            if (row.current) {
                                if (!start.previous || row.current.getDate() > start.previous) {
                                    start.previous = row.current.getDate();
                                    start.ts = row.get("current");
                                }
                            }
                            // anything to resend or to recover?
                            if (row.get("value") !== null) {
                                if (row.get("worker") === ctx.params.workerId) {
                                    match = row;
                                    match.resend = true;
                                    break;
                                } else {
                                    let ttr = new Date(row.ttr);
                                    if (ttr < recoverTime ) {
                                        match = row;
                                        match.recover = true;
                                        break;
                                    }
                                }
                            }
                        };
                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query });
                    return {};
                }

                //### recover
                if (match && match.recover) {
                    // set time to recover
                    let ttr = new Date();
                    ttr.setSeconds(ttr.getSeconds() + (ctx.params.timeToRecover || 60)); // default 60 s

                    // try recover
                    let queries =[];
                    queries.push({
                        query: "UPDATE " + this.queueTable + " SET ttr = null, value = null, oek = null, iv = null, current = null WHERE owner = :owner AND service = :service AND ts = minTimeuuid('1970-01-01 00:00:00') AND worker = :worker IF current = :current;",
                        params: {
                            owner: ownerId, 
                            service: serviceId, 
                            worker: match.get("worker"),
                            current: match.get("current")
                        }
                    });
                    queries.push({
                        // 
                        query: "INSERT INTO " + this.queueTable + " (owner,service,ts,worker,ttr,value,oek,iv,current) VALUES (:owner,:service,minTimeuuid('1970-01-01 00:00:00'),:worker,:ttr,:value,:oek,:iv,:current);",
                        params: {
                            owner: ownerId, 
                            service: serviceId, 
                            worker: ctx.params.workerId, 
                            ttr: ttr,
                            value: match.get("value"),
                            oek: match.get("oek"),
                            iv: match.get("iv"),
                            current: match.get("current")
                        }
                    });
                    try {
                        let result = await this.cassandra.batch(queries, { prepare: true });
                    } catch (err) {
                        this.logger.warn("Failed to recover", err, queries);
                        match = null;
                    }
                }

                //### nothing to re-send or to recover: fetch next
                if (!match) {
                    // set starting point (time uuid) for search
                    let tsStart = start.ts || TimeUuid.fromDate(new Date("1999-01-01T00:00:00"));

                    query = "SELECT owner, service, ts, value, oek, iv, fetched FROM " + this.queueTable;
                    query += " WHERE owner = :owner AND service = :service AND ts >= :start LIMIT 10;";
                    params = { 
                        owner: ownerId, 
                        service: serviceId, 
                        start: tsStart
                    };
                    try {
                        let result = await this.cassandra.execute(query, params, { prepare: true });
                        if (result.rows && Array.isArray(result.rows)) {
                            for (const row of result.rows) {
                                    // set time to recover
                                    let ttr = new Date();
                                    ttr.setSeconds(ttr.getSeconds() + (ctx.params.timeToRecover || 60)); // default 60 s

                                    if (!row.fetched) {
                                        // try to fetch a row
                                        let queries =[];
                                        queries.push({
                                            query: "UPDATE " + this.queueTable + " SET fetched = :fetched WHERE owner = :owner AND service = :service AND ts = :ts AND worker = :worker IF fetched = null;",
                                            params: {
                                                owner: ownerId, 
                                                service: serviceId, 
                                                ts: row.get("ts"),
                                                worker: "#",
                                                fetched: new Date()
                                            }
                                        });
                                        queries.push({
                                            // 
                                            query: "INSERT INTO " + this.queueTable + " (owner,service,ts,worker,ttr,value,oek,iv,current) VALUES (:owner,:service,minTimeuuid('1970-01-01 00:00:00'),:worker,:ttr,:value,:oek,:iv,:current);",
                                            params: {
                                                owner: ownerId, 
                                                service: serviceId, 
                                                worker: ctx.params.workerId, 
                                                ttr: ttr,
                                                value: row.get("value"),
                                                oek: row.get("oek"),
                                                iv: row.get("iv"),
                                                current: row.get("ts")
                                            }
                                        });
                                        try {
                                            let result = await this.cassandra.batch(queries, { prepare: true });
                                            match = row;
                                            break;
                                        } catch (err) {
                                            this.logger.warn("Failed to fetch", err, queries);
                                        }
                                    }
                            };
                        }
                    } catch (err) /* istanbul ignore next */ {
                        this.logger.error("Cassandra select error", { error: err.message, query });
                        return {};
                    }
                }

                //### decrypt and return value of fetched entry
                if (match) {
                    let oekId = match.get("oek");
                    let iv = Buffer.from(match.get("iv"), "hex");
                    let encrypted = match.get("value");
                    let value = null;
                    
                    // get owner's encryption key
                    let oek;
                    try {
                        oek = await this.getKey({ ctx: ctx, id: oekId });
                    } catch (err) {
                        this.logger.Error("Failed to retrieve owner encryption key", { ownerId, key: oekId });
                        throw new Error("failed to retrieve owner encryption key");
                    }

                    // decrypt value
                    try {
                        // hash received key with salt
                        let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                        value = this.decrypt({ encrypted: encrypted, secret: key, iv: iv });
                    } catch (err) {
                        throw new Error("failed to decrypt");
                    }
                    
                    // deserialize value
                    let container = await this.serializer.deserialize(value);
                    let val = await this.serializer.deserialize(container.value)
                    return val;
                } else {
                    this.logger.debug("Unvalid or empty result", { match });
                    return null;
                }

            }

         },
 
         /**
          * Confirm the last fetched task 
          * 
          * @param {String} serviceId        - uuid
          * @param {String} workerId         - external unique id of the worker, can be either uid or a string
          * @param {Any}    result           - optional: result to store in the context
          * @param {Object} error            - optional: error object, if handling failed
          * 
          * @returns {Boolean} result
          */
         ack: {
             acl: "before",
             params: {
                 serviceId: { type: "uuid", optional: true },
                 workerId: { type: "string" },
                 result: { type: "any", optional: true },
                 error: { type: "object", optional: true }
             },
             async handler(ctx) {
                 let serviceId = ctx?.meta?.service?.serviceId ?? (ctx?.params?.serviceId ?? null );
                 let ownerId = ctx?.meta?.ownerId ?? null;
                 if (!ownerId || !serviceId) throw new Error("not authenticated");
 
                //###  get current worker pointer
                let query = "SELECT owner, service, ts, worker, ttr, value, oek, iv FROM " + this.queueTable;
                query += " WHERE owner = :owner AND service = :service AND ts = minTimeuuid('1970-01-01 00:00:00') AND worker = :worker;";
                let params = { 
                    owner: ownerId, 
                    service: serviceId, 
                    worker: ctx.params.workerId
                };
                try {
                    let result = await this.cassandra.execute(query, params, { prepare: true });
                    let row = result.first();
                    if (row && row.get("value") !== null) {
                        // 
                        let oekId = row.get("oek");
                        let iv = Buffer.from(row.get("iv"), "hex");
                        let encrypted = row.get("value");
                        let value = null;
                        
                        // get owner's encryption key
                        let oek;
                        try {
                            oek = await this.getKey({ ctx: ctx, id: oekId });
                        } catch (err) {
                            this.logger.Error("Failed to retrieve owner encryption key", { ownerId, key: oekId });
                            throw new Error("failed to retrieve owner encryption key");
                        }
    
                        // decrypt value
                        try {
                            // hash received key with salt
                            let key = crypto.pbkdf2Sync(oek.key, iv, this.encryption.iterations, this.encryption.keylen, this.encryption.digest);
                            value = this.decrypt({ encrypted: encrypted, secret: key, iv: iv });
                        } catch (err) {
                            this.logger.Error("Failed to decrypt", { ownerId, key: oekId });
                            throw new Error("failed to decrypt");
                        }
                        
                        // deserialize container
                        let container = await this.serializer.deserialize(value);

                        // commit
                        let query = "UPDATE " + this.queueTable + " SET value = null, ttr = null, iv = null, oek = null WHERE owner = :owner AND service = :service AND ts = :ts AND worker = :worker;";
                        let params = {
                            owner: ownerId, 
                            service: serviceId, 
                            ts: row.get("ts"),
                            worker: ctx.params.workerId
                        }
                        try {
                            await this.cassandra.execute(query, params, { prepare: true });
                        } catch (err) /* istanbul ignore next */ {
                            this.logger.error("Cassandra update error", { error: err.message, query, params });
                            return false;
                        }

                        // given token ?
                        if (container.token) {
                            let token = await this.serializer.deserialize(container.token);
                            if (token && token.processId) {
                                let params = {
                                    token
                                };
                                if (ctx.params.result) params.result = ctx.params.result;
                                if (ctx.params.error) params.error = ctx.params.error;
                                this.logger.debug("call activity.completed", { params, meta: ctx.meta });
                                await ctx.call(this.services.activity + ".completed", params, { meta: ctx.meta });
                            }
                        }

                        return true;

                    }
                } catch (err) /* istanbul ignore next */ {
                    this.logger.error("Cassandra select error", { error: err.message, query, params, err });
                    return false;
                }
            }
         }
         
     },
 
     /**
      * Events
      */
     events: {},
 
     /**
      * Methods
      */
     methods: {
         
         async getKey ({ ctx = null, id = null } = {}) {
             
             let result = {};
             
             // try to retrieve from keys service
             let opts;
             if ( ctx ) opts = { meta: ctx.meta };
             let params = { 
                 service: this.name
             };
             if ( id ) params.id = id;
             
             // call key service and retrieve keys
             try {
                 result = await this.broker.call(this.services.keys + ".getOek", params, opts);
                 this.logger.debug("Got key from key service", { id: id });
             } catch (err) {
                 this.logger.error("Failed to receive key from key service", { id: id, meta: ctx.meta });
                 throw err;
             }
             if (!result.id || !result.key) throw new Error("Failed to receive key from service", { result: result });
             return result;
         },
         
         encrypt ({ value = ".", secret, iv }) {
             let cipher = crypto.createCipheriv("aes-256-cbc", secret, iv);
             let encrypted = cipher.update(value, "utf8", "hex");
             encrypted += cipher.final("hex");
             return encrypted;
         },
 
         decrypt ({ encrypted, secret, iv }) {
             let decipher = crypto.createDecipheriv("aes-256-cbc", secret, iv);
             let decrypted = decipher.update(encrypted, "hex", "utf8");
             decrypted += decipher.final("utf8");
             return decrypted;            
         },
         
         async connect () {
 
             // connect to cassandra cluster
             await this.cassandra.connect();
             this.logger.info("Connected to cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
             
             // create tables, if not exists
             let query = `CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.queueTable} `;
             query += " ( owner varchar, service uuid, ts timeuuid, worker varchar, ttr timestamp, value varchar, oek uuid, iv varchar, current timeuuid, fetched timestamp, PRIMARY KEY ((owner,service),ts,worker) ) ";
             query += " WITH comment = 'storing queue entries';";
             await this.cassandra.execute(query);
 
         },
         
         async disconnect () {
 
             // close all open connections to cassandra
             await this.cassandra.shutdown();
             this.logger.info("Disconnected from cassandra", { contactPoints: this.contactPoints, datacenter: this.datacenter, keyspace: this.keyspace });
             
         }
         
     },
 
     /**
      * Service created lifecycle event handler
      */
     async created() {
 
         // encryption setup
         this.encryption = {
             iterations: 1000,
             ivlen: 16,
             keylen: 32,
             digest: "sha512"
         };
         
         this.serializer = new Serializer();
 
        // cassandra setup
        this.contactPoints = ( this.settings?.cassandra?.contactPoints ?? "127.0.0.1" ).split(",");
        this.datacenter = this.settings?.cassandra?.datacenter ?? "datacenter1";
        this.keyspace = this.settings?.cassandra?.keyspace ?? "imicros_queue";
        this.queueTable = this.settings?.cassandra?.queueTable ?? "queue";
        this.pointerTable = this.settings?.cassandra?.pointerTable ?? "pointer";
        this.config = {
            contactPoints: this.contactPoints, 
            localDataCenter: this.datacenter, 
            keyspace: this.keyspace, 
            protocolOptions: { 
                port: this.settings?.cassandra?.port ?? (process.env.CASSANDRA_PORT || 9042 )
            },
            credentials: { 
                username: this.settings?.cassandra?.user ?? (process.env.CASSANDRA_USER || "cassandra"), 
                password: this.settings?.cassandra?.password ?? (process.env.CASSANDRA_PASSWORD || "cassandra") 
            }
        };
        this.cassandra = new Cassandra.Client(this.config);
 
         // set actions
         this.services = {
             keys: this.settings?.services?.keys ?? "keys",
             activity: this.settings?.services?.activity ?? "activity"
         };        
         
         this.broker.waitForServices(Object.values(this.services));
         
     },
 
     /**
      * Service started lifecycle event handler
      */
     async started() {
 
         // connect to db
         await this.connect();
         
     },
 
     /**
      * Service stopped lifecycle event handler
      */
     async stopped() {
         
         // disconnect from db
         await this.disconnect();
         
     }
 
 };
