// Cassandra settings
process.env.CASSANDRA_CONTACTPOINTS = "127.0.0.1";
process.env.CASSANDRA_DATACENTER = "datacenter1";
process.env.CASSANDRA_KEYSPACE = "imicros_flow";

/* Jest config */
module.exports = {
    testPathIgnorePatterns: ["/dev/"],
    coveragePathIgnorePatterns: ["/node_modules/","/dev/","/test/"]
};