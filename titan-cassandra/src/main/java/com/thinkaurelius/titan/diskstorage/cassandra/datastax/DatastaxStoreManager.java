package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.METRICS_JMX_ENABLED;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

@PreInitializeConfigOptions
public class DatastaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(DatastaxStoreManager.class);

    /**
     * CQL for get partitioner
     */
    private static final String SELECT_LOCAL = "SELECT partitioner FROM system.local WHERE key='local'";

    /**
     * CQL for create keyspace
     */
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class':'%s'%s}";

    /**
     * CQL for create table
     */
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (key blob,column1 blob,value blob,PRIMARY KEY (key,column1)) WITH comment = 'column family %s' ";

    //Config for datastax
    public static final ConfigNamespace DATASTAX_NS = new ConfigNamespace(CASSANDRA_NS, "datastax", "datastax-specific Cassandra options");

    /**
     * Default name for the Cassandra cluster //TODO should move to Cassandra Store Manager
     * <p/>
     */
    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(
                    DATASTAX_NS,
                    "cluster-name",
                    "Default name for the Cassandra cluster",
                    ConfigOption.Type.MASKABLE,
                    "Titan Cluster");

    //protocol config
    public static final ConfigOption<Integer> PROTOCOL_VERSION =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "protocol-version",
                    "The native protocol version to use." +
                            "The driver supports both version 1 and 2 of the native protocol. Version 2" +
                            "of the protocol has more features and should be preferred, but it is only" +
                            "supported by Cassandra 2.0 and above, so you will have to use version 1 with" +
                            "Cassandra 1.2 nodes." +
                            "By default the driver will 'auto-detect'(-1) which protocol version it can use" +
                            "when connecting to the first node",
                    ConfigOption.Type.FIXED,
                    2);

    public static final ConfigOption<ProtocolOptions.Compression> PROTOCOL_COMPRESSION =
            new ConfigOption<ProtocolOptions.Compression>(
                    DATASTAX_NS,
                    "protocol-compression",
                    "What kind of compression to use" +
                            "when sending data to a node: either no compression or snappy." +
                            "Snappycompression isoptimized for high speeds and reasonable compression",
                    ConfigOption.Type.FIXED,
                    ProtocolOptions.Compression.NONE
            );

    //pooling config
    public static final ConfigOption<HostDistance> HOST_DISTANCE =
            new ConfigOption<HostDistance>(
                    DATASTAX_NS,
                    "host-distance",
                    "The distance assigned to an host influences how many connections the driver" +
                            "maintains towards this host",
                    ConfigOption.Type.MASKABLE,
                    HostDistance.LOCAL
            );

    public static final ConfigOption<Integer> CORE_CONNECTIONS =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "core-connections",
                    "The core number of connections per host." +
                            "2 for HostDistance.LOCAL, 1 for HostDistance.REMOTE",
                    ConfigOption.Type.MASKABLE,
                    Integer.class
            );

    public static final ConfigOption<Integer> MAX_CONNECTIONS =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "max-connections",
                    "The maximum number of connections per host." +
                            "8 for HostDistance.LOCAL, 2 for HostDistance.REMOTE",
                    ConfigOption.Type.MASKABLE,
                    Integer.class
            );

    public static final ConfigOption<Integer> MIN_REQUESTS =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "min-requests",
                    "The number of simultaneous requests on a connection below" +
                            "which connections in excess are reclaimed",
                    ConfigOption.Type.MASKABLE,
                    25
            );

    public static final ConfigOption<Integer> MAX_REQUESTS =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "max-requests",
                    "The number of simultaneous requests on all connections " +
                            "to an host after which more connections are created",
                    ConfigOption.Type.MASKABLE,
                    100
            );

    //socket config
    public static final ConfigOption<Integer> CONNECT_TIMEOUT =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "connect-timeout",
                    "The connect timeout in milliseconds for the underlying Netty channel",
                    ConfigOption.Type.MASKABLE,
                    5000
            );

    public static final ConfigOption<Integer> READ_TIMEOUT =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "read-timeout",
                    "The read timeout in milliseconds for the underlying Netty channel.",
                    ConfigOption.Type.MASKABLE,
                    12000
            );

    public static final ConfigOption<Boolean> KEEP_ALIVE =
            new ConfigOption<Boolean>(
                    DATASTAX_NS,
                    "keep-alive",
                    "",
                    ConfigOption.Type.MASKABLE,
                    false
            );

    public static final ConfigOption<Boolean> REUSE_ADDRESS =
            new ConfigOption<Boolean>(
                    DATASTAX_NS,
                    "reuse-address",
                    "Whether to allow the same port to be bound to multiple times",
                    ConfigOption.Type.MASKABLE,
                    false
            );

    public static final ConfigOption<Boolean> TCP_NO_DELAY =
            new ConfigOption<Boolean>(
                    DATASTAX_NS,
                    "tcp-no-delay",
                    "Disables Nagle's algorithm on the underlying socket",
                    ConfigOption.Type.MASKABLE,
                    false
            );

    public static final ConfigOption<Integer> SO_LINGER =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "so-linger",
                    "When specified, disables the immediate return from a call to close() on a TCP socket",
                    ConfigOption.Type.MASKABLE,
                    Integer.class
            );

    public static final ConfigOption<Integer> RECEIVE_BUFFER_SIZE =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "receive-buffer-size",
                    "A hint on the size of the buffer used to send data",
                    ConfigOption.Type.MASKABLE,
                    Integer.class
            );

    public static final ConfigOption<Integer> SEND_BUFFER_SIZE =
            new ConfigOption<Integer>(
                    DATASTAX_NS,
                    "send-buffer-size",
                    "A hint on the size of the buffer used to receive data",
                    ConfigOption.Type.MASKABLE,
                    Integer.class
            );

    //policies config
    public static final ConfigOption<String> RETRY_POLICY =
            new ConfigOption<String>(
                    DATASTAX_NS,
                    "retry-policy",
                    "Datastax's retry policy implementation",
                    ConfigOption.Type.MASKABLE,
                    "DefaultRetryPolicy");

    public static final ConfigOption<String> RECONNECTION_POLICY =
            new ConfigOption<String>(
                    DATASTAX_NS,
                    "reconnection-policy",
                    "Datastax's reconnection policy implementation with configuration parameters",
                    ConfigOption.Type.MASKABLE,
                    "com.datastax.driver.core.policies.ExponentialReconnectionPolicy,1000,5000");

    public static final ConfigOption<String> LOAD_BALANCING_POLICY =
            new ConfigOption<String>(
                    DATASTAX_NS,
                    "load-balancing-policy",
                    "Datastax's reconnection policy implementation with configuration parameters",
                    ConfigOption.Type.MASKABLE,
                    "com.datastax.driver.core.policies.RoundRobinPolicy");

    private final Cluster cluster;
    private final Session session;
    private final Map<String, DatastaxKeyColumnValueStore> openStores;

    public DatastaxStoreManager(Configuration config) throws BackendException {
        super(config);

        RetryPolicy retryPolicy = getRetryPolicy(config.get(RETRY_POLICY));
        ReconnectionPolicy reconnectionPolicy = getReconnectionPolicy(config.get(RECONNECTION_POLICY));
        LoadBalancingPolicy loadBalancingPolicy = getLoadBalancingPolicy(config.get(LOAD_BALANCING_POLICY));

        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(StringUtils.join(hostnames, ","));
        builder.withPort(port);//TODO must set value
        builder.withClusterName(config.get(CLUSTER_NAME));

        //protocol options
        if (config.has(PROTOCOL_VERSION)) {
            builder.withProtocolVersion(config.get(PROTOCOL_VERSION));
        }

        if (config.has(PROTOCOL_COMPRESSION)) {
            builder.withCompression(config.get(PROTOCOL_COMPRESSION));
        }

        if (hasAuthentication()) {
            builder.withAuthProvider(new PlainTextAuthProvider(username, password));
        }

        if (config.get(SSL_ENABLED)) {
            try {
                builder.withSSL(
                        new SSLOptions(
                                SSLContext.getInstance(
                                        config.get(SSL_TRUSTSTORE_LOCATION),
                                        config.get(SSL_TRUSTSTORE_PASSWORD)),
                                SSLOptions.DEFAULT_SSL_CIPHER_SUITES));
            } catch (NoSuchAlgorithmException e) {
                throw new TemporaryBackendException(e);
            } catch (NoSuchProviderException e) {
                throw new TemporaryBackendException(e);
            }
        }

        //pooling options
        PoolingOptions poolingOptions = new PoolingOptions();
        HostDistance distance = config.get(HOST_DISTANCE);
        if (distance != HostDistance.IGNORED) {
            int coreConnections = config.has(CORE_CONNECTIONS) ?
                    config.get(CORE_CONNECTIONS) :
                    (distance == HostDistance.LOCAL ? 2 : 1);
            poolingOptions.setCoreConnectionsPerHost(distance, coreConnections);

            int maxConnections = config.has(MAX_CONNECTIONS) ?
                    config.get(MAX_CONNECTIONS) :
                    (distance == HostDistance.LOCAL ? 8 : 2);
            poolingOptions.setMaxConnectionsPerHost(distance, maxConnections);

            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(distance, config.get(MAX_REQUESTS));
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(distance, config.get(MIN_REQUESTS));
        }
        builder.withPoolingOptions(poolingOptions);

        //socket options
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setKeepAlive(config.get(KEEP_ALIVE));
        socketOptions.setReuseAddress(config.get(REUSE_ADDRESS));
        socketOptions.setTcpNoDelay(config.get(TCP_NO_DELAY));
        socketOptions.setConnectTimeoutMillis(config.get(CONNECT_TIMEOUT));
        socketOptions.setReadTimeoutMillis(config.get(READ_TIMEOUT));
        if (config.has(SO_LINGER)) {
            socketOptions.setSoLinger(config.get(SO_LINGER));
        }
        if (config.has(RECEIVE_BUFFER_SIZE)) {
            socketOptions.setSoLinger(config.get(RECEIVE_BUFFER_SIZE));
        }
        if (config.has(SEND_BUFFER_SIZE)) {
            socketOptions.setSoLinger(config.get(SEND_BUFFER_SIZE));
        }

        //metrics options
        if (!config.get(BASIC_METRICS)) {//disable metrics
            builder.withoutMetrics();
        } else if (!config.get(METRICS_JMX_ENABLED)) {//disable Jmx report
            builder.withoutJMXReporting();
        }

        //query options
        builder.withQueryOptions(new QueryOptions().setFetchSize(pageSize));

        //policy options
        builder.withRetryPolicy(retryPolicy);
        builder.withReconnectionPolicy(reconnectionPolicy);
        builder.withLoadBalancingPolicy(loadBalancingPolicy);

        cluster = builder.build();
        ensureKeyspaceExists();

        session = cluster.connect(keySpaceName);

        openStores = new ConcurrentHashMap<String, DatastaxKeyColumnValueStore>(8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public IPartitioner<? extends Token<?>> getCassandraPartitioner() throws BackendException {
        Session session = cluster.connect();
        Row row = session.execute(SELECT_LOCAL).one();
        String partitioner = row.getString("partitioner");
        //use default
        if (partitioner == null) {
            partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
        }
        try {
            return FBUtilities.newPartitioner(partitioner);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws BackendException {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keySpaceName);
        if (keyspaceMetadata == null) {
            throw new PermanentBackendException("Keyspace " + keySpaceName + " is undefined");
        }
        TableMetadata tableMetadata = keyspaceMetadata.getTable(cf);
        if (tableMetadata == null) {
            throw new PermanentBackendException("Column family " + cf + " is undefined");
        }
        return tableMetadata.getOptions().getCompression();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        DatastaxKeyColumnValueStore store = openStores.get(name);
        if (store == null) {
            ensureColumnFamilyExists(name);
            store = new DatastaxKeyColumnValueStore(name, session, this);
            openStores.put(name, store);
        }
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        ConsistencyLevel consistencyLevel = getTx(txh).getWriteConsistencyLevel().getDatastax();

        BatchStatement batchStatement = new BatchStatement();//max is 65536
        batchStatement.setSerialConsistencyLevel(consistencyLevel);

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchEntry : mutations.entrySet()) {
            String table = batchEntry.getKey();

            for (Map.Entry<StaticBuffer, KCVMutation> entry : batchEntry.getValue().entrySet()) {
                KCVMutation mutation = entry.getValue();
                ByteBuffer key = entry.getKey().asByteBuffer();

                if (mutation.hasDeletions()) {

                    for (StaticBuffer buffer : mutation.getDeletions()) {
                        batchStatement.add(
                                delete()
                                        .from(table)
                                        .using(timestamp(commitTime.getDeletionTime(times.getUnit())))
                                        .where(eq("key", key)).and(eq("column1", buffer.asByteBuffer())));
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry e : mutation.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        if (null != ttl && ttl > 0) {
                            batchStatement.add(
                                    insertInto(table)
                                            .using(timestamp(commitTime.getAdditionTime(times.getUnit()))).and(ttl(ttl))
                                            .values(
                                                    new String[]{"key", "column1", "value"},
                                                    new ByteBuffer[]{key, e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY)}));
                        } else {
                            batchStatement.add(
                                    insertInto(table)
                                            .using(timestamp(commitTime.getAdditionTime(times.getUnit())))
                                            .values(
                                                    new String[]{"key", "column1", "value"},
                                                    new ByteBuffer[]{key, e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY)}));
                        }
                    }
                }
            }
        }
        try {
            session.execute(batchStatement);
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public void close() throws BackendException {
        try {
            cluster.close();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        Session session = cluster.connect();
        try {
            session.execute("drop keyspace " + keySpaceName);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            session.close();
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    private void ensureKeyspaceExists() throws BackendException {
        StringBuilder sob = new StringBuilder(40);
        for (Map.Entry<String, String> entry : strategyOptions.entrySet()) {
            sob.append(String.format(",'%s':'%s'", entry.getKey(), entry.getValue()));
        }

        Session session = cluster.connect();
        try {
            log.debug("Checking and create keyspace {}", keySpaceName);
            session.execute(String.format(CREATE_KEYSPACE,
                    keySpaceName,
                    storageConfig.get(REPLICATION_STRATEGY),
                    sob.toString()));
        } catch (Exception e) {
            log.debug("Failed to check or create keyspace {}", keySpaceName);
            throw new TemporaryBackendException(e);
        } finally {
            session.close();
        }
    }

    private void ensureColumnFamilyExists(String name) throws BackendException {
        StringBuilder cob = new StringBuilder();
        if (compressionEnabled) {
            cob.append(String.format(" AND compression = {'sstable_compression':'%s', 'chunk_length_kb':'%s'}", compressionClass, compressionChunkSizeKB));
        }
        if (storageConfig.has(CF_COMPACTION_STRATEGY, name)) {
            cob.append(String.format(" And compaction = {'class','%s'", storageConfig.get(CF_COMPACTION_STRATEGY, name)));
            if (storageConfig.has(CF_COMPACTION_OPTIONS, name)) {
                List<String> options = storageConfig.get(CF_COMPACTION_OPTIONS, name);
                if (options.size() % 2 != 0)
                    throw new IllegalArgumentException(CF_COMPACTION_OPTIONS.getName() + "." + name + " should have even number of elements.");
                for (int i = 0; i < options.size(); i += 2) {
                    cob.append(String.format(",'%s':%s", options.get(i), options.get(i + 1)));
                }
            }
            cob.append("}");
        }

        try {
            log.debug("Checking and adding column family {} to keyspace {}", name, keySpaceName);
            session.execute(String.format(CREATE_TABLE, name, cob.toString()));
        } catch (Exception e) {
            log.debug("Failed to create column family {} to keyspace {}", name, keySpaceName);
            throw new TemporaryBackendException(e);
        }
    }

    enum DatastaxRetryPolicy {

        DefaultRetryPolicy {
            @Override
            public RetryPolicy getRetryPolicy(DatastaxRetryPolicy policy) {
                return com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;
            }
        },
        DowngradingConsistencyRetryPolicy {
            @Override
            public RetryPolicy getRetryPolicy(DatastaxRetryPolicy policy) {
                return com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy.INSTANCE;
            }
        },
        FallthroughRetryPolicy {
            @Override
            public RetryPolicy getRetryPolicy(DatastaxRetryPolicy policy) {
                return com.datastax.driver.core.policies.FallthroughRetryPolicy.INSTANCE;
            }
        },
        LoggingRetryPolicy {
            @Override
            public RetryPolicy getRetryPolicy(DatastaxRetryPolicy policy) {
                return new LoggingRetryPolicy(policy.getRetryPolicy(null));
            }
        };

        public abstract RetryPolicy getRetryPolicy(DatastaxRetryPolicy policy);

    }

    private RetryPolicy getRetryPolicy(String policyString) {
        if (policyString.indexOf(",") > 0) {
            String[] polices = policyString.split(",");
            if (!polices[0].equals("LoggingRetryPolicy") || polices[1].equals("LoggingRetryPolicy")) {
                throw new IllegalArgumentException("config " + RETRY_POLICY.getName() + " is wrong when it set to LoggingRetryPolicy");
            }
            return DatastaxRetryPolicy.valueOf(polices[0]).getRetryPolicy(DatastaxRetryPolicy.valueOf(polices[1]));
        }
        return DatastaxRetryPolicy.valueOf(policyString).getRetryPolicy(null);
    }

    //TODO the load Balancing Policy is very difficult to build
    private LoadBalancingPolicy getLoadBalancingPolicy(String policyString) {
        return null;
    }

    private ReconnectionPolicy getReconnectionPolicy(String policyString) throws BackendException {
        String[] tokens = policyString.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Integer[] args = new Integer[argCount];
        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            ReconnectionPolicy rp = instantiate(policyClassName, args, policyString);
            log.debug("Instantiated ReconnectionPolicy object {} from config string \"{}\"", rp, policyString);
            return rp;
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to instantiate Datastax Reconnection Policy class", e);
        }
    }

    //init policy
    @SuppressWarnings("unchecked")
    private static <V> V instantiate(String policyClassName, Integer[] args, String raw) throws Exception {
        for (Constructor<?> con : Class.forName(policyClassName).getConstructors()) {
            Class<?>[] parameterTypes = con.getParameterTypes();

            // match constructor by number of arguments first
            if (args.length != parameterTypes.length)
                continue;

            // check if the constructor parameter types are compatible with argument types (which are integer)
            // note that we allow long.class arguments too because integer is cast to long by runtime.
            boolean intsOrLongs = true;
            for (Class<?> pc : parameterTypes) {
                if (!pc.equals(int.class) && !pc.equals(long.class)) {
                    intsOrLongs = false;
                    break;
                }
            }

            // we found a constructor with required number of parameters but times didn't match, let's carry on
            if (!intsOrLongs)
                continue;

            if (log.isDebugEnabled())
                log.debug("About to instantiate class {} with {} arguments", con.toString(), args.length);

            return (V) con.newInstance(args);
        }

        throw new Exception("Failed to identify a class matching the config string \"" + raw + "\"");
    }

}


