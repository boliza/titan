package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

@PreInitializeConfigOptions
public class DatastaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(DatastaxStoreManager.class);

    private static final String SELECT_LOCAL = "SELECT partitioner FROM system.local WHERE key='local'";
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class':'%s'%s}";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (key blob,column1 blob,value blob,PRIMARY KEY (key,column1)) WITH comment = 'column family %s' ";

    //Config for datastax
    public static final ConfigNamespace DATASTAX_NS = new ConfigNamespace(CASSANDRA_NS, "datastax", "datastax-specific Cassandra options");

    private final Cluster cluster;
    private final Map<String, DatastaxKeyColumnValueStore> openStores;

    public DatastaxStoreManager(Configuration config) throws BackendException {
        super(config);
        cluster = Cluster.builder().addContactPoints(StringUtils.join(hostnames, ",")).build();
        ensureKeyspaceExists();
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
            store = new DatastaxKeyColumnValueStore(name, keySpaceName, cluster, this);
            openStores.put(name, store);
        }
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {

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

        Session session = cluster.connect(keySpaceName);
        try {
            log.debug("Checking and adding column family {} to keyspace {}", name, keySpaceName);
            session.execute(String.format(CREATE_TABLE, name, cob.toString()));
        } catch (Exception e) {
            log.debug("Failed to create column family {} to keyspace {}", name, keySpaceName);
            throw new TemporaryBackendException(e);
        } finally {
            session.close();
        }
    }

}


