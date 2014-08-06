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

    //Config for datastax
    public static final ConfigNamespace DATASTAX_NS =
            new ConfigNamespace(CASSANDRA_NS, "datastax", "datastax-specific Cassandra options");

    private final Cluster cluster;
    private final Map<String, DatastaxKeyColumnValueStore> openStores;

    public DatastaxStoreManager(Configuration config) {
        super(config);
        cluster = Cluster.builder().addContactPoints(StringUtils.join(hostnames, ",")).build();
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
        return null;
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
}


