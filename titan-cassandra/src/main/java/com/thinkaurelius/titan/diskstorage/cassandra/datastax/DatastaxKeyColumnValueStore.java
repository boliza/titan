package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class DatastaxKeyColumnValueStore implements KeyColumnValueStore {

    private final String table;
    private final DatastaxStoreManager storeManager;
    private final Session session;

    DatastaxKeyColumnValueStore(String table,
                                String keySpaceName,
                                Cluster cluster,
                                DatastaxStoreManager storeManager) {
        this.table = table;
        this.storeManager = storeManager;
        this.session = cluster.connect(keySpaceName);
    }

    @Override
    public void close() throws BackendException {
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys,
                                                 SliceQuery query,
                                                 StoreTransaction txh) throws BackendException {
        return getNamesSlice(keys, query, txh);
    }

    public Map<StaticBuffer, EntryList> getNamesSlice(StaticBuffer key,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws BackendException {
        return getNamesSlice(ImmutableList.of(key), query, txh);
    }


    public Map<StaticBuffer, EntryList> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        mutateMany(ImmutableMap.of(key, new KCVMutation(additions, deletions)), txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(table, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(@Nullable SliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public String getName() {
        return table;
    }

}
