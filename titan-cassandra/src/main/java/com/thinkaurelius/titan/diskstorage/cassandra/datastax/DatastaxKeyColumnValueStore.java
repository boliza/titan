package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.datastax.driver.core.Cluster;
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

    private final Cluster cluster;
    private final String keySpaceName;
    private final String columnFamilyName;
    private final DatastaxStoreManager storeManager;

    DatastaxKeyColumnValueStore(String columnFamilyName,
                                String keySpaceName,
                                Cluster cluster,
                                DatastaxStoreManager storeManager) {
        this.cluster = cluster;
        this.keySpaceName = keySpaceName;
        this.columnFamilyName = columnFamilyName;
        this.storeManager = storeManager;

    }

    @Override
    public void close() throws BackendException {
        //Do nothing
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(keys, query, txh);
    }

    public Map<StaticBuffer, EntryList> getNamesSlice(StaticBuffer key,
                                                      SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(ImmutableList.of(key), query, txh);
    }


    public Map<StaticBuffer, EntryList> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        mutateMany(ImmutableMap.of(key, new KCVMutation(additions, deletions)), txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(columnFamilyName, mutations), txh);
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
        // this query could only be done when byte-ordering partitioner is used
        // because Cassandra operates on tokens internally which means that even contiguous
        // range of keys (e.g. time slice) with random partitioner could produce disjoint set of tokens
        // returning ambiguous results to the user.
        return null;
    }

    @Override
    public String getName() {
        return columnFamilyName;
    }

}
