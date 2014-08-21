package com.thinkaurelius.titan.diskstorage.cassandra.datastax;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.Partitioner;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;

public class DatastaxKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(DatastaxKeyColumnValueStore.class);


    private final String table;
    private final DatastaxStoreManager storeManager;
    private final Session session;
    private final DatastaxGetter getter;
    private final PreparedStatement keyRangeStatement;

    DatastaxKeyColumnValueStore(String table,
                                Session session,
                                DatastaxStoreManager storeManager) {
        this.table = table;
        this.storeManager = storeManager;
        this.session = session;
        this.getter = new DatastaxGetter(storeManager.getMetaDataSchema(table));
        this.keyRangeStatement = session.prepare("select * from " + table + " where token(key) >= token(?) and token(key) <= token(?) limit ?");
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
        Select select = select().from(table);
        select.where(in("key", CassandraHelper.convert(keys).toArray()))
                .and(gte("column1", query.getSliceStart().asByteBuffer()))
                .and(lte("column1", query.getSliceEnd().asByteBuffer()));
        if (query.hasLimit()) {
            select.limit(query.getLimit());
        }
        select.setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getDatastax());

        List<Row> rows = session.execute(select).all();
        Map<ByteBuffer, List<Row>> rowMap = rowToMap(rows, keys.size());
        Map<StaticBuffer, EntryList> result = new HashMap<StaticBuffer, EntryList>(keys.size());
        for (Map.Entry<ByteBuffer, List<Row>> entry : rowMap.entrySet()) {
            result.put(StaticArrayBuffer.of(entry.getKey()), CassandraHelper.makeEntryList(entry.getValue(), getter, query.getSliceEnd(), query.getLimit()));
        }
        return result;
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
        if (storeManager.getPartitioner() != Partitioner.BYTEORDER) {
            throw new PermanentBackendException("getKeys(KeyRangeQuery could only be used with byte-ordering partitioner.");
        }
        ResultSet resultSet = session.execute(keyRangeStatement.bind(
                query.getKeyStart().asByteBuffer(),
                query.getKeyEnd().asByteBuffer(),
                query.getLimit())
                .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getDatastax()));
        Map<ByteBuffer, List<Row>> rowMap = rowToMap(resultSet.all(), 1 << 4);
        return new DatastaxIterator(rowMap, query);
    }

    @Override
    public String getName() {
        return table;
    }

    private Map<ByteBuffer, List<Row>> rowToMap(List<Row> rows, int size) {
        Map<ByteBuffer, List<Row>> rowMap = new HashMap<ByteBuffer, List<Row>>(size);
        for (Row row : rows) {
            ByteBuffer key = row.getBytes("key");
            if (rowMap.containsKey(row.getBytes("key"))) {
                rowMap.get(key).add(row);
            } else {
                List<Row> list = Lists.newArrayList();
                list.add(row);
                rowMap.put(key, list);
            }
        }
        return rowMap;
    }

    private static class DatastaxIterator implements KeyIterator {

        

        public DatastaxIterator(Map<ByteBuffer, List<Row>> rowMap, SliceQuery query) {

        }

        @Override
        public RecordIterator<Entry> getEntries() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public StaticBuffer next() {
            return null;
        }
    }

    private static class DatastaxGetter implements StaticArrayEntry.GetColVal<Row, ByteBuffer> {

        private final EntryMetaData[] schema;

        private DatastaxGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public ByteBuffer getColumn(Row element) {
            return element.getBytes("column1");
        }

        @Override
        public ByteBuffer getValue(Row element) {
            return element.getBytes("value");
        }

        @Override
        public EntryMetaData[] getMetaSchema(Row element) {
            return schema;
        }

        @Override
        public Object getMetaData(Row element, EntryMetaData meta) {
            return null;
        }
    }


}
