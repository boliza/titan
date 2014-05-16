package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Buffers mutations against multiple {@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore} from the same storage backend for increased
 * write performance. The buffer size (i.e. number of mutations after which to flush) is configurable.
 * <p/>
 * A BufferTransaction also attempts to flush multiple times in the event of temporary storage failures for increased
 * write robustness.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BufferTransaction implements StoreTransaction {


    private static final Logger log =
            LoggerFactory.getLogger(BufferTransaction.class);

    private final StoreTransaction tx;
    private final KeyColumnValueStoreManager manager;
    private final int bufferSize;
    private final int mutationAttempts;
    private final int attemptWaitTime;

    private int numMutations;
    private final Map<String, Map<StaticBuffer, KCVMutation>> mutations;

    /*当在提交没问题的时候，将此值修改为true，此时方可以进行真正的rollback。
    1:在出现事务锁的时候，说明没有去提交，无须真正回滚
    2:在提交database过程中导致的异常，不做处理，因为没有真正修改数据库
    3:当数据真正修改了，而其他的地方导致事务回滚了，这个时候为了一致性，就需要去rollback，目前仅支持insert操作
    */
    private boolean shouldReallyRollback = false;

    public BufferTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int bufferSize, int attempts, int waitTime) {
        this(tx, manager, bufferSize, attempts, waitTime, 8);
    }

    public BufferTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int bufferSize, int attempts, int waitTime, int expectedNumStores) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(manager);
        Preconditions.checkArgument(bufferSize > 1, "Buffering only makes sense when bufferSize>1");
        this.tx = tx;
        this.manager = manager;
        this.numMutations = 0;
        this.bufferSize = bufferSize;
        this.mutationAttempts = attempts;
        this.attemptWaitTime = waitTime;
        this.mutations = new HashMap<String, Map<StaticBuffer, KCVMutation>>(expectedNumStores);
    }

    public StoreTransaction getWrappedTransactionHandle() {
        return tx;
    }

    public void mutate(String store, StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        Preconditions.checkNotNull(store);
        if (additions.isEmpty() && deletions.isEmpty()) return;

        KCVMutation m = new KCVMutation(additions, deletions);
        Map<StaticBuffer, KCVMutation> storeMutation = mutations.get(store);
        if (storeMutation == null) {
            storeMutation = new HashMap<StaticBuffer, KCVMutation>();
            mutations.put(store, storeMutation);
        }
        KCVMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        numMutations += additions.size();
        numMutations += deletions.size();

        if (numMutations >= bufferSize) {
            flushInternal();
        }
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        tx.flush();
    }

    private void flushInternal() throws StorageException {
        if (numMutations > 0) {
            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    try {
                        manager.mutateMany(mutations, tx);
                        shouldReallyRollback = true;
                    } catch (StorageException e) {
                        throw e;
                    }
                    return true;
                }

                @Override
                public String toString() {
                    return "BufferMutation";
                }
            }, mutationAttempts, attemptWaitTime);
        }
    }

    public void clear() {
        clearInternal();
        tx.clear();
    }

    private void clearInternal() {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        if (shouldReallyRollback) {
            log.info("rollback the insert mutation");
            final Map<String, Map<StaticBuffer, KCVMutation>> rollbackMutations = Maps.newHashMap();
            //将insert修改成删除
            for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchEntry : mutations.entrySet()) {
                Map<StaticBuffer, KCVMutation> deleteKcvMutationMap = Maps.newHashMap();
                for (Map.Entry<StaticBuffer, KCVMutation> kCVMutationEntry : batchEntry.getValue().entrySet()) {
                    KCVMutation kcvMutation = kCVMutationEntry.getValue();
                    //只有增加，而没有删除，就代表是new insert
                    if (kcvMutation.hasAdditions() && !kcvMutation.hasDeletions()) {
                        List<StaticBuffer> deletions = Lists.newArrayList();
                        for (Entry entry : kcvMutation.getAdditions()) {
                            deletions.add(entry.getColumn());
                        }
                        KCVMutation deleteMutation = new KCVMutation(new ArrayList<Entry>(), deletions);
                        deleteKcvMutationMap.put(kCVMutationEntry.getKey(), deleteMutation);
                    }
                }
                if (!deleteKcvMutationMap.isEmpty()) {
                    rollbackMutations.put(batchEntry.getKey(), deleteKcvMutationMap);
                }
            }
            if (!rollbackMutations.isEmpty()) {
                BackendOperation.execute(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        tx.getConfiguration().setTimestamp();
                        manager.mutateMany(rollbackMutations, tx);
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "BufferMutation";
                    }
                }, mutationAttempts, attemptWaitTime);
            }
        }
        clearInternal();
        tx.rollback();
    }

    @Override
    public StoreTxConfig getConfiguration() {
        return tx.getConfiguration();
    }
}
