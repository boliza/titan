package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationTypeIndexWrapper implements RelationTypeIndex {

    public static final char RELATION_INDEX_SEPARATOR = ':';

    private final InternalRelationType type;

    public RelationTypeIndexWrapper(InternalRelationType type) {
        Preconditions.checkArgument(type!=null && type.getBaseType()!=null);
        this.type = type;
    }

    @Override
    public RelationType getType() {
        return type.getBaseType();
    }

    @Override
    public String getName() {
        String typeName = type.getName();
        int index = typeName.lastIndexOf(RELATION_INDEX_SEPARATOR);
        Preconditions.checkArgument(index>0 && index<typeName.length()-1,"Invalid name encountered: %s",typeName);
        return typeName.substring(index+1,typeName.length());
    }

    @Override
    public Order getSortOrder() {
        return type.getSortOrder();

    }

    @Override
    public RelationType[] getSortKey() {
        StandardTitanTx tx = type.tx();
        long[] ids = type.getSortKey();
        RelationType[] keys = new RelationType[ids.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i]=tx.getExistingRelationType(ids[i]);
        }
        return keys;
    }

    @Override
    public Direction getDirection() {
        if (type.isUnidirected(Direction.BOTH)) return Direction.BOTH;
        else if (type.isUnidirected(Direction.OUT)) return Direction.OUT;
        else if (type.isUnidirected(Direction.IN)) return Direction.IN;
        throw new AssertionError();
    }

}
