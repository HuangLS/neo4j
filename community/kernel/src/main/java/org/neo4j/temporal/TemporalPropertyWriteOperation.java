package org.neo4j.temporal;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.Slice;

/**
 * supported operations: set( create new temporal property, set value for one entity)
 * Created by song on 2018-04-21.
 */
public class TemporalPropertyWriteOperation
{
    public static int NOW = -1;
    private final int end;
    private final Object value;
    private final InternalKey startKey;
    private Slice valueSlice;

    public TemporalPropertyWriteOperation( long entityId, int proId, int start, int end, ValueType valueType, Object value )
    {
        this.startKey = new InternalKey( proId, entityId, start, valueType);
        this.end = end;
        this.value = value;
    }

    public long getEntityId()
    {
        return startKey.getEntityId();
    }

    public int getProId()
    {
        return startKey.getPropertyId();
    }

    public Object getValue()
    {
        return value;
    }

    public InternalKey getInternalKey()
    {
        return startKey;
    }

    public int getEnd(){
        return end;
    }

    public boolean isEndEqNow(){
        return end==NOW;
    }

    public void setValueSlice( Slice valueSlice )
    {
        this.valueSlice = valueSlice;
    }

    public Slice getValueSlice()
    {
        return valueSlice;
    }
}
