package org.neo4j.temporal;

/**
 * Created by song on 2018-05-12.
 */
public class IntervalEntry
{
    private final int start;
    private final int end;
    private final long entityId;
    private final Object[] val;

    public IntervalEntry( int start, int end, long entityId, Object[] val )
    {
        this.start = start;
        this.end = end;
        this.entityId = entityId;
        this.val = val;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }

    public long getEntityId()
    {
        return entityId;
    }

    public Object getVal(int i)
    {
        return val[i];
    }
}
