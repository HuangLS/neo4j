package org.neo4j.temporal;

import org.act.temporalProperty.query.TimePointL;

/**
 * Created by song on 2018-05-12.
 */
public class IntervalEntry
{
    private final TimePointL start;
    private final TimePointL end;
    private final long entityId;
    private final Object[] val;

    public IntervalEntry(TimePointL start, TimePointL end, long entityId, Object[] val )
    {
        this.start = start;
        this.end = end;
        this.entityId = entityId;
        this.val = val;
    }

    public TimePointL getStart()
    {
        return start;
    }

    public TimePointL getEnd()
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
