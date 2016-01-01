package org.neo4j.kernel.impl.locking.community;

public class DynProResourceId implements LockResouceId
{

    private long id;
    private int proId;
    
    public DynProResourceId( long id, int proId )
    {
        this.id = id;
        this.proId = proId;
    }
    
    public int hashCode()
    {
        int result = proId;
        result = result*31 + (int)(id^(id>>>32));
        return result;
    };
    
    @Override
    public boolean equals( LockResouceId other )
    {
        if( other == this )
            return true;
        if( null == other || other.getClass() == this.getClass() )
            return false;
        DynProResourceId that = (DynProResourceId)other;
        return that.proId == this.proId && that.id == this.id;
    }
}
