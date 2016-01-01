package org.neo4j.kernel.impl.locking.community;

public class NRLockResouceId implements LockResouceId
{
    private long id;
    public NRLockResouceId( long id )
    {
        this.id = id;
    }
    
    public int hashCode()
    {
        return (int)(id ^ (id >>> 32));
    }
    
    @Override
    public boolean equals( LockResouceId o )
    {
        if( o == this )
            return true;
        if( o == null || o.getClass() != this.getClass() )
            return false;
        NRLockResouceId that = (NRLockResouceId)o;
        return that.id == this.id;
    }
}
