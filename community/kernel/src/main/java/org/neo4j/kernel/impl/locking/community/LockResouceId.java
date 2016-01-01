package org.neo4j.kernel.impl.locking.community;

public interface LockResouceId
{
    public boolean equals( LockResouceId other );
    public int hashCode();
}
