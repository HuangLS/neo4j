/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking.community;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.Locks.ResourceType;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static java.lang.String.format;

public class CommunityLockClient implements Locks.Client
{
    
    @Override
    public void acquireDynPropertyExclusive( ResourceType nodeProperaty, long Id, int propertyKeyId, int time )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        manager.getWriteLock( resource, lockTransaction, time );
        localLocks.put(id, resource);
    }
    
    @Override
    public void acquireDynPropertyShared( ResourceType nodeProperaty, long Id, int propertyKeyId, int start,
            int end ) throws AcquireLockTimeoutException
    {
        Map<LockResouceId,LockResource> localLocks = localShared( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        manager.getReadLock( resource, lockTransaction, start, end );
        localLocks.put(id, resource);
    }

    @Override
    public boolean tryDynPropertyExclusive( ResourceType nodeProperaty, long Id, int propertyKeyId, int time )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        if(manager.trygetWriteLock( resource, lockTransaction, time ))
        {
            localLocks.put(id, resource);
            return true;
        }
        return false;
    }

    @Override
    public boolean tryDynPropertyShared( ResourceType nodeProperaty, long Id, int propertyKeyId, int start, int end )
    {
        Map<LockResouceId,LockResource> localLocks = localShared( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        if(manager.trygetReadLock( resource, lockTransaction, start, end ))
        {
            localLocks.put(id, resource);
            return true;
        }
        return false;
    }

    @Override
    public void releaseDynPropertyExclusive( ResourceType nodeProperaty, long Id, int propertyKeyId, int time )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        manager.releaseWriteLock( resource, lockTransaction, time );
        localLocks.put(id, resource);
    }

    @Override
    public void releaseDynPropertyShared( ResourceType nodeProperaty, long Id, int propertyKeyId, int start,
            int end )
    {
        Map<LockResouceId,LockResource> localLocks = localShared( nodeProperaty );
        DynProResourceId id =  new DynProResourceId( Id, propertyKeyId );
        LockResource resource = localLocks.get( id );
        if( resource != null )
        {
            resource.acquireReference();
        }

        resource = new LockResource( nodeProperaty, id );
        manager.releaseReadLock( resource, lockTransaction, start, end );
        localLocks.put(id, resource);
    }
    
    private final LockManagerImpl manager;
    private final LockTransaction lockTransaction = new LockTransaction();

    private final PrimitiveIntObjectMap<Map<LockResouceId,LockResource>> sharedLocks = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<Map<LockResouceId,LockResource>> exclusiveLocks = Primitive.intObjectMap();

    public CommunityLockClient( LockManagerImpl manager )
    {
        this.manager = manager;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id = new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( id );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, id );
            manager.getReadLock( resource, lockTransaction );
            localLocks.put(id, resource);
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id =  new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( id );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, id );
            manager.getWriteLock( resource, lockTransaction );
            localLocks.put(id, resource);
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id = new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( id );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, id );
            if(manager.tryWriteLock( resource, lockTransaction ))
            {
                localLocks.put(id, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id = new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( id );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, id );
            if(manager.tryReadLock( resource, lockTransaction ))
            {
                localLocks.put(id, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id = new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( id );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseReadLock( new LockResource( resourceType, id ), lockTransaction );
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        Map<LockResouceId,LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            NRLockResouceId id = new NRLockResouceId( resourceId );
            LockResource resource = localLocks.get( resourceId );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseWriteLock( new LockResource( resourceType, id ), lockTransaction );
        }
    }

    @Override
    public void releaseAllShared()
    {
        sharedLocks.visitEntries( typeReadReleaser );
        sharedLocks.clear();
    }

    @Override
    public void releaseAllExclusive()
    {
        exclusiveLocks.visitEntries( typeWriteReleaser );
        exclusiveLocks.clear();
    }

    @Override
    public void releaseAll()
    {
        releaseAllExclusive();
        releaseAllShared();
    }

    @Override
    public void close()
    {
        releaseAll();
    }

    @Override
    public int getLockSessionId()
    {
        return lockTransaction.getId();
    }

    private Map<LockResouceId,LockResource> localShared( Locks.ResourceType resourceType )
    {
        Map<LockResouceId,LockResource> map = sharedLocks.get( resourceType.typeId() );
        if(map == null)
        {
            map = new HashMap<LockResouceId,LockResource>();
            sharedLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    private Map<LockResouceId,LockResource> localExclusive( Locks.ResourceType resourceType )
    {
        Map<LockResouceId,LockResource> map = exclusiveLocks.get( resourceType.typeId() );
        if(map == null)
        {
            map = new HashMap<LockResouceId,LockResource>();
            exclusiveLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    private final PrimitiveIntObjectVisitor<Map<LockResouceId,LockResource>, RuntimeException> typeReadReleaser = new
            PrimitiveIntObjectVisitor<Map<LockResouceId,LockResource>, RuntimeException>()
    {
        @Override
        public boolean visited( int key, Map<LockResouceId,LockResource> value ) throws RuntimeException
        {
            for(Entry<LockResouceId,LockResource> entry : value.entrySet() )
            {
                manager.releaseReadLock( entry.getValue(), lockTransaction );
            }
            return false;
        }
    };

    private final PrimitiveIntObjectVisitor<Map<LockResouceId,LockResource>, RuntimeException> typeWriteReleaser = new
            PrimitiveIntObjectVisitor<Map<LockResouceId,LockResource>, RuntimeException>()
    {
        @Override
        public boolean visited( int key, Map<LockResouceId,LockResource> value ) throws RuntimeException
        {
            for(Entry<LockResouceId,LockResource> entry : value.entrySet() )
            {
                manager.releaseWriteLock( entry.getValue(), lockTransaction );
            }
            return false;
        }
    };

    private final PrimitiveLongObjectVisitor<LockResource, RuntimeException> writeReleaser = new PrimitiveLongObjectVisitor<LockResource, RuntimeException>()
    {
        @Override
        public boolean visited( long key, LockResource lockResource ) throws RuntimeException
        {
            manager.releaseWriteLock( lockResource, lockTransaction );
            return false;
        }
    };

    private final PrimitiveLongObjectVisitor<LockResource, RuntimeException> readReleaser = new PrimitiveLongObjectVisitor<LockResource, RuntimeException>()
    {
        @Override
        public boolean visited( long key, LockResource lockResource ) throws RuntimeException
        {
            manager.releaseReadLock( lockResource, lockTransaction );
            return false;
        }
    };

    @Override
    public String toString()
    {
        return format( "%s[%d]", getClass().getSimpleName(), getLockSessionId() );
    }

}
