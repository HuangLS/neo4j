package org.neo4j.kernel.impl.api.store;

import org.act.dynproperty.impl.RangeQueryCallBack;

public class DynamicPropertyRead
{
    
    private DynamicPropertyStore store;
    public DynamicPropertyRead( DynamicPropertyStore store )
    {
        this.store = store;
    }
    
    public byte[] getNodeProperty( long nodeId, int propertyKeyId, int time )
    {
        return this.store.getNodeProperty( nodeId, propertyKeyId, time );
    }

    public byte[] getRelationshipProperty( long relId, int propertyKeyId, int time )
    {
        return this.store.getRelationshipProperty( relId, propertyKeyId, time );
    }
    
    public byte[] getNodeProperty( long nodeId, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        return this.store.getNodeProperty( nodeId, proId, startTime, endTime, callback );
    }
    
    public byte[] getRelationshipProperty( long nodeId, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        return this.store.getReltaionshipProperty( nodeId, proId, startTime, endTime, callback );
    }

    public int getNodePropertyLatestTime( long nodeId, int propertyKeyId )
    {
        return this.store.getNodePropertyLatestTime(  nodeId,  propertyKeyId );
    }

    public int getRelationshipPropertyLatestTime( long relId, int propertyKeyId )
    {
        return this.store.getRelationshipPropertyLatestTime( relId, propertyKeyId );
    }
}
