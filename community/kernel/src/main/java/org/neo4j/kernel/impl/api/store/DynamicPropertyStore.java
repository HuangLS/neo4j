package org.neo4j.kernel.impl.api.store;

import java.io.File;

import org.act.dynproperty.impl.Levels;
import org.act.dynproperty.impl.RangeQueryCallBack;
import org.act.dynproperty.util.Slice;
import org.neo4j.kernel.api.properties.DynamicProperty;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DynamicPropertyStore extends LifecycleAdapter
{
    
    private File dbDir;
    private String nodeDir;
    private String relDir;
    private Config configes;
    private Levels nodeStore;
    private Levels relStore;
    
    private static DynamicPropertyStore instence;
    
    public static synchronized DynamicPropertyStore instence( Config configs, File dbDir )
    {
        if( null == instence )
            instence = new DynamicPropertyStore( configs, dbDir );
        return instence;
    }
    
    private DynamicPropertyStore( Config configs , File dbDir )
    {
        this.dbDir = dbDir;
        this.configes = configs;
    }
    
    @Override
    public void start()
    {
        checkExitorCreatDir();
        this.nodeStore = new Levels( nodeDir );
        this.relStore = new Levels( relDir );
    }
    
    private void checkExitorCreatDir()
    {
        File nodeDir = new File( dbDir.getAbsolutePath() + "/dynNode" );
        if( !nodeDir.exists() )
            nodeDir.mkdirs();
        this.nodeDir = nodeDir.getAbsolutePath();
        File relDir = new File( dbDir.getAbsolutePath() + "/dynRelationship" );
        if( !relDir.exists() )
            relDir.mkdirs();
        this.relDir = relDir.getAbsolutePath();
    }

    public void NodeAdd( long id, int proId, int time, byte[] value )
    {
        Slice valueSlice = new Slice( value );
        this.nodeStore.add( id, proId, time, valueSlice );
    }

    public void RelationshipAdd( long id, int proId, int time, byte[] value )
    {
        Slice valueSlice = new Slice( value );
        this.relStore.add( id, proId, time, valueSlice );
    }

    public byte[] getNodeProperty( long nodeId, int propertyKeyId, int time )
    {
        Slice valueSlice = this.nodeStore.getPointValue( nodeId, propertyKeyId, time );
        return valueSlice.copyBytes();
    }

    public byte[] getRelationshipProperty( long relId, int propertyKeyId, int time )
    {
        Slice valueSlice = this.relStore.getPointValue( relId, propertyKeyId, time );
        return valueSlice.copyBytes();
    }

    public byte[] getNodeProperty( long nodeId, int proId, int startTime, int endTime, RangeQueryCallBack callback )
    {
        Slice valueSlice = this.nodeStore.getRangeValue( nodeId, proId, startTime, endTime, callback );
        return valueSlice.copyBytes();
    }

    public byte[] getReltaionshipProperty( long nodeId, int proId, int startTime, int endTime,
            RangeQueryCallBack callback )
    {
        Slice valueSlice = this.relStore.getRangeValue( nodeId, proId, startTime, endTime, callback );
        return valueSlice.copyBytes();
    }

    public int getNodePropertyLatestTime( long nodeId, int propertyKeyId )
    {
        return this.nodeStore.getPropertyLatestTime( nodeId, propertyKeyId );
    }

    public int getRelationshipPropertyLatestTime( long relId, int propertyKeyId )
    {
        return this.relStore.getPropertyLatestTime( relId, propertyKeyId );
    }
}
