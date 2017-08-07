package org.neo4j.kernel.impl.store;


import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.TemporalPropertyStoreFactory;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.util.Slice;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.io.File;
import java.io.IOException;

/**
 * Created by song on 17-7-7.
 */
public class TemporalPropertyStoreAdapter extends LifecycleAdapter
{
    private File dbDir;
    private String nodeDir;
    private String relDir;
    private Config config;
    private TemporalPropertyStore nodeStore;
    private TemporalPropertyStore relStore;

    private static TemporalPropertyStoreAdapter instance;

    public static synchronized TemporalPropertyStoreAdapter getInstance(Config configs, File dbDir )
    {
        if( null == instance)
            instance = new TemporalPropertyStoreAdapter( configs, dbDir );
        return instance;
    }

    private TemporalPropertyStoreAdapter(Config configs , File dbDir )
    {
        this.dbDir = dbDir;
        this.config = configs;
    }

    @Override
    public void init() throws Throwable {
        ResolveStoreDirs();
        this.nodeStore = TemporalPropertyStoreFactory.newPropertyStore( nodeDir );
        this.relStore = TemporalPropertyStoreFactory.newPropertyStore( relDir );
    }

    @Override
    public void shutdown()
    {
        this.nodeStore.shutDown();
        this.relStore.shutDown();
    }

    private void ResolveStoreDirs() throws IOException
    {
        File nodeDir = new File( dbDir.getAbsolutePath(),  "temporal.node.properties" );
        if( nodeDir.exists() ) {
            if(!nodeDir.isDirectory()){
                throw new IOException("temporal.node.properties is not a directory.");
            }
        }else{
            if (!nodeDir.mkdirs()){
                throw new IOException("create temporal.node.properties dir failed.");
            }
        }

        File relDir = new File( dbDir.getAbsolutePath(), "temporal.relationship.properties" );
        if( relDir.exists() ) {
            if (!relDir.isDirectory()) {
                throw new IOException("temporal.node.properties is not a directory.");
            }
        }else{
            if (!relDir.mkdirs()) {
                throw new IOException("create temporal.relationship.properties dir failed.");
            }
        }
        
        this.nodeDir = nodeDir.getAbsolutePath();
        this.relDir = relDir.getAbsolutePath();
    }

    public void relationshipDelete( Slice id )
    {
        this.relStore.delete(id);
    }

    public void nodeDelete( Slice id )
    {
        this.nodeStore.delete(id);
    }

    public void nodeSet( Slice id, byte[] value )
    {
        this.nodeStore.setProperty(id, value);
    }

    public void relationshipSet( Slice id, byte[] value )
    {
        this.relStore.setProperty( id, value );
    }

    public Slice getNodeProperty( long nodeId, int propertyKeyId, int time )
    {
        return this.nodeStore.getPointValue(nodeId, propertyKeyId, time);
    }

    public Slice getRelationshipProperty( long relId, int propertyKeyId, int time )
    {
        return this.relStore.getPointValue( relId, propertyKeyId, time );
    }

    public Object getRelationshipProperty( long relId, int propertyKeyId, int startTime, int endTime,
                                           RangeQueryCallBack callback )
    {
        return this.relStore.getRangeValue(relId, propertyKeyId, startTime, endTime, callback);
    }

    public void flushAll()
    {
        if( this.relStore != null )
        {
            this.relStore.flushMemTable2Disk();
            this.relStore.flushMetaInfo2Disk();
        }
        if( this.nodeStore != null )
        {
            this.nodeStore.flushMemTable2Disk();
            this.nodeStore.flushMetaInfo2Disk();
        }

    }
}
