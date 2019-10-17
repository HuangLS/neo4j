package org.neo4j.kernel.impl.store;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.TemporalPropertyStoreFactory;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.Slice;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.temporal.TemporalPropertyReadOperation;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

/**
 * Created by song on 17-7-7.
 */
public class TemporalPropertyStoreAdapter extends LifecycleAdapter
{
    private File dbDir;
    private Config config;
    private TemporalPropertyStore nodeStore;
    private TemporalPropertyStore relStore;

    private static TemporalPropertyStoreAdapter instance;

    public static synchronized TemporalPropertyStoreAdapter getInstance( Config configs, File dbDir )
    {
        if ( null == instance )
        {
            instance = new TemporalPropertyStoreAdapter( configs, dbDir );
        }
        return instance;
    }

    public static synchronized TemporalPropertyStoreAdapter getInstance()
    {
        if ( null == instance )
        {
            throw new IllegalStateException( "temporal property storage not initialized!" );
        }
        else
        {
            return instance;
        }
    }

    private TemporalPropertyStoreAdapter( Config configs, File dbDir )
    {
        this.dbDir = dbDir;
        this.config = configs;
    }

    @Override
    public void init() throws Throwable
    {
        this.nodeStore = TemporalPropertyStoreFactory.newPropertyStore( resolveStoreDir( "temporal.node.properties" ) );
        this.relStore = TemporalPropertyStoreFactory.newPropertyStore( resolveStoreDir( "temporal.relationship.properties" ) );
    }

    @Override
    public void shutdown() throws Throwable
    {
        this.nodeStore.shutDown();
        this.relStore.shutDown();
    }

    private File resolveStoreDir( String folder ) throws IOException
    {
        File dir = new File( dbDir.getAbsolutePath(), folder );
        if ( dir.exists() )
        {
            if ( !dir.isDirectory() )
            {
                throw new IOException( folder + " is not a directory." );
            }
        }
        else
        {
            if ( !dir.mkdirs() )
            {
                throw new IOException( "create temporal.node.properties dir failed." );
            }
        }
        return dir;
    }

    public TemporalPropertyStore nodeStore()
    {
        return nodeStore;
    }

    public TemporalPropertyStore relStore()
    {
        return relStore;
    }

    public Slice getPoint( TemporalPropertyStore store, TemporalPropertyReadOperation query )
    {
        return store.getPointValue( query.getEntityId(), query.getProId(), query.getStart() );
    }

    public Object getRange( TemporalPropertyStore store, TemporalPropertyReadOperation query, MemTable oneEntityData )
    {
        return store.getRangeValue( query.getEntityId(), query.getProId(), query.getStart(), query.getEnd(), query.callBack(), oneEntityData );
    }

    public Object getAggrIndex( TemporalPropertyStore store, TemporalPropertyReadOperation query, MemTable oneEntityData )
    {
        return store.getByIndex( query.getIndexId(), query.getEntityId(), query.getProId(), query.getStart(), query.getEnd(), oneEntityData );
    }

    public void setValue( TemporalPropertyStore store, TimeIntervalKey intervalKey, Slice value )
    {
        store.setProperty( intervalKey, value );
    }

    public void createAggrMinMaxIndex(TemporalPropertyStore store, int propertyId, TimePointL start, TimePointL end )
    {
        store.createAggrMinMaxIndex( propertyId, start, end, 100, Calendar.MINUTE, IndexType.AGGR_MIN_MAX );
    }

    public void flushAll()
    {
        if ( this.relStore != null )
        {
            this.relStore.flushMemTable2Disk();
            this.relStore.flushMetaInfo2Disk();
        }
        if ( this.nodeStore != null )
        {
            this.nodeStore.flushMemTable2Disk();
            this.nodeStore.flushMetaInfo2Disk();
        }
    }

    public TemporalPropertyStore getNodeStore()
    {
        return nodeStore;
    }

    public TemporalPropertyStore getRelStore()
    {
        return relStore;
    }

}
