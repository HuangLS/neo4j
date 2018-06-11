package org.neo4j.temporal;

import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.TemporalPropertyStoreAdapter;

/**
 * Created by song on 2018-04-21.
 */
public class TemporalIndexManager
{
    private static TemporalIndexManager self;
    private final ThreadToStatementContextBridge bridge;

    public TemporalIndexManager( ThreadToStatementContextBridge bridge )
    {
        this.bridge = bridge;
    }

    public static TemporalIndexManager getInstance( ThreadToStatementContextBridge bridge )
    {
        if ( self == null )
        {
            self = new TemporalIndexManager( bridge );
        }
        return self;
    }

    public List<IndexMetaData> nodeIndexes()
    {
        return tpStore().getNodeStore().listIndex();
    }

    public PropertyValueIntervalBuilder nodeQueryValueIndex( int start, int end )
    {
        return new PropertyValueIntervalBuilder( start, end, true );
    }

    public long nodeCreateValueIndex( int start, int end, String... proNames )
    {
        return tpStore().getNodeStore().createValueIndex( start, end, proName2Id( proNames ) );
    }

    public long nodeCreateDurationIndex( int start, int end, String proName, int every, int timeUnit, ValueGroupingMap valueGroup )
    {
        return tpStore().getNodeStore().createAggrDurationIndex( proName2Id( proName ), start, end, valueGroup, every, timeUnit );
    }

    public long nodeCreateMinMaxIndex( int start, int end, String proName, int every, int timeUnit, IndexType type )
    {
        return tpStore().getNodeStore().createAggrMinMaxIndex( proName2Id( proName ), start, end, every, timeUnit, type );
    }

    public List<IndexMetaData> relIndexes()
    {
        return tpStore().getRelStore().listIndex();
    }

    public PropertyValueIntervalBuilder relQueryValueIndex( int start, int end )
    {
        return new PropertyValueIntervalBuilder( start, end, false );
    }

    public long relCreateValueIndex( int start, int end, String... proNames )
    {
        return tpStore().getRelStore().createValueIndex( start, end, proName2Id( proNames ) );
    }

    public long relCreateDurationIndex( int start, int end, String proName, int every, int timeUnit, ValueGroupingMap valueGroup )
    {
        return tpStore().getRelStore().createAggrDurationIndex( proName2Id( proName ), start, end, valueGroup, every, timeUnit );
    }

    public long relCreateMinMaxIndex( int start, int end, String proName, int every, int timeUnit, IndexType type )
    {
        return tpStore().getRelStore().createAggrMinMaxIndex( proName2Id( proName ), start, end, every, timeUnit, type );
    }

    private ReadOperations read()
    {
        return bridge.get().readOperations();
    }

    private int proName2Id( String name )
    {
        return read().propertyKeyGetForName( name );
    }

    private List<Integer> proName2Id( String[] names )
    {
        List<Integer> proIds = new ArrayList<>();
        for ( String proName : names )
        {
            proIds.add( proName2Id( proName ) );
        }
        return proIds;
    }

    private TemporalPropertyStoreAdapter tpStore()
    {
        return TemporalPropertyStoreAdapter.getInstance();
    }

    public class PropertyValueIntervalBuilder
    {
        private final int start;
        private final int end;
        private final boolean isNode;
        private final Map<Integer,Triple<String,Object,Object>> valInterval;//proId, minVal, maxVal;

        PropertyValueIntervalBuilder( int start, int end, boolean isNode )
        {
            this.start = start;
            this.end = end;
            this.isNode = isNode;
            this.valInterval = new HashMap<>();
        }

        public PropertyValueIntervalBuilder propertyValRange( String key, Object valMin, Object valMax )
        {
            int proId = proName2Id( key );
            this.valInterval.put( proId, Triple.of( key, valMin, valMax ) );
            return this;
        }

        public List<IntervalEntry> query()
        {
            try
            {
                return read().getTemporalPropertyByValueIndex( this );
            }
            catch ( PropertyNotFoundException e )
            {
                throw new NotFoundException( e );
            }
        }

        public Map<Integer,Triple<String,Object,Object>> getPropertyValues()
        {
            return valInterval;
        }

        public int getStart()
        {
            return start;
        }

        public int getEnd()
        {
            return end;
        }

        public boolean isNode()
        {
            return isNode;
        }
    }
}
