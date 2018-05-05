package org.neo4j.temporal;

import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;

import java.util.ArrayList;
import java.util.List;

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

    public List<IndexMetaData> nodeIndexes(){
        return tpStore().getNodeStore().listIndex();
    }

    public Object nodeQueryValueIndex(IndexQueryRegion condition){
        return tpStore().getNodeStore().getEntries( condition );
    }

    public long nodeCreateValueIndex( int start, int end, String... proNames )
    {
        return tpStore().getNodeStore().createValueIndex( start, end, proName2Id( proNames ) );
    }

    public long nodeCreateAggregationDurationIndex( int start, int end, String proName, int every, int timeUnit, ValueGroupingMap valueGroup )
    {
        return tpStore().getNodeStore().createAggrDurationIndex( proName2Id( proName ), start, end, valueGroup, every, timeUnit );
    }

    public long nodeCreateAggregationMinMaxIndex( int start, int end, String proName, int every, int timeUnit, IndexType type )
    {
        return tpStore().getNodeStore().createAggrMinMaxIndex( proName2Id( proName ), start, end, every, timeUnit, type );
    }

    public List<IndexMetaData> relIndexes(){
        return tpStore().getRelStore().listIndex();
    }

    public Object relQueryValueIndex(IndexQueryRegion condition){
        return tpStore().getRelStore().getEntries( condition );
    }

    public long relCreateValueIndex( int start, int end, String... proNames )
    {
        return tpStore().getRelStore().createValueIndex( start, end, proName2Id( proNames ) );
    }

    public long relCreateAggregationDurationIndex( int start, int end, String proName, int every, int timeUnit, ValueGroupingMap valueGroup )
    {
        return tpStore().getRelStore().createAggrDurationIndex( proName2Id( proName ), start, end, valueGroup, every, timeUnit );
    }

    public long relCreateAggregationMinMaxIndex( int start, int end, String proName, int every, int timeUnit, IndexType type )
    {
        return tpStore().getRelStore().createAggrMinMaxIndex( proName2Id( proName ), start, end, every, timeUnit, type );
    }

    private int proName2Id( String name )
    {
        return bridge.get().readOperations().propertyKeyGetForName( name );
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
}
