package org.neo4j.temporal;

import org.act.temporalProperty.query.TimeInterval;

import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Created by song on 2018-12-11.
 */
public class TemporalIndexDescriptor extends IndexDescriptor
{
    private final int from;
    private final int to;

    public TimeInterval timeInterval()
    {
        return new TimeInterval( from, to );
    }

    public enum IndexType{
        VALUE(0), DURATION(1), MIN_MAX(3);

        private final int id;

        IndexType( int id ){
            this.id = id;
        }

        public int getId(){
            return id;
        }

        public static IndexType fromId(int id){
            switch ( id ){
            case 0: return VALUE;
            case 1: return DURATION;
            case 2: return MIN_MAX;
            default: throw new RuntimeException( "TGraph SNH: unknown index type id "+id );
            }
        }
    }

    protected TemporalIndexDescriptor( IndexType indexType, int propertyKeyId, int from, int to )
    {
        super( indexType.getId(), propertyKeyId );
        this.from = from;
        this.to = to;
    }

    public static MinMax minMaxIndexDescriptor( int propertyKeyId, int from, int to ){
        return new MinMax( propertyKeyId, from, to );
    }


    public static class MinMax extends TemporalIndexDescriptor
    {
        public MinMax( int propertyKeyId, int from, int to )
        {
            super( IndexType.MIN_MAX, propertyKeyId, from, to );
        }
    }
}
