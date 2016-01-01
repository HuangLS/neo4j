package org.neo4j.kernel.impl.util.collection;

import java.util.ArrayList;
import org.neo4j.kernel.impl.util.TimeInterval;

//FIXME use priority queue as the base list
public class TimeIntervalList
{
    private ArrayList<TimeInterval> list;
    
    public TimeIntervalList()
    {
        this.list = new ArrayList<TimeInterval>();
    }
    
    
    public boolean union(TimeInterval element)
    {
        for( TimeInterval t : list )
        {
            if( t.union( element ) )
                return true;
        }
        return false;
    }
    
    
    public void remove( TimeInterval t )
    {
        list.remove( t );
    }
    
    public TimeInterval get( int index )
    {
        if( index < 0 || index >= list.size() )
            throw new IndexOutOfBoundsException();
        return list.get( index );
    }
    
    public void insert(TimeInterval element )
    {
        for( int i = 0; i<list.size(); i++ )
        {
            if( element.compareTo( list.get( i ) ) <=0 )
            {
                list.add( i, element );
                return;
            }
        }
        list.add( element );
    }
}
