package org.neo4j.kernel.impl.locking.community;

import java.util.ArrayList;
import java.util.List;

//FIXME use priority queue as the base list
public class TimeIntervalList
{
    private ArrayList<TimeInterval> list;
    
    public TimeIntervalList()
    {
        this.list = new ArrayList<TimeInterval>();
    }
    
    
    public int size()
    {
        return this.list.size();
    }
    
    public boolean unionOrLessThan(TimeInterval element)
    {
        if( list.size() > 0 )
        {
            if( element.getEnd() < list.get( 0 ).getStart() )
                return true;
        }
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
    
    public boolean unionExept(TimeIntervalList other, TimeInterval element )
    {
        List<TimeInterval> temp = new ArrayList<TimeInterval>(this.list);
        temp.removeAll(other.list);
        if( temp.size() > 0 )
        {
            if( element.getEnd() < temp.get( 0 ).getStart() )
                return true;
        }
        for( TimeInterval t : temp )
        {
            if( t.union( element ) )
                return true;
        }
        return false;
    }


    public void remove( TimeIntervalList other )
    {
        this.list.removeAll(other.list);
    }
}
