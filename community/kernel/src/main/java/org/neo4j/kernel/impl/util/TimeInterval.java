package org.neo4j.kernel.impl.util;

public class TimeInterval implements Comparable<TimeInterval>
{
    private int start;
    private int end;
    public TimeInterval(int s, int e)
    {
        this.start = s;
        this.end = e;
    }
    public int getStart()
    {
        return start;
    }
    public int getEnd()
    {
        return end;
    }
    public int hashCode()
    {
        return start*31 + end;
    }
    public String toString()
    {
        return "[TimeInterval:" + start + " - " + end + "]";
    }
    public boolean equals( Object o )
    {
        if( o == this )
            return true;
        if( null == o || o.getClass() != this.getClass() )
            return false;
        TimeInterval that = (TimeInterval)o;
        return that.start == this.start && that.end == this.end;
    }
    @Override
    public int compareTo( TimeInterval o )
    {
        int toret = o.start - this.start;
        if( toret != 0 )
            return toret;
        return o.end = this.end;
    }
    public boolean union(TimeInterval o)
    {
        int min = Math.max( this.start, o.start );
        int max = Math.min( this.end, o.end );
        return min <= max;
    }
}
