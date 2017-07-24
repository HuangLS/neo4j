package org.neo4j.kernel.impl.locking.community;

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
        int result = o.start - this.start;
        if( result != 0 )
            return result;
        return o.end = this.end;
    }
    public boolean union(TimeInterval o)
    {
        return !(this.start > o.end || this.end < o.start);
    }
}
