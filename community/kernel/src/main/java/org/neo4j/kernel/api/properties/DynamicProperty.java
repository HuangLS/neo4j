package org.neo4j.kernel.api.properties;

import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfArray;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;
/**
 * DynamicProperty is just a representation for a dynamic property update, including the update time, update value.
 *
 *
 * @author huanghx( huanghx@act.buaa.edu.cn )
 */
public class DynamicProperty extends IntegralArrayProperty
{

    public DynamicProperty( int propertyKeyId, int time, byte[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
        this.time = time;
    }

    private final byte[] value;
    private final int time;

    public int time()
    {
        return this.time;
    }
    
    @Override
    public byte[] value()
    {
        return value.clone();
    }

    @Override
    public int length()
    {
        return value.length;
    }

    @Override
    public long longValue( int index )
    {
        return value[index];
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return withObjectOverhead( withReference( sizeOfArray( value ) ) ) + 8;
    
    }
}

