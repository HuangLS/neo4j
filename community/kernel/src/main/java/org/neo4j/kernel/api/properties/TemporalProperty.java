package org.neo4j.kernel.api.properties;

/**
 * Created by song on 17-7-7.
 */
public class TemporalProperty extends IntegralArrayProperty {

    private final byte[] value;
    private final int valueLength;
    private final int time;

    public TemporalProperty(int propertyKeyId, int time, int valueLength, byte[] value)
    {
        super(propertyKeyId);
        this.time = time;
        this.valueLength = valueLength;
        this.value = value;
    }

    @Override
    public int length() {
        return valueLength;
    }

    @Override
    public long longValue(int index) {
        return value[index];
    }

    @Override
    public byte[] value() {
        return value.clone();
    }


    public int time() {
        return this.time;
    }

    public int valueLength() {
        return this.valueLength;
    }
}
