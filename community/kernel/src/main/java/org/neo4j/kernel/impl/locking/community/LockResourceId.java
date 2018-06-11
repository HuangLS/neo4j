package org.neo4j.kernel.impl.locking.community;


import java.util.Objects;

/**
 * Created by song on 2017/7/22 0022.
 */
public interface LockResourceId extends Comparable<LockResourceId> {
    long get();

    class Normal implements LockResourceId
    {
        private final long id;
        public Normal(long value)
        {
            this.id = value;
        }
        @Override
        public long get(){
            return id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Normal that = (Normal) o;
            return id == that.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id);
        }

        @Override
        public String toString()
        {
            return "Normal{" +
                    "id=" + id +
                    '}';
        }

        @Override
        public int compareTo(LockResourceId o)
        {
            return Long.valueOf(id).compareTo(o.get());
        }
    }

    class TemporalProp implements LockResourceId
    {
        final private long entityId;
        final private int propertyKeyId;
        public TemporalProp(long entityId, int propertyKeyId){
            this.entityId = entityId;
            this.propertyKeyId = propertyKeyId;
        }

        public long get()
        {
            return entityId;
        }

        public int getPropertyKeyId()
        {
            return propertyKeyId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TemporalProp that = (TemporalProp) o;
            return entityId == that.entityId && propertyKeyId == that.propertyKeyId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(entityId, propertyKeyId);
        }

        @Override
        public String toString()
        {
            return "TemporalProp{" +
                    "entityId=" + entityId +
                    ", propertyKeyId=" + propertyKeyId +
                    '}';
        }

        @Override
        public int compareTo(LockResourceId o)
        {
            int tmp = Long.valueOf(entityId).compareTo(o.get());
            if(tmp==0 && o instanceof TemporalProp){
                TemporalProp t = (TemporalProp) o;
                return Integer.valueOf(propertyKeyId).compareTo(t.getPropertyKeyId());
            }else{
                return tmp;
            }
        }
    }
}

