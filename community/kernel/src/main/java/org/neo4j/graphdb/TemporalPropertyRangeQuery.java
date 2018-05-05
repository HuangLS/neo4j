package org.neo4j.graphdb;

import org.act.temporalProperty.query.range.TimeRangeQuery;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TemporalPropertyValueConvertor;

/**
 * Created by song on 2017/8/1 0001.
 */
public abstract class TemporalPropertyRangeQuery extends TimeRangeQuery
{
//    public static TemporalPropertyRangeQuery COUNT = new TemporalPropertyRangeQuery()
//    {
//        @Override
//        public void setValueType(String valueType)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onNewValue(int time, Slice value)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onCallBatch(Slice batchValue)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public Object onReturn()
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//        }
//
//        @Override
//        public CallBackType getType()
//        {
//            return CallBackType.COUNT;
//        }
//    };
//
//    public static TemporalPropertyRangeQuery SUM = new TemporalPropertyRangeQuery()
//    {
//        @Override
//        public void setValueType(String valueType)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onNewValue(int time, Slice value)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onCallBatch(Slice batchValue)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public Object onReturn()
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//        }
//
//        @Override
//        public CallBackType getType()
//        {
//            return CallBackType.SUM;
//        }
//    };
//
//    public static TemporalPropertyRangeQuery MIN = new TemporalPropertyRangeQuery()
//    {
//        @Override
//        public void setValueType(String valueType)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onNewValue(int time, Slice value)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onCallBatch(Slice batchValue)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public Object onReturn()
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//        }
//
//        @Override
//        public CallBackType getType()
//        {
//            return CallBackType.MIN;
//        }
//    };
//
//    public static TemporalPropertyRangeQuery MAX = new TemporalPropertyRangeQuery()
//    {
//        @Override
//        public void setValueType(String valueType)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onNewValue(int time, Slice value)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public void onCallBatch(Slice batchValue)
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//
//        }
//
//        @Override
//        public Object onReturn()
//        {
//            //FIXME TGraph: Not Implement.
//            throw new TGraphNoImplementationException();
//        }
//
//        @Override
//        public CallBackType getType()
//        {
//            return CallBackType.USER;
//        }
//    };

    private String valType;

    @Override
    public CallBackType getType()
    {
        return CallBackType.USER;
    }

    @Override
    public void setValueType( String valueType )
    {
        this.valType = valueType;
    }

    @Override
    public void onNewValue(int time, Slice value)
    {
        if(value==null)
        {
            this.onTimePoint(time, null);
        }else{
            this.onTimePoint(time, TemporalPropertyValueConvertor.revers(this.valType, value.getBytes()));
        }
    }

    abstract public boolean onTimePoint(int time, Object value);

    @Override
    public void onCallBatch(Slice batchValue)
    {
        //FIXME TGraph: Not Implement.
        //throw new TGraphNoImplementationException();
    }
}
