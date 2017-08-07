package org.act.temporal;

import org.act.temporalProperty.util.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by song on 2017/8/1 0001.
 */
public class RangeIndexTest
{
    private static GraphDatabaseService db;
    private static Logger log = LoggerFactory.getLogger(RangeIndexTest.class);

    @Before
    public void initDB()
    {
        File dir = new File( "D:/songjh/projects/TGraph/runtime/test", "graph-db");
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dir );
    }

    @Test
    public void test()
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.createTemporalProperty("amtb", 0, 4, 0);
            tx.success();
        }
        int i=1;

        for(int k=0; k<1000; k++)
        {
            try (Transaction tx = db.beginTx())
            {
                for (int j = 0; j < 20000; j++)
                {
                    node.setTemporalProperty("amtb", i, i);
                    i++;
                }
                tx.success();
            }
            if(k%100==0){
                log.debug("write {} tx.", k);
            }
        }

        try ( Transaction tx = db.beginTx() )
        {
            long x = (long) node.getTemporalProperties("haha", 1, i, new TemporalPropertyRangeQuery()
            {
                private long x;
                @Override
                public boolean onTimePoint(int time, Object value)
                {
                    x += (int) value;
                    return true;
                }

                @Override
                public void onCallBatch(Slice batchValue)
                {
                    log.trace("on Call batch: {}", batchValue);
                }

                @Override
                public Object onReturn()
                {
                    log.trace("on Return");
                    return x;
                }
            });
            log.trace("x={}", x);
            tx.success();
        }
    }

    @After
    public void finish()
    {
        if(db!=null) db.shutdown();
    }
}
