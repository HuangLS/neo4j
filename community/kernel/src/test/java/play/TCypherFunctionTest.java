package play;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.temporal.TimePoint;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TCypherFunctionTest {
    @Test
    public void write() throws IOException {
        GraphDatabaseService db = initDB(true);
        long nodeId = 0;
        try(Transaction tx = db.beginTx()){
            Node n = db.createNode();
            nodeId = n.getId();
            n.setTemporalProperty("travel_time", new TimePoint(0), 0);
            tx.success();
        }
        try(Transaction tx = db.beginTx()){
            db.execute("Match (n) WHERE n.id='"+nodeId+"' SET n.travel_time = TV(3~NOW:40)");
            tx.success();
        }
        try(Transaction tx = db.beginTx()){
            for(int t : Arrays.asList(0, 1, 2, 3, 4)){
                Object o = db.getNodeById(nodeId).getTemporalProperty("travel_time", new TimePoint(t));
                System.out.println(o);
            }
            tx.success();
        }
    }


    private static void deleteFile(File element ) {
        if (element.isDirectory()) {
            for (File sub : element.listFiles()) {
                deleteFile(sub);
            }
        }
        element.delete();
    }


    private GraphDatabaseService initDB(boolean fromScratch ) throws IOException
    {
        File dir = new File( System.getProperty( "java.io.tmpdir" ), "TGRAPH-db" );
        if ( fromScratch )
        {
            deleteFile( dir );
        }
        return new GraphDatabaseFactory().newEmbeddedDatabase( dir );
    }
}
