package play;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.temporal.TimePoint;

/**
 * Created by song on 2019-07-08.
 */
public class TGraphFunctionTest
{


    @Test
    public void indexCreation() throws IOException
    {
        GraphDatabaseService tgraph = db( true );
        try ( Transaction tx = tgraph.beginTx() )
        {
            for(int i=0; i<100; i++){
                Node node = tgraph.createNode();
                for(int j=0; j<2000; j++){
                    node.setTemporalProperty( "test", new TimePoint(j), 0 );
                }

                node.setProperty( "hehe", 1 );
                node.setProperty( "hehe", 2 );
                tx.success();
            }

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

    private GraphDatabaseService db( boolean fromScratch ) throws IOException
    {
        File dir = new File( System.getProperty( "java.io.tmpdir" ), "TGRAPH-db" );
        if ( fromScratch )
        {
            deleteFile( dir );
        }
        return new GraphDatabaseFactory().newEmbeddedDatabase( dir );
    }

}
