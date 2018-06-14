package play;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Created by song on 2018-06-11.
 */
public class Neo4jTest
{
    public GraphDatabaseService db( boolean fromScratch ) throws IOException
    {
        File dir = new File( System.getProperty( "java.io.tmpdir" ), "TGRAPH-db" );
        if ( fromScratch )
        {
            Files.deleteIfExists( dir.toPath() );
        }
        return new GraphDatabaseFactory().newEmbeddedDatabase( dir );
    }

    @Test
    public void createIndexes() throws IOException
    {
        GraphDatabaseService neo4j = db( true );
        try ( Transaction tx = neo4j.beginTx() )
        {
            Node node = neo4j.createNode();
            node.setProperty( "hehe", "haha" );
//            neo4j.index().forNodes( "hehe" );
            node.setProperty( "hehe", 1 );
            node.setProperty( "hehe", 2 );
//            neo4j.registerKernelEventHandler( new KernelEventHandler() {
//                @Override
//                public void beforeShutdown()
//                {
//                    //
//                }
//
//                @Override
//                public void kernelPanic( ErrorState error )
//                {
//                    //
//                }
//
//                @Override
//                public Object getResource()
//                {
//                    return null;
//                }
//
//                @Override
//                public ExecutionOrder orderComparedTo( KernelEventHandler other )
//                {
//                    return null;
//                }
//            } );
//            neo4j.registerTransactionEventHandler( new TransactionEventHandler<Object>() {
//                @Override
//                public Object beforeCommit( TransactionData data ) throws Exception
//                {
//                    //return null;
//                }
//
//                @Override
//                public void afterCommit( TransactionData data, Object state )
//                {
//                    //
//                }
//
//                @Override
//                public void afterRollback( TransactionData data, Object state )
//                {
//                    //
//                }
//            } )
        }
        neo4j.shutdown();
    }
}
