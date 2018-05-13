/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package examples;

import org.act.temporalProperty.query.aggr.ValueGroupingMap;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.temporal.TemporalIndexManager;

import static java.lang.System.getProperty;
import static java.util.Calendar.SECOND;

public class TakeItForASpin
{
    public static void main( String[] args )
    {
        File dir = new File( getProperty( "java.io.tmpdir" ), "graph-db");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dir );
        long i;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            i = node.getId();
            System.out.println(i);
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            TemporalIndexManager ti = db.temporalIndex();
            ti.nodeCreateValueIndex(10, 20, "hehe");
            ValueGroupingMap vMap = new ValueGroupingMap.IntValueGroupMap();
            // fixme: should add group here.
            ti.nodeCreateDurationIndex( 10, 20, "haha", 2, SECOND, vMap );
            ti.nodeQueryValueIndex( 10, 20 ).propertyValRange( "hehe", 2, 20 ).propertyValRange( "haha", 4, 30 ).query();
            Node node = db.getNodeById(i);
            node.setProperty("hehe", "haha");
            node.getProperty("hehe");
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema();
            Node node = db.getNodeById(i);
            Object s = node.getProperty("hehe");
            System.out.println(s);

            int t = time();
            node.getTemporalPropertyWithIndex("haha", t, t+4, 1 );
            node.setTemporalProperty("haha", t, "hehe");
            Object v = node.getTemporalProperty("haha", t);
            if(v instanceof String){
                System.out.println(v);
            }
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
//        try
//        {
//            System.in.read();
//        } catch (IOException e)
//        {
//            e.printStackTrace();
//        }
    }

    private static int time(){
        return (int) (System.currentTimeMillis()/1000);
    }
}
