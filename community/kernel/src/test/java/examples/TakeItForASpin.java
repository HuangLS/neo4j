/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

public class TakeItForASpin
{
    
    private static final int NODE_NUM = 10;
    private static List<String> PRO_NAMES = new ArrayList<String>( 10 );
    private static final int TIME_NUM = 100;
    private static final int TX_NUM = 5;
    static
    {
        for( int i = 0; i<10; i++ )
        {
            PRO_NAMES.add( String.valueOf(i) );
        }
    }
    
    
    public static void main( String[] args )
    {
        File dir = TargetDirectory.forTest( TakeItForASpin.class ).makeGraphDbDir();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dir.getAbsolutePath() );
        
        {
            db.shutdown();
        }
    }
}
