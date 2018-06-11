package org.neo4j.graphdb;

/**
 * TGraph development exception. used when some features are not implemented.
 * Created by song on 17-6-30.
 */
public class TGraphNoImplementationException extends RuntimeException
{
    public TGraphNoImplementationException(String msg){
        super(msg);
    }

    public TGraphNoImplementationException(){
        super();
    }
}
