package org.neo4j.graphdb;

/**
 * TGraph development exception. used when some features are not implemented.
 * Created by song on 17-7-18.
 */
public class TGraphInternalError extends RuntimeException
{
    public TGraphInternalError(String msg){
        super(msg);
    }

    public TGraphInternalError(){
        super();
    }
}
