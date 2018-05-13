package org.neo4j.temporal;

import org.act.temporalProperty.exception.TPSRuntimeException;

/**
 * Created by song on 2018-05-13.
 */
public class TGraphUserInputException extends TPSRuntimeException
{
    public TGraphUserInputException( String msg ) { super( msg ); }

    public TGraphUserInputException( String msg, Object... o ) { super( msg, o ); }
}
