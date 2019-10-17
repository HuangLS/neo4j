package org.neo4j.temporal;

import org.neo4j.kernel.impl.store.TemporalPropertyStoreAdapter;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

import java.io.IOException;

/**
 * Created by song on 17-7-10.
 */
public class TemporalPropertyStoreHandler extends CommandHandler.Adapter
{
    private final TemporalPropertyStoreAdapter store;

    public TemporalPropertyStoreHandler(TemporalPropertyStoreAdapter temporalPropStore)
    {
        this.store = temporalPropStore;
    }

//    @Override
//    public boolean visitNodeTemporalPropertyDeleteCommand(Command.NodeTemporalPropertyDeleteCommand command) throws IOException
//    {
//        this.store.nodeDelete(command.getId());
//        return false;
//    }
//
//    @Override
//    public boolean visitRelationshipTemporalPropertyDeleteCommand(Command.RelationshipTemporalPropertyDeleteCommand command) throws IOException
//    {
//        this.store.relationshipDelete(command.getId());
//        return false;
//    }

    @Override
    public boolean visitNodeTemporalPropertyIndexCommand( Command.NodeTemporalPropertyIndexCommand command ) throws IOException
    {
        this.store.createAggrMinMaxIndex( store.nodeStore(), command.getPropertyId(), command.getStart(), command.getEnd() );
        return false;
    }

    @Override
    public boolean visitNodeTemporalPropertyCommand(Command.NodeTemporalPropertyCommand command) throws IOException
    {
        this.store.setValue( store.nodeStore(), command.getIntervalEntry().getKey(), command.getIntervalEntry().getValue());
        return false;
    }

    @Override
    public boolean visitRelationshipTemporalPropertyCommand(Command.RelationshipTemporalPropertyCommand command) throws IOException
    {
        this.store.setValue( store.relStore(), command.getIntervalEntry().getKey(), command.getIntervalEntry().getValue());
        return false;
    }
}
