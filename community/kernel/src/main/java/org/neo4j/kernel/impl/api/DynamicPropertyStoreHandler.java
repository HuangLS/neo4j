package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.api.store.DynamicPropertyStore;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;

public class DynamicPropertyStoreHandler extends NeoCommandHandler.Adapter
{
    
    private DynamicPropertyStore store;
    public DynamicPropertyStoreHandler ( DynamicPropertyStore store )
    {
        this.store = store;
    }
    
    
    @Override
    public boolean visitNodeDynProCommand(Command.NodeDynProCommand command)
    {
        long id = command.getProContainerKey();
        int proId = command.getProId();
        int time = command.getTime();
        byte[] value = command.getValue();
        this.store.NodeAdd( id, proId, time, value );
        return false;
    }
    
    @Override 
    public boolean visitRelationshipDynProCommand(Command.RelationshipDynProCommand command)
    {
        long id = command.getProContainerKey();
        int proId = command.getProId();
        int time = command.getTime();
        byte[] value = command.getValue();
        this.store.RelationshipAdd( id, proId, time, value );
        return false;
    }
}
