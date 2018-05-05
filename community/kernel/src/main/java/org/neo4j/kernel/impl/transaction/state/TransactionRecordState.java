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
package org.neo4j.kernel.impl.transaction.state;

import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.util.Slice;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.Mode;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.statistics.IntCounter;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Transaction containing {@link org.neo4j.kernel.impl.transaction.command.Command commands} reflecting the operations
 * performed in the transaction.
 * <p>
 * This class currently has a symbiotic relationship with {@link KernelTransaction}, with which it always has a 1-1
 * relationship.
 * <p>
 * The idea here is that KernelTransaction will eventually take on the responsibilities of WriteTransaction, such as
 * keeping track of transaction state, serialization and deserialization to and from logical log, and applying things
 * to store. It would most likely do this by keeping a component derived from the current WriteTransaction
 * implementation as a sub-component, responsible for handling logical log commands.
 * <p>
 * The class XAResourceManager plays in here as well, in that it shares responsibilities with WriteTransaction to
 * write data to the logical log. As we continue to refactor this subsystem, XAResourceManager should ideally not know
 * about the logical log, but defer entirely to the Kernel to handle this. Doing that will give the kernel full
 * discretion to start experimenting with higher-performing logical log implementations, without being hindered by
 * having to contend with the JTA compliance layers. In short, it would encapsulate the logical log/storage logic better
 * and thus make it easier to change.
 */
public class TransactionRecordState implements RecordState
{
    public static class TemporalProKeyValue
    {
        private InternalKey key;
        private Slice value;

        public TemporalProKeyValue( InternalKey key, Slice value )
        {
            this.key = key;
            this.value = value;
        }

        InternalKey getKey()
        {
            return key;
        }

        Slice getValue()
        {
            return value;
        }
    }

    public void nodeTemporalPropertyChange( long id, MemTable changes )
    {
        this.nodeTemporalPropertyChanges.merge( changes );
    }

    public void relationshipTemporalPropertyChange( long id, MemTable changes )
    {
        this.relationshipTemporalPropertyChanges.merge( changes );
    }

    private final MemTable nodeTemporalPropertyChanges = new MemTable();
    private final MemTable relationshipTemporalPropertyChanges = new MemTable();
//    private final List<TemporalProKeyValue> nodeDeleteTemporalPropertyPoint = new LinkedList<>();
//    private final List<Slice> nodeDeleteTemporalProperties = new LinkedList<>();
//    private final List<TemporalProKeyValue> relationshipDeleteTemporalPropertyPoint = new LinkedList<TemporalProKeyValue>();
//    private final List<Slice> relationshipDeleteTemporalProperties = new LinkedList<Slice>();

    private final IntegrityValidator integrityValidator;
    private final NeoStoreTransactionContext context;
    private final NodeStore nodeStore;
    private final MetaDataStore metaDataStore;
    private final SchemaStore schemaStore;

    private RecordChanges<Long,NeoStoreRecord,Void> neoStoreRecord;
    private long lastCommittedTxWhenTransactionStarted;
    private boolean prepared;

    public TransactionRecordState( NeoStores neoStores, IntegrityValidator integrityValidator, NeoStoreTransactionContext context )
    {
        this.nodeStore = neoStores.getNodeStore();
        this.metaDataStore = neoStores.getMetaDataStore();
        this.schemaStore = neoStores.getSchemaStore();
        this.integrityValidator = integrityValidator;
        this.context = context;
    }

    /**
     * Set this record state to a pristine state, acting as if it had never been used.
     *
     * @param lastCommittedTxWhenTransactionStarted is the highest committed transaction id when this transaction
     * begun. No operations in this transaction are allowed to have
     * taken place before that transaction id. This is used by
     * constraint validation - if a constraint was not online when this
     * transaction begun, it will be verified during prepare. If you are
     * writing code against this API and are unsure about what to set
     * this value to, 0 is a safe choice. That will ensure all
     * constraints are checked.
     */
    public void initialize( long lastCommittedTxWhenTransactionStarted )
    {
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        prepared = false;
    }

    public void clear()
    {
        // no point in caching neostore record changes since they are rare, let's simply make sure they are cleared
        neoStoreRecord = null;
        context.clear();
    }

    @Override
    public boolean hasChanges()
    {
        return context.hasChanges() || (neoStoreRecord != null && neoStoreRecord.changeSize() > 0);
    }

    // Only for test-access
    public NeoStoreTransactionContext getContext()
    {
        return context;
    }

    @Override
    public void extractCommands( Collection<Command> commands ) throws TransactionFailureException
    {
        assert !prepared : "Transaction has already been prepared";

        integrityValidator.validateTransactionStartKnowledge( lastCommittedTxWhenTransactionStarted );

        int noOfCommands = context.getNodeRecords().changeSize() + context.getRelRecords().changeSize() + context.getPropertyRecords().changeSize() +
                context.getSchemaRuleChanges().changeSize() + context.getPropertyKeyTokenRecords().changeSize() + context.getLabelTokenRecords().changeSize() +
                context.getRelationshipTypeTokenRecords().changeSize() + context.getRelGroupRecords().changeSize() +
                (neoStoreRecord != null ? neoStoreRecord.changeSize() : 0);

        PeekingIterator<Entry<TimeIntervalKey,Slice>> nodeTpIter = nodeTemporalPropertyChanges.intervalEntryIterator();
        while ( nodeTpIter.hasNext() )
        {
            Entry<TimeIntervalKey,Slice> entry = nodeTpIter.next();
            Command.NodeTemporalPropertyCommand command = new Command.NodeTemporalPropertyCommand( entry.getKey(), entry.getValue() );
            commands.add( command );
            noOfCommands++;
        }
        PeekingIterator<Entry<TimeIntervalKey,Slice>> relTpIter = relationshipTemporalPropertyChanges.intervalEntryIterator();
        while ( relTpIter.hasNext() )
        {
            Entry<TimeIntervalKey,Slice> entry = relTpIter.next();
            Command.RelationshipTemporalPropertyCommand command = new Command.RelationshipTemporalPropertyCommand( entry.getKey(), entry.getValue() );
            commands.add( command );
            noOfCommands++;
        }


        for ( RecordProxy<Integer,LabelTokenRecord,Void> record : context.getLabelTokenRecords().changes() )
        {
            Command.LabelTokenCommand command = new Command.LabelTokenCommand();
            command.init( record.forReadingLinkage() );
            commands.add( command );
        }
        for ( RecordProxy<Integer,RelationshipTypeTokenRecord,Void> record : context.getRelationshipTypeTokenRecords().changes() )
        {
            Command.RelationshipTypeTokenCommand command = new Command.RelationshipTypeTokenCommand();
            command.init( record.forReadingLinkage() );
            commands.add( command );
        }
        for ( RecordProxy<Integer,PropertyKeyTokenRecord,Void> record : context.getPropertyKeyTokenRecords().changes() )
        {
            Command.PropertyKeyTokenCommand command = new Command.PropertyKeyTokenCommand();
            command.init( record.forReadingLinkage() );
            commands.add( command );
        }

        // Collect nodes, relationships, properties
        List<Command> nodeCommands = new ArrayList<>( context.getNodeRecords().changeSize() );
        int skippedCommands = 0;
        for ( RecordProxy<Long,NodeRecord,Void> change : context.getNodeRecords().changes() )
        {
            NodeRecord record = change.forReadingLinkage();
            integrityValidator.validateNodeRecord( record );
            Command.NodeCommand command = new Command.NodeCommand();
            command.init( change.getBefore(), record );
            nodeCommands.add( command );
        }
        Collections.sort( nodeCommands, COMMAND_SORTER );

        List<Command> relCommands = new ArrayList<>( context.getRelRecords().changeSize() );
        for ( RecordProxy<Long,RelationshipRecord,Void> record : context.getRelRecords().changes() )
        {
            Command.RelationshipCommand command = new Command.RelationshipCommand();
            command.init( record.forReadingLinkage() );
            relCommands.add( command );
        }
        Collections.sort( relCommands, COMMAND_SORTER );

        List<Command> propCommands = new ArrayList<>( context.getPropertyRecords().changeSize() );
        for ( RecordProxy<Long,PropertyRecord,PrimitiveRecord> change : context.getPropertyRecords().changes() )
        {
            Command.PropertyCommand command = new Command.PropertyCommand();
            command.init( change.getBefore(), change.forReadingLinkage() );
            propCommands.add( command );
        }
        Collections.sort( propCommands, COMMAND_SORTER );

        List<Command> relGroupCommands = new ArrayList<>( context.getRelGroupRecords().changeSize() );
        for ( RecordProxy<Long,RelationshipGroupRecord,Integer> change : context.getRelGroupRecords().changes() )
        {
            if ( change.isCreated() && !change.forReadingLinkage().inUse() )
            {
                /*
                 * This is an edge case that may come up and which we must handle properly. Relationship groups are
                 * not managed by the tx state, since they are created as side effects rather than through
                 * direct calls. However, they differ from say, dynamic records, in that their management can happen
                 * through separate code paths. What we are interested in here is the following scenario.
                 * 0. A node has one less relationship that is required to transition to dense node. The relationships
                 *    it has belong to at least two different types
                 * 1. In the same tx, a relationship is added making the node dense and all the relationships of a type
                 *    are removed from that node. Regardless of the order these operations happen, the creation of the
                 *    relationship (and the transition of the node to dense) will happen first.
                 * 2. A relationship group will be created because of the transition to dense and then deleted because
                 *    all the relationships it would hold are no longer there. This results in a relationship group
                 *    command that appears in the tx as not in use. Depending on the final order of operations, this
                 *    can end up using an id that is higher than the highest id seen so far. This may not be a problem
                 *    for a single instance, but it can result in errors in cases where transactions are applied
                 *    externally, such as backup or HA.
                 *
                 * The way we deal with this issue here is by not issuing a command for that offending record. This is
                 * safe, since the record is not in use and never was, so the high id is not necessary to change and
                 * the store remains consistent.
                 */
                skippedCommands++;
                continue;
            }
            Command.RelationshipGroupCommand command = new Command.RelationshipGroupCommand();
            command.init( change.forReadingData() );
            relGroupCommands.add( command );
        }
        Collections.sort( relGroupCommands, COMMAND_SORTER );

        addFiltered( commands, Mode.CREATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.UPDATE, propCommands, relCommands, relGroupCommands, nodeCommands );
        addFiltered( commands, Mode.DELETE, propCommands, relCommands, relGroupCommands, nodeCommands );

        if ( neoStoreRecord != null )
        {
            for ( RecordProxy<Long,NeoStoreRecord,Void> change : neoStoreRecord.changes() )
            {
                Command.NeoStoreCommand command = new Command.NeoStoreCommand();
                command.init( change.forReadingData() );
                commands.add( command );
            }
        }
        for ( RecordProxy<Long,Collection<DynamicRecord>,SchemaRule> change : context.getSchemaRuleChanges().changes() )
        {
            integrityValidator.validateSchemaRule( change.getAdditionalData() );
            Command.SchemaRuleCommand command = new Command.SchemaRuleCommand();
            command.init( change.getBefore(), change.forChangingData(), change.getAdditionalData() );
            commands.add( command );
        }
        assert commands.size() == noOfCommands - skippedCommands :
                format( "Expected %d final commands, got %d " + "instead, with %d skipped", noOfCommands, commands.size(), skippedCommands );

        prepared = true;
    }

    public void relCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        context.relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public void relDelete( long relId )
    {
        context.relationshipDelete( relId );
    }

    @SafeVarargs
    private final void addFiltered( Collection<Command> target, Mode mode, Collection<? extends Command>... commands )
    {
        for ( Collection<? extends Command> c : commands )
        {
            for ( Command command : c )
            {
                if ( command.getMode() == mode )
                {
                    target.add( command );
                }
            }
        }
    }

    /**
     * Deletes a node by its id, returning its properties which are now removed.
     *
     * @param nodeId The id of the node to delete.
     * @return The properties of the node that were removed during the delete.
     */
    public void nodeDelete( long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete Node[" + nodeId + "] since it has already been deleted." );
        }
        nodeRecord.setInUse( false );
        nodeRecord.setLabelField( Record.NO_LABELS_FIELD.intValue(), markNotInUse( nodeRecord.getDynamicLabelRecords() ) );
        getAndDeletePropertyChain( nodeRecord );
    }

    private Collection<DynamicRecord> markNotInUse( Collection<DynamicRecord> dynamicLabelRecords )
    {
        for ( DynamicRecord record : dynamicLabelRecords )
        {
            record.setInUse( false );
        }
        return dynamicLabelRecords;
    }

    private void getAndDeletePropertyChain( NodeRecord nodeRecord )
    {
        context.getAndDeletePropertyChain( nodeRecord );
    }

    /**
     * Removes the given property identified by its index from the relationship
     * with the given id.
     *
     * @param relId The id of the relationship that is to have the property
     * removed.
     * @param propertyKey The index key of the property.
     */
    public void relRemoveProperty( long relId, int propertyKey )
    {
        RecordProxy<Long,RelationshipRecord,Void> rel = context.getRelRecords().getOrLoad( relId, null );
        context.removeProperty( rel, propertyKey );
    }

    /**
     * Removes the given property identified by indexKeyId of the node with the
     * given id.
     *
     * @param nodeId The id of the node that is to have the property removed.
     * @param propertyKey The index key of the property.
     */
    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        RecordProxy<Long,NodeRecord,Void> node = context.getNodeRecords().getOrLoad( nodeId, null );
        context.removeProperty( node, propertyKey );
    }

    /**
     * Changes an existing property's value of the given relationship, with the
     * given index to the passed value
     *
     * @param relId The id of the relationship which holds the property to
     * change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        RecordProxy<Long,RelationshipRecord,Void> rel = context.getRelRecords().getOrLoad( relId, null );
        context.primitiveSetProperty( rel, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     *
     * @param nodeId The id of the node which holds the property to change.
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        RecordProxy<Long,NodeRecord,Void> node = context.getNodeRecords().getOrLoad( nodeId, null ); //getNodeRecord( nodeId );
        context.primitiveSetProperty( node, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     *
     * @param relId The id of the relationship to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        RecordProxy<Long,RelationshipRecord,Void> rel = context.getRelRecords().getOrLoad( relId, null );
        context.primitiveSetProperty( rel, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Adds a property to the given node, with the given index and value.
     *
     * @param nodeId The id of the node to which to add the property.
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        RecordProxy<Long,NodeRecord,Void> node = context.getNodeRecords().getOrLoad( nodeId, null );
        context.primitiveSetProperty( node, propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Creates a node for the given id
     *
     * @param nodeId The id of the node to create.
     */
    public void nodeCreate( long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().create( nodeId, null ).forChangingData();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param key The key of the property index, as a string.
     * @param id The property index record id.
     */
    public void createPropertyKeyToken( String key, int id )
    {
        context.createPropertyKeyToken( key, id );
    }

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param name The key of the property index, as a string.
     * @param id The property index record id.
     */
    public void createLabelToken( String name, int id )
    {
        context.createLabelToken( name, id );
    }

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param name The name of the relationship type.
     * @param id The id of the new relationship type record.
     */
    public void createRelationshipTypeToken( String name, int id )
    {
        context.createRelationshipTypeToken( name, id );
    }

    private static class CommandSorter implements Comparator<Command>
    {
        @Override
        public int compare( Command o1, Command o2 )
        {
            long id1 = o1.getKey();
            long id2 = o2.getKey();
            long diff = id1 - id2;
            if ( diff > Integer.MAX_VALUE )
            {
                return Integer.MAX_VALUE;
            }
            else if ( diff < Integer.MIN_VALUE )
            {
                return Integer.MIN_VALUE;
            }
            else
            {
                return (int) diff;
            }
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof CommandSorter;
        }

        @Override
        public int hashCode()
        {
            return 3217;
        }
    }

    private static final CommandSorter COMMAND_SORTER = new CommandSorter();

    private RecordProxy<Long,NeoStoreRecord,Void> getOrLoadNeoStoreRecord()
    {
        // TODO Move this neo store record thingie into RecordAccessSet
        if ( neoStoreRecord == null )
        {
            neoStoreRecord = new RecordChanges<>( new RecordChanges.Loader<Long,NeoStoreRecord,Void>()
            {
                @Override
                public NeoStoreRecord newUnused( Long key, Void additionalData )
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public NeoStoreRecord load( Long key, Void additionalData )
                {
                    return metaDataStore.asRecord();
                }

                @Override
                public void ensureHeavy( NeoStoreRecord record )
                {
                }

                @Override
                public NeoStoreRecord clone( NeoStoreRecord neoStoreRecord )
                {
                    // We do not expect to manage the before state, so this operation will not be called.
                    throw new UnsupportedOperationException( "Clone on NeoStoreRecord" );
                }
            }, false, new IntCounter() );
        }
        return neoStoreRecord.getOrLoad( 0L, null );
    }

    /**
     * Adds a property to the graph, with the given index and value.
     *
     * @param propertyKey The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        context.primitiveSetProperty( getOrLoadNeoStoreRecord(), propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Changes an existing property of the graph, with the given index to
     * the passed value
     *
     * @param propertyKey The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        context.primitiveSetProperty( getOrLoadNeoStoreRecord(), propertyKey, value );
        return Property.property( propertyKey, value );
    }

    /**
     * Removes the given property identified by indexKeyId of the graph with the
     * given id.
     *
     * @param propertyKey The index key of the property.
     */
    public void graphRemoveProperty( int propertyKey )
    {
        RecordProxy<Long,NeoStoreRecord,Void> recordChange = getOrLoadNeoStoreRecord();
        context.removeProperty( recordChange, propertyKey );
    }

    public void createSchemaRule( SchemaRule schemaRule )
    {
        for ( DynamicRecord change : context.getSchemaRuleChanges().create( schemaRule.getId(), schemaRule ).forChangingData() )
        {
            change.setInUse( true );
            change.setCreated();
        }
    }

    public void dropSchemaRule( SchemaRule rule )
    {
        RecordProxy<Long,Collection<DynamicRecord>,SchemaRule> change = context.getSchemaRuleChanges().getOrLoad( rule.getId(), rule );
        Collection<DynamicRecord> records = change.forChangingData();
        for ( DynamicRecord record : records )
        {
            record.setInUse( false );
        }
    }

    public void addLabelToNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).add( labelId, nodeStore, nodeStore.getDynamicLabelStore() );
    }

    public void removeLabelFromNode( int labelId, long nodeId )
    {
        NodeRecord nodeRecord = context.getNodeRecords().getOrLoad( nodeId, null ).forChangingData();
        parseLabelsField( nodeRecord ).remove( labelId, nodeStore );
    }

    public void setConstraintIndexOwner( IndexRule indexRule, long constraintId )
    {
        RecordProxy<Long,Collection<DynamicRecord>,SchemaRule> change = context.getSchemaRuleChanges().getOrLoad( indexRule.getId(), indexRule );
        Collection<DynamicRecord> records = change.forChangingData();

        indexRule = indexRule.withOwningConstraint( constraintId );

        records.clear();
        records.addAll( schemaStore.allocateFrom( indexRule ) );
    }

    public interface PropertyReceiver
    {
        void receive( DefinedProperty property, long propertyRecordId );
    }
}
