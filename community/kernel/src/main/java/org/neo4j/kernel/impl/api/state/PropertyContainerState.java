/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Predicate;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.TemporalProperty;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSinglePropertyCursor;
import org.neo4j.kernel.impl.util.VersionedHashMap;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

/**
 * Represents the property changes to a {@link NodeState node} or {@link RelationshipState relationship}:
 * <ul>
 * <li>{@linkplain #addedProperties() Added properties},</li>
 * <li>{@linkplain #removedProperties() removed properties}, and </li>
 * <li>{@linkplain #changedProperties() changed property values}.</li>
 * </ul>
 */
public interface PropertyContainerState
{
    Iterator<DefinedProperty> addedProperties();

    Iterator<DefinedProperty> changedProperties();

    Iterator<Integer> removedProperties();

    Iterator<DefinedProperty> addedAndChangedProperties();

    Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator );

    void accept( Visitor visitor ) throws ConstraintValidationKernelException;

    Cursor<PropertyItem> augmentPropertyCursor( Supplier<TxAllPropertyCursor> propertyCursor,
            Cursor<PropertyItem> cursor );

    Cursor<PropertyItem> augmentSinglePropertyCursor( Supplier<TxSinglePropertyCursor> propertyCursor,
            Cursor<PropertyItem> cursor, int propertyKeyId );

    interface Visitor
    {
        void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed,
                Iterator<Integer> removed ) throws ConstraintValidationKernelException;

        void visitTemporalPropertyChanges(
                long entityId,
                Iterator<TemporalProperty> added, Iterator<TemporalProperty> addedInvalid,
                Iterator<TemporalProperty> deletedRecord, Iterator<Integer> deleted );
    }

    class Mutable implements PropertyContainerState
    {
        private final long id;
        private final EntityType entityType;
        private static final ResourceIterator<DefinedProperty> NO_PROPERTIES = emptyIterator();


        private VersionedHashMap<TemporalPropertyRecordKey, TemporalProperty> addedTemporalPropertyRecords;
        private VersionedHashMap<TemporalPropertyRecordKey, TemporalProperty> addedInvalidTemporalPropertyRecords;
        private VersionedHashMap<TemporalPropertyRecordKey, TemporalProperty> deletedTemporalPropertyRecords;
        private VersionedHashMap<Integer,Byte> deletedTemporalProperties;


        private VersionedHashMap<Integer, DefinedProperty> addedProperties;
        private VersionedHashMap<Integer, DefinedProperty> changedProperties;
        private VersionedHashMap<Integer, DefinedProperty> removedProperties;

        private final Predicate<DefinedProperty> excludePropertiesWeKnowAbout = new Predicate<DefinedProperty>()
        {
            @Override
            public boolean test( DefinedProperty item )
            {
                return (removedProperties == null || !removedProperties.containsKey( item.propertyKeyId() ))
                        && (addedProperties == null || !addedProperties.containsKey( item.propertyKeyId() ))
                        && (changedProperties == null || !changedProperties.containsKey( item.propertyKeyId() ));
            }
        };

        Mutable( long id, EntityType entityType )
        {
            this.id = id;
            this.entityType = entityType;
        }

        public long getId()
        {
            return id;
        }

        public void clear()
        {
            if ( changedProperties != null )
            {
                changedProperties.clear();
            }
            if ( addedProperties != null )
            {
                addedProperties.clear();
            }
            if ( removedProperties != null )
            {
                removedProperties.clear();
            }
        }

        public void changeProperty( DefinedProperty property )
        {
            if ( addedProperties != null )
            {
                if ( addedProperties.containsKey( property.propertyKeyId() ) )
                {
                    addedProperties.put( property.propertyKeyId(), property );
                    return;
                }
            }

            if ( changedProperties == null )
            {
                changedProperties = new VersionedHashMap<>();
            }
            changedProperties.put( property.propertyKeyId(), property );
            if ( removedProperties != null )
            {
                removedProperties.remove( property.propertyKeyId() );
            }
        }

        public void addProperty( DefinedProperty property )
        {
            if ( removedProperties != null )
            {
                DefinedProperty removed = removedProperties.remove( property.propertyKeyId() );
                if ( removed != null )
                {
                    // This indicates the user did remove+add as two discrete steps, which should be translated to
                    // a single change operation.
                    changeProperty( property );
                    return;
                }
            }
            if ( addedProperties == null )
            {
                addedProperties = new VersionedHashMap<>();
            }
            addedProperties.put( property.propertyKeyId(), property );
        }

        public void removeProperty( DefinedProperty property )
        {
            if ( addedProperties != null )
            {
                if ( addedProperties.remove( property.propertyKeyId() ) != null )
                {
                    return;
                }
            }
            if ( removedProperties == null )
            {
                removedProperties = new VersionedHashMap<>();
            }
            removedProperties.put( property.propertyKeyId(), property );
            if ( changedProperties != null )
            {
                changedProperties.remove( property.propertyKeyId() );
            }
        }

        @Override
        public Iterator<DefinedProperty> addedProperties()
        {
            return addedProperties != null ? addedProperties.values().iterator() : NO_PROPERTIES;
        }

        @Override
        public Iterator<DefinedProperty> changedProperties()
        {
            return changedProperties != null ? changedProperties.values().iterator() : NO_PROPERTIES;
        }

        @Override
        public Iterator<Integer> removedProperties()
        {
            return removedProperties != null ? removedProperties.keySet().iterator()
                    : IteratorUtil.<Integer>emptyIterator();
        }

        @Override
        public Iterator<DefinedProperty> addedAndChangedProperties()
        {
            Iterator<DefinedProperty> out = null;
            if ( addedProperties != null )
            {
                out = addedProperties.values().iterator();
            }
            if ( changedProperties != null )
            {
                if ( out != null )
                {
                    out = new CombiningIterator<>(
                            IteratorUtil.iterator( out, changedProperties.values().iterator() ) );
                }
                else
                {
                    out = changedProperties.values().iterator();
                }
            }
            return out != null ? out : NO_PROPERTIES;
        }

        @Override
        public Cursor<PropertyItem> augmentPropertyCursor( Supplier<TxAllPropertyCursor> propertyCursorCache,
                Cursor<PropertyItem> cursor )
        {
            if ( removedProperties != null || addedProperties != null || changedProperties != null )
            {
                return propertyCursorCache.get().init( cursor, addedProperties, changedProperties, removedProperties );
            }
            else
            {
                return cursor;
            }
        }

        @Override
        public Cursor<PropertyItem> augmentSinglePropertyCursor( Supplier<TxSinglePropertyCursor> propertyCursorCache,
                Cursor<PropertyItem> cursor, int propertyKeyId )
        {
            if ( removedProperties != null || addedProperties != null || changedProperties != null )
            {
                return propertyCursorCache.get().init( cursor, addedProperties, changedProperties, removedProperties,
                        propertyKeyId );
            }
            else
            {
                return cursor;
            }
        }

        @Override
        public Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator )
        {
            if ( removedProperties != null || addedProperties != null || changedProperties != null )
            {
                iterator = new FilteringIterator<>( iterator, excludePropertiesWeKnowAbout );

                if ( addedProperties != null && !addedProperties.isEmpty() )
                {
                    iterator = new CombiningIterator<>(
                            IteratorUtil.iterator( iterator, addedProperties.values().iterator() ) );
                }
                if ( changedProperties != null && !changedProperties.isEmpty() )
                {
                    iterator = new CombiningIterator<>(
                            IteratorUtil.iterator( iterator, changedProperties.values().iterator() ) );
                }
            }

            return iterator;
        }

        @Override
        public void accept( Visitor visitor ) throws ConstraintValidationKernelException
        {
            if ( addedProperties != null || removedProperties != null || changedProperties != null )
            {
                visitor.visitPropertyChanges( id, addedProperties(), changedProperties(), removedProperties() );
            }
            if ( addedTemporalPropertyRecords != null || addedInvalidTemporalPropertyRecords != null ||
                    deletedTemporalPropertyRecords != null || deletedTemporalProperties != null){

                Iterator<Integer> delProps;
                Iterator<TemporalProperty> add;
                Iterator<TemporalProperty> invalid;
                Iterator<TemporalProperty> delete;

                if ( addedTemporalPropertyRecords == null){
                    add = emptyIterator();
                }else{
                    add = addedTemporalPropertyRecords.values().iterator();
                }

                if ( addedInvalidTemporalPropertyRecords == null ){
                    invalid = emptyIterator();
                }else{
                    invalid = addedInvalidTemporalPropertyRecords.values().iterator();
                }

                if ( deletedTemporalPropertyRecords == null ){
                    delete = emptyIterator();
                }else{
                    delete = deletedTemporalPropertyRecords.values().iterator();
                }

                if( deletedTemporalProperties == null ){
                    delProps = emptyIterator();
                }else{
                    delProps = deletedTemporalProperties.keySet().iterator();
                }

                visitor.visitTemporalPropertyChanges( id, add, invalid, delete, delProps);
            }
        }

        public void doCreateTemporalPropertyRecord(TemporalProperty temporalProperty)
        {
            TemporalPropertyRecordKey key = new TemporalPropertyRecordKey( temporalProperty.propertyKeyId(), temporalProperty.time() );
            if( null != this.deletedTemporalPropertyRecords )
            {
                this.deletedTemporalPropertyRecords.remove( key );
            }
            if( null == this.addedTemporalPropertyRecords )
            {
                this.addedTemporalPropertyRecords = new VersionedHashMap<>();
            }
            this.addedTemporalPropertyRecords.put( key, temporalProperty );
        }

        public void doCreateTemporalPropertyInvalid(TemporalProperty temporalProperty)
        {
            TemporalPropertyRecordKey key = new TemporalPropertyRecordKey( temporalProperty.propertyKeyId(), temporalProperty.time() );
            if( null != this.deletedTemporalPropertyRecords )
            {
                this.deletedTemporalPropertyRecords.remove( key );
            }
            if( null == this.addedInvalidTemporalPropertyRecords )
            {
                this.addedInvalidTemporalPropertyRecords = new VersionedHashMap<>();
            }
            this.addedInvalidTemporalPropertyRecords.put( key, temporalProperty );
        }

        public void doDeleteTemporalPropertyRecord(TemporalProperty temporalProperty)
        {
            TemporalPropertyRecordKey key = new TemporalPropertyRecordKey( temporalProperty.propertyKeyId(), temporalProperty.time() );
            if( null != this.addedTemporalPropertyRecords )
            {
                this.addedTemporalPropertyRecords.remove(key);
            }
            if( null != this.addedInvalidTemporalPropertyRecords )
            {
                this.addedInvalidTemporalPropertyRecords.remove(key);
            }
            if( null == this.deletedTemporalPropertyRecords )
            {
                this.deletedTemporalPropertyRecords = new VersionedHashMap<>();
            }
            this.deletedTemporalPropertyRecords.put( key, temporalProperty );
        }

        public void doDeleteTemporalProperty(int propertyKeyId)
        {
            List<TemporalPropertyRecordKey> keySet = new LinkedList<>();

            if( null != this.addedTemporalPropertyRecords )
            {
                for( TemporalPropertyRecordKey key : this.addedTemporalPropertyRecords.keySet() )
                {
                    if( key.propertyId == propertyKeyId )
                    {
                        keySet.add(key);
                    }
                }
                for( TemporalPropertyRecordKey key : keySet )
                {
                    this.addedTemporalPropertyRecords.remove( key );
                }
            }

            keySet.clear();

            if( null != this.addedInvalidTemporalPropertyRecords )
            {
                for( TemporalPropertyRecordKey key : this.addedInvalidTemporalPropertyRecords.keySet() )
                {
                    if( key.propertyId == propertyKeyId )
                    {
                        keySet.add(key);
                    }
                }
                for( TemporalPropertyRecordKey key : keySet )
                {
                    this.addedInvalidTemporalPropertyRecords.remove( key );
                }
            }

            if( null == this.deletedTemporalProperties)
            {
                this.deletedTemporalProperties = new VersionedHashMap<>();
            }
            this.deletedTemporalProperties.put( propertyKeyId, null );
        }

        public TemporalProperty getTemporalProperty(int propertyKeyId, int time)
        {
            //Fixme TGraph: Logic Error here, should re-think
            TemporalProperty result = null;
            int closest = -1;
            if(addedTemporalPropertyRecords!=null)
            {
                for (TemporalProperty p : addedTemporalPropertyRecords.values())
                {
                    if (p.propertyKeyId() == propertyKeyId && p.time() > closest && p.time() <= time)
                    {
                        result = p;
                        closest = p.time();
                    }
                }
            }
            if(addedInvalidTemporalPropertyRecords!=null && result!=null)
            {
                for (TemporalProperty pro : addedInvalidTemporalPropertyRecords.values())
                {
                    if (pro.time() > result.time() && pro.time() <= time)
                    {
                        return new TemporalProperty(propertyKeyId, time, 0, new byte[0]);
                    }
                }
            }
            if(deletedTemporalPropertyRecords!=null && result!=null)
            {
                for (TemporalProperty p : deletedTemporalPropertyRecords.values())
                {
                    if (p.time() > result.time() && p.time() <= time)
                    {
                        return new TemporalProperty(propertyKeyId, time, 0, new byte[0]);
                    }
                }
            }
            return result;
        }
    }

    class TemporalPropertyRecordKey
    {
        private int propertyId;
        private int time;

        TemporalPropertyRecordKey(int proId, int t )
        {
            this.propertyId = proId;
            this.time = t;
        }

        public int getPropertyId()
        {
            return propertyId;
        }

        public int getTime()
        {
            return time;
        }

        @Override
        public int hashCode()
        {
            int result = propertyId;
            result = 31 * result + time;
            return result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TemporalPropertyRecordKey that = (TemporalPropertyRecordKey) o;

            return propertyId == that.propertyId && time == that.time;
        }
    }
}
