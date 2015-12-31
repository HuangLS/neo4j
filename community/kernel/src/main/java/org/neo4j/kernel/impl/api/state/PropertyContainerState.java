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
package org.neo4j.kernel.impl.api.state;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.DynamicProperty;
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
interface PropertyContainerState
{
    Iterator<DefinedProperty> addedProperties();
    
    /**
     * 
     * @return null means no appended.......
     */
    Iterator<DynamicProperty> appendedDynamicProperties();
    
    Iterator<DynamicProperty> appendDynamicPropertyById( int proId );

    Iterator<DefinedProperty> changedProperties();

    Iterator<Integer> removedProperties();

    Iterator<DefinedProperty> addedAndChangedProperties();

    Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator );

    void accept( Visitor visitor );

    interface Visitor
    {
        void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                   Iterator<DefinedProperty> changed,
                                   Iterator<Integer> removed );
        
        void visiteAppendedDynamicProperty( long entityId, Iterator<DynamicProperty> appended );
    }

    class Mutable implements PropertyContainerState
    {
        
        private Map<Integer, List<DynamicProperty>> appendedDynamicProperties;
        
        private final long id;
        private static final ResourceIterator<DefinedProperty> NO_PROPERTIES = emptyIterator();

        private VersionedHashMap<Integer, DefinedProperty> addedProperties;
        private VersionedHashMap<Integer, DefinedProperty> changedProperties;
        private VersionedHashMap<Integer, DefinedProperty> removedProperties;

        private final Predicate<DefinedProperty> excludePropertiesWeKnowAbout = new Predicate<DefinedProperty>()
        {
            @Override
            public boolean accept( DefinedProperty item )
            {
                return (removedProperties == null || !removedProperties.containsKey( item.propertyKeyId() ))
                       && (addedProperties == null || !addedProperties.containsKey( item.propertyKeyId() ))
                       && (changedProperties == null || !changedProperties.containsKey( item.propertyKeyId() ));
            }
        };

        Mutable( long id )
        {
            this.id = id;
        }

        public long getId()
        {
            return id;
        }

        public void clear()
        {
            if ( null != appendedDynamicProperties ) appendedDynamicProperties.clear();
            if ( changedProperties != null ) changedProperties.clear();
            if ( addedProperties != null ) addedProperties.clear();
            if ( removedProperties != null ) removedProperties.clear();
        }

        public void appendDynamicProperty( DynamicProperty property )
        {
            if( null == this.appendedDynamicProperties )
                this.appendedDynamicProperties = new HashMap<Integer,List<DynamicProperty>>();
            List<DynamicProperty> list = this.appendedDynamicProperties.get( property.propertyKeyId() );
            if( null == list )
            {
                list = new LinkedList<DynamicProperty>();
                this.appendedDynamicProperties.put( property.propertyKeyId(), list );
            }
            list.add(property);
        }
        
        public Iterator<DynamicProperty> appendDynamicPropertyById( int proId )
        {
            return this.appendedDynamicProperties == null ? null : this.appendedDynamicProperties.get( proId ).iterator();
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
        public Iterator<DynamicProperty> appendedDynamicProperties()
        {
            if( null != appendedDynamicProperties )
            {
                List<DynamicProperty> list = new LinkedList<DynamicProperty>();
                for( List<DynamicProperty> l : appendedDynamicProperties.values() )
                {
                    list.addAll( l );
                }
                list.sort( new Comparator<DynamicProperty>()
                {

                    @Override
                    public int compare( DynamicProperty o1, DynamicProperty o2 )
                    {
                        return o1.time() - o2.time();
                    }
                } );
                return list.iterator();
            }
            else
                return null;
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
        public void accept( Visitor visitor )
        {
            if ( addedProperties != null || removedProperties != null || changedProperties != null )
            {
                visitor.visitPropertyChanges( id, addedProperties(), changedProperties(), removedProperties() );
            }
            
            if( null != appendedDynamicProperties )
            {
                List<DynamicProperty> list = new LinkedList<DynamicProperty>();
                for( List<DynamicProperty> l : appendedDynamicProperties.values() )
                {
                    list.addAll( l );
                }
                list.sort( new Comparator<DynamicProperty>()
                {

                    @Override
                    public int compare( DynamicProperty o1, DynamicProperty o2 )
                    {
                        return o1.time() - o2.time();
                    }
                } );
                visitor.visiteAppendedDynamicProperty( id, list.iterator() );
            }
        }
    }
}
