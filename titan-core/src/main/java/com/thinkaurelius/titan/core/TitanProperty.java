
package com.thinkaurelius.titan.core;

/**
 * TitanProperty is a {@link TitanRelation} connecting a vertex to a value.
 * TitanProperty extends {@link TitanRelation}, with methods for retrieving the property's value and key.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see TitanRelation
 * @see PropertyKey
 */
public interface TitanProperty extends TitanRelation {

    /**
     * Returns the property key of this property
     *
     * @return property key of this property
     * @see PropertyKey
     */
    public PropertyKey getPropertyKey();


    /**
     * Returns the vertex on which this property is incident.
     *
     * @return The vertex of this property.
     */
    public TitanVertex getVertex();

    /**
     * Returns the value of this property.
     *
     * @return value of this property
     */
    public Object getValue();

    /**
     * Returns the value of this property cast to the specified class.
     *
     * @param <O>   Class to cast the value to
     * @param clazz Class to cast the value to
     * @return Value of this property cast to the specified class.
     * @throws ClassCastException if the value cannot be cast to clazz.
     */
    public <O> O getValue(Class<O> clazz);


}
