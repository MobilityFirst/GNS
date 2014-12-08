package edu.umass.cs.gns.util;
/**
@author V. Arun
 * @param <ObjectType>
 */
/* Stringifiable means that ObjectType can be converted
 * to a string and back, similar in spirit to Serializable.
 * As all objects already have a toString() method, we 
 * just need a valueOf method for the reverse conversion.
 */
public interface Stringifiable<ObjectType> {
        /**
         * Converts the string value back to the ObjectType. Performs the
         * reverse of the toString() method for things that are Stringifiable.
         * @param strValue
         * @return 
         */
	public ObjectType valueOf(String strValue); 
}
