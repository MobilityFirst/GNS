package edu.umass.cs.gns.util;
/**
@author V. Arun
 */
/* Stringifiable means that ObjectType can be converted
 * to a string and back, similar in spirit to Serializable.
 * As all objects already have a toString() method, we 
 * just need a valueOf method for the reverse conversion.
 */
public interface Stringifiable<ObjectType> {
	public ObjectType valueOf(String strValue); 
}
