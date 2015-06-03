package edu.umass.cs.nio;

import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author V. Arun
 * @param <ObjectType>
 */
/* Stringifiable means that ObjectType can be converted
 * to a string and back, similar in spirit to Serializable.
 * As all objects already have a toString() method, we 
 * just need a valueOf method for the reverse conversion.
 */
public interface Stringifiable<ObjectType> {

  /**
   *
   * Converts a string representation of a node id into the appropriate node id type.
   * Put another way, this performs the reverse of the toString() method for 
   * things that are Stringifiable.
   *
   * @param strValue
   * @return
   */
  public ObjectType valueOf(String strValue);
  
   /**
   * Converts a set of string node ids using valueOf.
   * 
   * @param strNodes
   * @return 
   */
  public Set<ObjectType> getValuesFromStringSet(Set<String> strNodes);
  
  public Set<ObjectType> getValuesFromJSONArray(JSONArray array) throws JSONException;

}
