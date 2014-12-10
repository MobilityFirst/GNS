package edu.umass.cs.gns.util;

import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * Adds additional useful functionality to the Stringifiable class.
 * Namely the ability to get values from collections.
 * 
 * @author westy
 * @param <ObjectType> 
 */
public interface StringifiableValues<ObjectType> extends Stringifiable<ObjectType> {
  
  /**
   * Converts a set of string node ids using valueOf.
   * 
   * @param strNodes
   * @return 
   */
  public Set<ObjectType> getValuesFromStringSet(Set<String> strNodes);
  
  public Set<ObjectType> getValuesFromJSONArray(JSONArray array) throws JSONException;
	
}
