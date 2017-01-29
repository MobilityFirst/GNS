
package edu.umass.cs.gnsclient.client.util;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.Set;



public interface Stringifiable<ObjectType> {


  public ObjectType valueOf(String strValue);
  

  public Set<ObjectType> getValuesFromStringSet(Set<String> strNodes);
  

  public Set<ObjectType> getValuesFromJSONArray(JSONArray array) throws JSONException;

}
