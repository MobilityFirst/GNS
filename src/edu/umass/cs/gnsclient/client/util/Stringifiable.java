/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util;

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
