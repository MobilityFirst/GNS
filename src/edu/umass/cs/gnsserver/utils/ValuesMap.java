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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnscommon.utils.JSONDotNotation;
import edu.umass.cs.gnsserver.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Iterator;
import org.json.JSONArray;

/**
 * This is the key / value representation for keys and values when we are manipulating them in memory.
 *
 * This class also has some code that supports backwards compatability with older code. 
 * In particular, in some older code result values are always a list.
 *
 * @author westy
 */
public class ValuesMap extends JSONObject {

  private static boolean debuggingEnabled = false;

  /**
   * Creates an empty ValuesMap.
   */
  public ValuesMap() {
    super();
  }

  /**
   * Creates a ValuesMap from a JSONObject by copying the JSONObject.
   *
   * @param json
   */
  // Might be redundant, but there's no obvious way to copy a JSONObject in the current lib.
  // Makes a fresh JSONObject
  public ValuesMap(JSONObject json) {
    super();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        super.put(key, json.get(key));
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }

  /**
   * Returns a JSONObject that contains just the first element of each JSONArray of the values list.
   * Currently assumes each element of the JSONObject is a key / values[list].
   * Used by the READONE command.
   *
   * @return a JSONObject
   * @throws JSONException
   */
  //
  public JSONObject toJSONObjectFirstOnes() throws JSONException {
    JSONObject json = new JSONObject();
    addFirstOneToJSONObject(json);
    return json;
  }

  private void addFirstOneToJSONObject(JSONObject json) throws JSONException {
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      json.put(key, super.getJSONArray(key).get(0));
    }
  }

  @Override
  /**
   * Returns true if the ValuesMap contains the key.
   * Supports dot notation.
   */
  public boolean has(String key) {
    // if key is "flapjack.sally" this returns true if the map looks like this
    // {"flapjack.sally":{"left":"eight","right":"seven"}}
    // or this
    // {"flapjack":{sally":{"left":"eight","right":"seven"}}}
    return super.has(key) || JSONDotNotation.containsFieldDotNotation(key, this);
  }

  /**
   * Returns the value to which the specified key is mapped as an array,
   * or null if this ValuesMap contains no mapping for the key.
   * Supports dot notation.
   *
   * @param key
   * @return a {@link ResultValue}
   */
  public ResultValue getAsArray(String key) {
    try {
      // handles this case: // {"flapjack.sally":{"left":"eight","right":"seven"}}
      if (super.has(key)) {
        return new ResultValue(JSONUtils.JSONArrayToArrayList(super.getJSONArray(key)));
      }
      // handles this case: // {"flapjack":{sally":{"left":"eight","right":"seven"}}}
      if (JSONDotNotation.containsFieldDotNotation(key, this)) {
        Object object = JSONDotNotation.getWithDotNotation(key, this);
        if (object instanceof JSONArray) {
          return new ResultValue(JSONUtils.JSONArrayToArrayList((JSONArray) object));
        }
        throw new JSONException("JSONObject[" + quote(key) + "] is not a JSONArray.");

        //return new ResultValue(JSONUtils.JSONArrayToArrayList((JSONArray)getWithDotNotation(key, this)));
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON array: " + e);
      e.printStackTrace();
      return new ResultValue();
    }
  }

  /**
   * Associates the specified value which is an array with the specified key in this ValuesMap.
   * Supports dot notation.
   *
   * @param key
   * @param value
   */
  public void putAsArray(String key, ResultValue value) {
    try {
      JSONDotNotation.putWithDotNotation(this, key, new JSONArray(value));
      //super.put(key, value);
      //GNS.getLogger().severe("@@@@@AFTER PUT (key =" + key + " value=" + value + "): " + newContent.toString());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Unable to add JSON array to JSON Object: " + e);
    }
  }

  /**
   * Write the contents of this ValuesMap to the destination ValuesMap.
   *
   * Keys not contained in the source ValuesMap are not altered in the destination ValuesMap.
   * Supports dot notation, that is the keys in the source ValuesMap can be dotted to
   * index subfields in the destination.
   *
   * @param destination
   * @return true if the destination was updated
   */
  public boolean writeToValuesMap(ValuesMap destination) {
    boolean somethingChanged = false;
    Iterator<?> keyIter = keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      try {
        //destination.put(key, super.get(key));
        JSONDotNotation.putWithDotNotation(destination, key, super.get(key));
        somethingChanged = true;
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to write " + key + " field to ValuesMap:" + e);
      }
    }
    return somethingChanged;
  }

}
