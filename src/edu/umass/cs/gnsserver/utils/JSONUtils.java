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

import com.google.common.collect.ImmutableSet;

import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.main.GNS;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Utilities to help convert to and from JSON.
 */
// FIXME: Clean these up and remove redundant ones.
public class JSONUtils {

  /**
   * Converts a JSON array to an ArrayList of Objects.
   *
   * @param jsonArray
   * @return an ArrayList of Objects
   * @throws JSONException
   */
  public static ArrayList<Object> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<Object> list = new ArrayList<Object>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.get(i));
    }
    return list;
  }

  /**
   * Converts a JSON array to an ArrayList of Strings.
   *
   * @param jsonArray
   * @return an ArrayList of Strings
   * @throws JSONException
   */
  public static ArrayList<String> JSONArrayToArrayListString(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList<String>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getString(i));
    }
    return list;
  }

  /**
   * Converts a JSON array to a {@link ResultValue}.
   *
   * @param jsonArray
   * @return a ResultValue
   * @throws JSONException
   */
  public static ResultValue JSONArrayToResultValue(JSONArray jsonArray) throws JSONException {
    ResultValue list = new ResultValue();
    for (int i = 0; i < jsonArray.length(); i++) {
      // NOTE THE USE OF GET INSTEAD OF GETSTRING!
      list.add(jsonArray.get(i));
    }
    return list;
  }

  /**
   * Converts a JSON array to a HashSet.
   *
   * @param jsonArray
   * @return a HashSet
   * @throws JSONException
   */
  public static HashSet<String> JSONArrayToHashSet(JSONArray jsonArray) throws JSONException {
    HashSet<String> set = new HashSet<String>();
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }

  /**
   * Converts a JSON array to an ArrayList of Integers.
   *
   * @param jsonArray
   * @return an ArrayList of Integers
   * @throws JSONException
   */
  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getInt(i));
    }
    return list;
  }

  /**
   * Converts a JSONArray to an immutable set of Integers.
   *
   * @param json JSONArray
   * @return ArrayList set of Integers
   * @throws JSONException
   */
  public static ImmutableSet<Integer> JSONArrayToImmutableSetInteger(JSONArray json) throws JSONException {
    Set<Integer> set = new HashSet<Integer>();

    if (json == null) {
      return ImmutableSet.copyOf(set);
    }

    for (int i = 0; i < json.length(); i++) {
      final Integer integer = new Integer(json.getString(i));
      set.add(integer);
    }

    return ImmutableSet.copyOf(set);
  }

  /**
   * Converts a JSONArray to Set of Integers.
   *
   * @param json JSONArray
   * @return Set with the contents of JSONArray.
   * @throws JSONException
   */
  public static Set<Integer> JSONArrayToSetInteger(JSONArray json) throws JSONException {
    Set<Integer> set = new HashSet<Integer>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(new Integer(json.getString(i)));
    }

    return set;
  }

  /**
   * Converts a JSONArray to a set of Strings.
   *
   * @param json JSONArray
   * @return a set of strings
   * @throws JSONException
   */
  public static Set<String> JSONArrayToSetString(JSONArray json) throws JSONException {
    Set<String> set = new HashSet<String>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
    }

    return set;
  }

  /**
   * Converts a JSONArray to an set of NodeIds.
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException
   */
  public static Set<Object> JSONArrayToSetNodeIdString(JSONArray json) throws JSONException {
    Set<Object> set = new HashSet<Object>();
    if (json == null) {
      return set;
    }
    for (int i = 0; i < json.length(); i++) {
      set.add(json.get(i));
    }

    return set;
  }

  /**
   * Converts a JSONObject to a map of strings to {@link ResultValue}.
   *
   * @param json
   * @return map of string to {@link ResultValue}
   * @throws JSONException
   */
  public static Map<String, ResultValue> JSONObjectToMap(JSONObject json) throws JSONException {
    Map<String, ResultValue> result = new HashMap<String, ResultValue>();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      result.put(key, new ResultValue(JSONUtils.JSONArrayToResultValue(json.getJSONArray(key))));
    }
    return result;
  }

  // This code is an abomination...
  /**
   * Extracts the column field from the JSONObject.
   *
   * @param field
   * @param jsonObject
   * @return the value of the field
   * @throws JSONException
   */
  public static Object getObject(ColumnField field, JSONObject jsonObject) throws JSONException {
    if (jsonObject.has(field.getName())) {
      switch (field.type()) {
        case BOOLEAN:
          return jsonObject.getBoolean(field.getName());
        case INTEGER:
          return jsonObject.getInt(field.getName());
        case STRING:
          return jsonObject.getString(field.getName());
        case SET_INTEGER:
          return JSONUtils.JSONArrayToSetInteger(jsonObject.getJSONArray(field.getName()));
        case SET_STRING:
          return JSONUtils.JSONArrayToSetString(jsonObject.getJSONArray(field.getName()));
        case SET_NODE_ID_STRING:
          return JSONUtils.JSONArrayToSetNodeIdString(jsonObject.getJSONArray(field.getName()));
        case LIST_INTEGER:
          return JSONUtils.JSONArrayToArrayListInteger(jsonObject.getJSONArray(field.getName()));
        case LIST_STRING:
          return JSONUtils.JSONArrayToArrayListString(jsonObject.getJSONArray(field.getName()));
        case VALUES_MAP:
          return new ValuesMap(jsonObject.getJSONObject(field.getName()));
        default:
          GNS.getLogger().severe("Exception Error ERROR: unknown type: " + field.type());
          break;

      }
    }
    return null;
  }

  /**
   * Inserts a column field value into a JSONObject.
   *
   * @param field
   * @param value
   * @param jsonObject
   * @throws JSONException
   */
  @SuppressWarnings("unchecked") // because we assume the field types get it right
  public static void putFieldInJsonObject(ColumnField field, Object value, JSONObject jsonObject) throws JSONException {
    try {
      if (value == null) {
        return;
      }
      switch (field.type()) {
        case BOOLEAN:
          jsonObject.put(field.getName(), value);
          break;
        case INTEGER:
          jsonObject.put(field.getName(), value);
          break;
        case STRING:
          jsonObject.put(field.getName(), value);
          break;
        case SET_INTEGER:
          jsonObject.put(field.getName(), (Set<Integer>) value);
          break;
        case SET_STRING:
          jsonObject.put(field.getName(), (Set<String>) value);
          break;
        case SET_NODE_ID_STRING:
          Set set = (Set) value;
          jsonObject.put(field.getName(), nodeIdSetToStringSet(set));
          break;
        case LIST_INTEGER:
          jsonObject.put(field.getName(), (ArrayList<Integer>) value);
          break;
        case LIST_STRING:
          jsonObject.put(field.getName(), (ArrayList<String>) value);
          break;
        case VALUES_MAP:
          jsonObject.put(field.getName(), ((ValuesMap) value));
          break;
        default:
          GNS.getLogger().severe("Exception Error ERROR: unknown type: " + field.type());
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem putting field in JSON Object: Value = " + value + " Field = " + field);
      e.printStackTrace();
    }
  }

  /**
   *
   * @param set
   * @return a set of strings
   */
  public static Set<String> nodeIdSetToStringSet(Set set) {
    Set<String> result = new HashSet<String>();
    for (Object id : set) {
      result.add(id.toString());
    }
    return result;
  }
}
