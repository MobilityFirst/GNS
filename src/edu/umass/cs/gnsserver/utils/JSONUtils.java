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
public class JSONUtils {

  /**
   * Make a copy of a JSON Object.
   * Much better than new JSONObject(record.toString()).
   *
   * @param record
   * @return a JSON Object
   * @throws JSONException
   */
  public static JSONObject copyJsonObject(JSONObject record) throws JSONException {
    JSONObject copy = new JSONObject();
    for (String key : JSONObject.getNames(record)) {
      copy.put(key, record.get(key));
    }
    return copy;
  }

  /**
   * Converts a JSON array to an ArrayList of Objects.
   *
   * @param jsonArray
   * @return an ArrayList of Objects
   * @throws JSONException
   */
  public static ArrayList<Object> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<Object> list = new ArrayList<>();
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
    ArrayList<String> list = new ArrayList<>();
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
    HashSet<String> set = new HashSet<>();
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
    ArrayList<Integer> list = new ArrayList<>();
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
    Set<Integer> set = new HashSet<>();

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
    Set<Integer> set = new HashSet<>();

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
    Set<String> set = new HashSet<>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
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
    Map<String, ResultValue> result = new HashMap<>();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      result.put(key, new ResultValue(JSONUtils.JSONArrayToResultValue(json.getJSONArray(key))));
    }
    return result;
  }

  /**
   * Returns true if the JSON Array contains the object.
   * Returns false if the object is null or the array is empty.
   *
   * @param jsonArray
   * @return an ArrayList of Objects
   * @throws JSONException
   */
  public static boolean JSONArrayContains(Object object, JSONArray jsonArray) throws JSONException {
    if (object != null) {
      for (int i = 0; i < jsonArray.length(); i++) {
        if (object.equals(jsonArray.get(i))) {
          return true;
        }
      }
    }
    return false;
  }
}
