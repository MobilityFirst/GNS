/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

// NEW
//
import edu.umass.cs.gns.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This is the key / value representation for keys and values when we are manipulating them in memory.
 *
 * This is really just a JSONObject, but we made a class for this (as opposed to just using a
 * JSONObject) so we can dispatch off it in methods and also more easily instrument it. This might change soon.
 *
 * Keys are strings and CURRENTLY values are always a list (see also ResultValue), but this
 * restriction will be going away.
 *
 * @author westy
 */
public class ValuesMap {

  private JSONObject newContent;

  public ValuesMap() {
    this.newContent = new JSONObject();
  }

  public ValuesMap(JSONObject json) {
    // makes a fresh JSONObject
    this.newContent = new JSONObject();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      try {
        this.newContent.put(key, json.get(key));
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to parse JSON: " + e);
      }
    }
  }

  public ValuesMap(ValuesMap map) {
    this(map.newContent);
  }

  /**
   * Returns a JSONObject representing this value.
   * 
   * @return a JSONObject 
   */
  public JSONObject toJSONObject() {
    return this.newContent;
  }

  /**
   * Returns a JSONObject that contains just the first element of each JSONArray of the values list.
   * Currently assumes each element of the JSONObject is a key / values[list].
   * Used by the  READONE command.
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
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      json.put(key, newContent.getJSONArray(key).get(0));
    }
  }

  /**
   * Returns the value to which the specified key is mapped, or null if this valuesmap contains no mapping for the key.
   *
   * @param key
   * @return
   */
  public ResultValue get(String key) {
    try {
      if (containsKey(key)) {
        return new ResultValue(JSONUtils.JSONArrayToArrayList(newContent.getJSONArray(key)));
      } else {
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON array: " + e);
      return new ResultValue();
    }
  }

  /**
   * Associates the specified value with the specified key in this valuesmap.
   *
   * @param key
   * @param value
   */
  public void put(String key, ResultValue value) {
    try {
      newContent.put(key, value);
      //GNS.getLogger().severe("@@@@@AFTER PUT (key =" + key + " value=" + value + "): " + newContent.toString());
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to add JSON array to JSON Object: " + e);
    }
  }

  /**
   * Removes the mapping for a key from this valuesmap if it is present.
   *
   * @param key
   */
  public void remove(String key) {
    newContent.remove(key);
  }

  /**
   * Returns true if this valuesmap contains a mapping for the specified key.
   *
   * @param key
   * @return
   */
  public boolean containsKey(String key) {
    return newContent.has(key);
  }

  /**
   * Returns true if this valuesmap contains no key-value mappings.
   *
   * @return
   */
  public boolean isEmpty() {
    return newContent.length() == 0;
  }

  public Set<String> keySet() {
    Set<String> result = new HashSet<String>();
    Iterator<String> keyIter = newContent.keys();
    while (keyIter.hasNext()) {
      result.add(keyIter.next());
    }
    return result;
  }

  public Set<Entry<String, ResultValue>> entrySet() {
    Set<Entry<String, ResultValue>> result = new HashSet<Entry<String, ResultValue>>();
    Iterator<String> keyIter = newContent.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      result.add(new AbstractMap.SimpleImmutableEntry<String, ResultValue>(key, get(key)));
    }
    return result;
  }

  @Override
  public String toString() {
    return newContent.toString();
  }

}
