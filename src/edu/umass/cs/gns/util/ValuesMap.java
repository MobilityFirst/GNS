/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

// NEW
//
import edu.umass.cs.gns.main.GNS;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Iterator;

/**
 * This is the key / value representation for keys and values when we are manipulating them in memory.
 *
 * We maintain this class for backwards compatability with older code. In particular, in some older
 * code values are always a list (a ResultValue).
 *
 * @author westy
 */
public class ValuesMap extends JSONObject {

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
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
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
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      json.put(key, super.getJSONArray(key).get(0));
    }
  }

  /**
   * Returns the value to which the specified key is mapped as an array,
   * or null if this ValuesMap contains no mapping for the key.
   *
   * @param key
   * @return
   */
  public ResultValue getAsArray(String key) {
    try {
      if (has(key)) {
        return new ResultValue(JSONUtils.JSONArrayToArrayList(super.getJSONArray(key)));
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
   * Associates the specified value with the specified key in this ValuesMap.
   *
   * @param key
   * @param value
   */
  public void putAsArray(String key, ResultValue value) {
    try {
      super.put(key, value);
      //GNS.getLogger().severe("@@@@@AFTER PUT (key =" + key + " value=" + value + "): " + newContent.toString());
    } catch (JSONException e) {
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
   * @return 
   */
  public boolean writeToValuesMap(ValuesMap destination) {
    boolean somethingChanged = false;
    Iterator<String> keyIter = keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      try {
        //destination.put(key, super.get(key));
        putWithDotNotation(destination, key, super.get(key));
        somethingChanged = true;
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to write " + key + " field to ValuesMap:" + e);
      }
    }
    return somethingChanged;
  }

  private static boolean putWithDotNotation(Object destination, String key, Object value) {
    try {
      //GNS.getLogger().info("###fullkey=" + key + " dest=" + destination);
      if (key.contains(".")) {
        int indexOfDot = key.indexOf(".");
        String subKey = key.substring(0, indexOfDot);
        //GNS.getLogger().info("###subkey=" + subKey);
        Object subDestination = ((JSONObject)destination).get(subKey);
        if (subDestination == null) {
          return false;
        }
        return putWithDotNotation(subDestination, key.substring(indexOfDot + 1), value);
      } else {
        ((JSONObject)destination).put(key, value);
        //GNS.getLogger().info("###write=" + key + "->" + value);
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to write " + key + " field to ValuesMap:" + e);
      return false;
    }
  }

  // Test Code
  
  public static void main(String[] args) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("name", "frank");
    json.put("occupation", "rocket scientist");
    json.put("location", "work");
    json.put("ip address", "127.0.0.1");
    json.put("friends", new ArrayList(Arrays.asList("Joe", "Sam", "Billy")));
    JSONObject subJson = new JSONObject();
    subJson.put("sammy", "green");
    JSONObject subsubJson = new JSONObject();
    subsubJson.put("right", "seven");
    subsubJson.put("left", "eight");
    subJson.put("sally", subsubJson);
    json.put("flapjack", subJson);
    ValuesMap valuesMap = new ValuesMap(json);
    putWithDotNotation(valuesMap, "flapjack.sally.right", "crank");
    System.out.println(valuesMap);
    putWithDotNotation(valuesMap, "flapjack.sammy", new ArrayList(Arrays.asList("One", "Ready", "Frap")));
    System.out.println(valuesMap);
    JSONObject moreJson = new JSONObject();
    moreJson.put("name", "dog");
    moreJson.put("flyer", "shattered");
    moreJson.put("crash", new ArrayList(Arrays.asList("Tango", "Sierra", "Alpha")));
    putWithDotNotation(valuesMap, "flapjack", moreJson);
    System.out.println(valuesMap);
  }

}
