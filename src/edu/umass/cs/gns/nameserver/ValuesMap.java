/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This is the key / value representation for keys and values when we are manipulating them in memory.
 *
 * Keys are strings and values are always a list (see also QueryResultValue).
 *
 * @author westy
 */
public class ValuesMap {

  private Map<String, ResultValue> content;
  // Instrumentation
  private long roundTripTime; // how long this query took

  public ValuesMap() {
    this.content = new HashMap<String, ResultValue>();
  }

  public ValuesMap(JSONObject json) throws JSONException {
    this();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      this.content.put(key, new ResultValue(JSONUtils.JSONArrayToResultValue(json.getJSONArray(key))));
    }
  }

  public ValuesMap(ValuesMap map) {
    this.content = new HashMap<String, ResultValue>(map.content);
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  public void addToJSONObject(JSONObject json) throws JSONException {
    for (Map.Entry<String, ResultValue> entry : content.entrySet()) {
      json.put(entry.getKey(), new JSONArray(entry.getValue()));
    }
  }

  // For the READONE command we just pull out the first item in each value of the key / values[list]
  public JSONObject toJSONObjectFirstOnes() throws JSONException {
    JSONObject json = new JSONObject();
    addFirstOneToJSONObject(json);
    return json;
  }

  public void addFirstOneToJSONObject(JSONObject json) throws JSONException {
    for (Map.Entry<String, ResultValue> entry : content.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        json.put(entry.getKey(), entry.getValue().get(0));
      }
    }
  }

  /**
   * Returns the value to which the specified key is mapped, or null if this valuesmap contains no mapping for the key.
   * 
   * @param key
   * @return 
   */
  public ResultValue get(String key) {
    return content.get(key);
  }

  /**
   * Associates the specified value with the specified key in this valuesmap.
   * @param key
   * @param value 
   */
  public void put(String key, ResultValue value) {
    content.put(key, value);
  }

  /**
   * Removes the mapping for a key from this valuesmap if it is present.
   * 
   * @param key 
   */
  public void remove(String key) {
    content.remove(key);
  }

  /**
   * Returns true if this valuesmap contains a mapping for the specified key.
   * 
   * @param key
   * @return 
   */
  public boolean containsKey(String key) {
    return content.containsKey(key);
  }

  /**
   * Returns true if this valuesmap contains no key-value mappings.
   * @return 
   */
  public boolean isEmpty() {
    return content.isEmpty();
  }

  public Set<String> keySet() {
    return content.keySet();
  }

  public Set<Entry<String, ResultValue>> entrySet() {
    return content.entrySet();
  }

  public Map getMap() {
    return content;
  }

  @Override
  public String toString() {
    return content.toString();
  }

  public long getRoundTripTime() {
    return roundTripTime;
  }

  public void setRoundTripTime(long roundTripTime) {
    this.roundTripTime = roundTripTime;
  }
  
}
