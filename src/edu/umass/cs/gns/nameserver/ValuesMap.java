/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This is the key / value representation for keys and values when we are manipulating them in memory.
 * 
 * Keys are strings and values are always a list (see also QueryResultValue).
 * 
 * @author westy
 */
public class ValuesMap {

  private Map<String, QueryResultValue> content;
  
  public ValuesMap() {
    this.content = new HashMap<String, QueryResultValue>();
  }

  public ValuesMap(JSONObject json) throws JSONException {
    this();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      this.content.put(key, new QueryResultValue(JSONUtils.JSONArrayToArrayList(json.getJSONArray(key))));
    }
  }
  
  public ValuesMap(ValuesMap map) {
    this.content = new HashMap<String, QueryResultValue>(map.content);
  }
  
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }
  
  public void addToJSONObject(JSONObject json) throws JSONException {
    for (Map.Entry<String, QueryResultValue> entry : content.entrySet()) {
      json.put(entry.getKey(), new JSONArray(entry.getValue()));
    }
  }
 
  public QueryResultValue get(String key) {
    return content.get(key);
  }
  
  public void put(String key, QueryResultValue value) {
    content.put(key, value);
  }
    public void remove(String key) {
        content.remove(key);
    }

    public boolean containsKey(String key) {
    return content.containsKey(key);
  }
  
  public boolean isEmpty() {
    return content.isEmpty();
  }
  
  public Set<String> keySet() {
    return content.keySet();
  }
  
  public Set<Entry<String, QueryResultValue>> entrySet() {
    return content.entrySet();
  }

  @Override
  public String toString() {
    return content.toString();  
  }
  
  
}
