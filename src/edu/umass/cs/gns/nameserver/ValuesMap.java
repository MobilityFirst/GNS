/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
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

  private Map<String, ArrayList<String>> content;

  public ValuesMap() {
    this.content = new HashMap<String, ArrayList<String>>();
  }

  public ValuesMap(JSONObject json) throws JSONException {
    this();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      this.content.put(key, new ArrayList<String>(JSONUtils.JSONArrayToArrayList(json.getJSONArray(key))));
    }
  }

  public ValuesMap(ValuesMap map) {
    this.content = new HashMap<String, ArrayList<String>>(map.content);
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  public void addToJSONObject(JSONObject json) throws JSONException {
    for (Map.Entry<String, ArrayList<String>> entry : content.entrySet()) {
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
    for (Map.Entry<String, ArrayList<String>> entry : content.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        json.put(entry.getKey(), entry.getValue().get(0));
      }
    }
  }

  public ArrayList<String> get(String key) {
    return content.get(key);
  }

  public void put(String key, ArrayList<String> value) {
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

  public Set<Entry<String, ArrayList<String>>> entrySet() {
    return content.entrySet();
  }

  public Map getMap() {
    return content;
  }

  @Override
  public String toString() {
    return content.toString();
  }
}
