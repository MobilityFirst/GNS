/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.util;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.nsdesign.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class JSONUtils {

  /**
   *
   * @param jsonArray
   * @return
   * @throws JSONException
   */
  public static ArrayList<Object> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<Object> list = new ArrayList<Object>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.get(i));
    }
    return list;
  }

  public static ArrayList<String> JSONArrayToArrayListString(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList<String>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getString(i));
    }
    return list;
  }

  public static ResultValue JSONArrayToResultValue(JSONArray jsonArray) throws JSONException {
    ResultValue list = new ResultValue();
    for (int i = 0; i < jsonArray.length(); i++) {
      // NOTE THE USE OF GET INSTEAD OF GETSTRING!
      list.add(jsonArray.get(i));
    }
    return list;
  }

  public static HashSet<String> JSONArrayToHashSet(JSONArray jsonArray) throws JSONException {
    HashSet<String> set = new HashSet<String>();
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }

  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getInt(i));
    }
    return list;
  }

  /**
   * **********************************************************
   * Converts a JSONArray to an ArrayList of Integers
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException
   **********************************************************
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
   * **********************************************************
   * Converts a JSONArray to an ArrayList of Integers
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException
   **********************************************************
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
   * Converts a JSONArray to an ArrayList of string addresses
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
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
   * Converts a JSONArray to an set of NodeIds
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException
   */
  public static Set<Object> JSONArrayToSetNodeIdStringOldVersion(JSONArray json) throws JSONException {
    Set<Object> set = new HashSet<Object>();
    if (json == null) {
      return set;
    }
    for (int i = 0; i < json.length(); i++) {
      set.add(json.get(i));
    }

    return set;
  }

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
          return JSONUtils.JSONArrayToSetNodeIdStringOldVersion(jsonObject.getJSONArray(field.getName()));
        case LIST_INTEGER:
          return JSONUtils.JSONArrayToArrayListInteger(jsonObject.getJSONArray(field.getName()));
        case LIST_STRING:
          return JSONUtils.JSONArrayToArrayListString(jsonObject.getJSONArray(field.getName()));
        case VALUES_MAP:
          return new ValuesMap(jsonObject.getJSONObject(field.getName()));
        case VOTES_MAP:
          return toIntegerMap(jsonObject.getJSONObject(field.getName()));
        case STATS_MAP:
          return toStatsMap(jsonObject.getJSONObject(field.getName()));
        default:
          GNS.getLogger().severe("Exception Error ERROR: unknown type: " + field.type());
          break;

      }
    }
    return null;
  }

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
          if (Config.debuggingEnabled) {
            GNS.getLogger().finer("$$$$$$$$$$ Set: " + (set != null ? Util.setOfNodeIdToString(set) : " is null "));
          }
          jsonObject.put(field.getName(), Util.nodeIdSetToStringSet(set));
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
        case VOTES_MAP:
          jsonObject.put(field.getName(), ((ConcurrentMap<Integer, Integer>) value));
          break;
        case STATS_MAP:
          jsonObject.put(field.getName(), ((ConcurrentMap<Integer, StatsInfo>) value));
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
   * *****************************************
   * Utilities for converting maps to JSON objects
   ******************************************
   */
  public static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) {
    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    try {
      Iterator<?> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = (String) nameItr.next();
        map.put(Integer.valueOf(name), json.getInt(name));
      }
    } catch (JSONException e) {
    }
    return new ConcurrentHashMap<Integer, Integer>(map);
  }

  public static ConcurrentHashMap<Integer, StatsInfo> toStatsMap(JSONObject json) { //
    HashMap<Integer, StatsInfo> map = new HashMap<Integer, StatsInfo>();
    try {
      Iterator<?> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = (String) nameItr.next();
        map.put(Integer.valueOf(name), new StatsInfo(json.getJSONObject(name)));
      }
    } catch (JSONException e) {
    }
    return new ConcurrentHashMap<Integer, StatsInfo>(map);
  }

  public JSONObject statsMapToJSONObject(ConcurrentMap<Integer, StatsInfo> map) {
    JSONObject json = new JSONObject();
    try {
      if (map != null) {
        for (Map.Entry<Integer, StatsInfo> e : map.entrySet()) {
          StatsInfo value = e.getValue();
          if (value != null) {
            JSONObject jsonStats = new JSONObject();
            jsonStats.put("read", value.getRead());
            jsonStats.put("write", value.getWrite());
            json.put(e.getKey().toString(), jsonStats);
          }
        }
      }
    } catch (JSONException e) {
    }
    return json;
  }
  
  /* Arun to Westy: A lot of the above methods seem redundant.
   * JSON does a nice job of handling collections and maps.
   */
  public static String toString(Collection<?> collection) {
	  JSONArray jsonArray = new JSONArray(collection);
	  return jsonArray.toString();
  }
  
  public static String[] jsonToStringArray(String jsonString) throws JSONException {
	  JSONArray jsonArray = new JSONArray(jsonString);
	  String[] stringArray = new String[jsonArray.length()];
	  for(int i=0; i<jsonArray.length(); i++) {
		  stringArray[i] = jsonArray.getString(i);
	  }
	  return stringArray;
  }
  
  public static String toString(int[] array) {
	  return toString(Util.arrayOfIntToStringSet(array));
  }
  
  public static void main(String[] args) {
	  Integer[] intArray = {3, 21, 43, 11};
	  Set<Integer> intSet = new HashSet<Integer>(Arrays.asList(intArray));
	  System.out.println(toString(intSet));
	  String[] strArray = {"hello", "world", "one,", ",comma,"};
	  Set<String> strSet = new HashSet<String>(Arrays.asList(strArray));
	  System.out.println(toString(strSet));
	  try {
		for(String s : jsonToStringArray(toString(strSet))) {
			assert(strSet.contains(s));
			System.out.println(s);
		  }
	} catch (JSONException e) {
		e.printStackTrace();
	}
  }
}
