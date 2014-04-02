/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.util;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.main.GNS;

import java.util.*;
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
    ArrayList<Object> list = new ArrayList();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.get(i));
    }
    return list;
  }
  
  public static ArrayList<String> JSONArrayToArrayListString(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList();
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
    HashSet<String> set = new HashSet();
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }

  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList();
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
   * **********************************************************
   * Converts a JSONArray to an ArrayList of string addresses
   *
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException 
   **********************************************************
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

  public static Map<String, ResultValue> JSONObjectToMap(JSONObject json) throws JSONException {
    Map<String, ResultValue> result = new HashMap<String, ResultValue>();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      result.put(key, new ResultValue(JSONUtils.JSONArrayToResultValue(json.getJSONArray(key))));
    }
    return result;
  }


  public static Object getObject(ColumnField field, JSONObject jsonObject) throws JSONException{
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


//  public static String getString(Field field, JSONObject jsonObject) throws JSONException{
//    if (jsonObject.has(field.getFieldName())) {
//      switch (field.type()) {
//        case INTEGER:
//          return jsonObject.getInt(field.getFieldName());
//        case STRING:
//          return jsonObject.getString(field.getFieldName());
//        case SET_INTEGER:
//          return JSONUtils.JSONArrayToSetInteger(jsonObject.getJSONArray(field.getFieldName()));
//        case LIST_STRING:
//          return JSONUtils.JSONArrayToArrayList(jsonObject.getJSONArray(field.getFieldName()));
//        case MAP:
//          return new ValuesMap(jsonObject.getJSONObject(field.getFieldName()));
//      }
//    }
//    return null;
//  }


  public static void putFieldInJsonObject(ColumnField field, Object value, JSONObject jsonObject) throws JSONException {
    try{
    if (value == null) return;
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
        jsonObject.put(field.getName(), (Set<Integer>)value);
        break;
      case LIST_INTEGER:
        jsonObject.put(field.getName(), (ArrayList<Integer>)value);
        break;
      case LIST_STRING:
        jsonObject.put(field.getName(), (ArrayList<String>)value);
        break;
      case VALUES_MAP:
        jsonObject.put(field.getName(), ((ValuesMap)value).toJSONObject());
        break;
      case VOTES_MAP:
        jsonObject.put(field.getName(), ((ConcurrentMap<Integer,Integer>)value));
        break;
      case STATS_MAP:
        jsonObject.put(field.getName(), ((ConcurrentMap<Integer,StatsInfo>)value));
        break;
      default:
        GNS.getLogger().severe("Exception Error ERROR: unknown type: " + field.type());
        break;
    }
    }catch (Exception e) {
      GNS.getLogger().fine(" Value " + value + " Field = " + field);
      e.printStackTrace();
    }
  }



  /*******************************************
   *  Utilities for converting maps to JSON objects
   *******************************************/


  public static ConcurrentHashMap<Integer, Integer> toIntegerMap(JSONObject json) {
    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    try {
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
        map.put(Integer.valueOf(name), json.getInt(name));
      }
    } catch (JSONException e) {
    }
    return new ConcurrentHashMap<Integer, Integer>(map);
  }

  public static ConcurrentHashMap<Integer, StatsInfo> toStatsMap(JSONObject json) { //
    HashMap<Integer, StatsInfo> map = new HashMap<Integer, StatsInfo>();
    try {
      Iterator<String> nameItr = json.keys();
      while (nameItr.hasNext()) {
        String name = nameItr.next();
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

}
