/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.util;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nameserver.fields.Field;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

//import edu.umass.cs.gns.packet.QueryResultValue;

/**
 *
 * @author westy
 */
public class JSONUtils {

  /**
   * org.JSON sucks!!!
   *
   * @param jsonArray
   * @return
   * @throws JSONException
   */
  public static ArrayList<String> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList();
    //org.JSON sucks!!!
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getString(i));
    }
    return list;
  }

  public static HashSet<String> JSONArrayToHashSet(JSONArray jsonArray) throws JSONException {
    HashSet<String> set = new HashSet();
    //org.JSON sucks!!!
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }

  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList();
    //org.JSON sucks!!!
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

  public static Map<String, ArrayList<String>> JSONObjectToMap(JSONObject json) throws JSONException {
    Map<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();
    Iterator<String> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      result.put(key, new ArrayList<String>(JSONUtils.JSONArrayToArrayList(json.getJSONArray(key))));
    }
    return result;
  }


  public static Object getObject(Field field, JSONObject jsonObject) throws JSONException{
    if (jsonObject.has(field.getFieldName())) {
      switch (field.type()) {
        case INTEGER:
          return jsonObject.getInt(field.getFieldName());
        case STRING:
          return jsonObject.getString(field.getFieldName());
        case SET_INTEGER:
          return JSONUtils.JSONArrayToSetInteger(jsonObject.getJSONArray(field.getFieldName()));
        case LIST_STRING:
          return JSONUtils.JSONArrayToArrayList(jsonObject.getJSONArray(field.getFieldName()));
        case MAP:
          return new ValuesMap(jsonObject.getJSONObject(field.getFieldName()));
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


  public static void putFieldInJsonObject(Field field, Object value, JSONObject jsonObject) throws JSONException {
    if (value == null) return;
    switch (field.type()) {

      case INTEGER:
        jsonObject.put(field.getFieldName(), (Integer)value);
        break;
      case STRING:
        jsonObject.put(field.getFieldName(), (String)value);
        break;
      case SET_INTEGER:
        jsonObject.put(field.getFieldName(), (Set<Integer>)value);
        break;
      case LIST_STRING:
        jsonObject.put(field.getFieldName(), (ArrayList<String>)value);
        break;
      case MAP:
        jsonObject.put(field.getFieldName(), ((ValuesMap)value).toJSONObject());
        break;

    }

  }
}
