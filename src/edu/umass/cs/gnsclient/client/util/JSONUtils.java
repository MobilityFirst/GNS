/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnsclient.client.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class JSONUtils {
  
   /** org.JSON sucks!!!
   * 
   * @param jsonArray
   * @return
   * @throws JSONException 
   */
  public static ArrayList<String> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList<>();
    //org.JSON sucks!!!
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getString(i));
    }
    return list;
  }
  
  public static HashSet<String> JSONArrayToHashSet(JSONArray jsonArray) throws JSONException {
    HashSet<String> set = new HashSet<>();
    //org.JSON sucks!!!
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }
  
  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList<>();
    //org.JSON sucks!!!
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getInt(i));
    }
    return list;
  }

  /************************************************************
   * Converts a JSONArray to an ArrayList of Integers
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException
   ***********************************************************/
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

  /************************************************************
   * Converts a JSONArray to an ArrayList of string addresses
   * @param json JSONArray
   * @return ArrayList with the content of JSONArray.
   * @throws JSONException 
   ***********************************************************/
  public static Set<String> JSONArraytToSetAddress(JSONArray json) throws JSONException {
    Set<String> set = new HashSet<String>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
    }

    return set;
  }
  
  public static Set<String> JSONArrayToSetString(JSONArray json)
          throws JSONException {
    Set<String> set = new HashSet<String>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
    }

    return set;
  }
  
//  /**
//   * Returns a JSON Object string sorted by keys. 
//   * This is only canonical one level deep. You've been warned.
//   * @param json
//   * @return 
//   */
//  public static String getCanonicalJSONString(JSONObject json) {
//    SortedMap map = new TreeMap<String, Object>();
//    Iterator<String> nameItr = json.keys();
//    while (nameItr.hasNext()) {
//      String key = nameItr.next();
//      try {
//        map.put(key, json.get(key));
//      } catch (JSONException e) {
//        // punt on any fields that hose us
//      }
//    }
//    return map.toString();
//  }

}
