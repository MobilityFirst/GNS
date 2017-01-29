
package edu.umass.cs.gnsclient.client.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;


public class JSONUtils {


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


  public static Set<String> JSONArrayToSetString(JSONArray json)
          throws JSONException {
    Set<String> set = new HashSet<>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
    }

    return set;
  }

}
