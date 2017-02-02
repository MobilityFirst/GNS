
package edu.umass.cs.gnsserver.utils;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class JSONUtils {


  public static JSONObject copyJsonObject(JSONObject record) throws JSONException {
    JSONObject copy = new JSONObject();
    for (String key : CanonicalJSON.getNames(record)) {
      copy.put(key, record.get(key));
    }
    return copy;
  }


  public static ArrayList<Object> JSONArrayToArrayList(JSONArray jsonArray) throws JSONException {
    ArrayList<Object> list = new ArrayList<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.get(i));
    }
    return list;
  }


  public static ArrayList<String> JSONArrayToArrayListString(JSONArray jsonArray) throws JSONException {
    ArrayList<String> list = new ArrayList<>();
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
    HashSet<String> set = new HashSet<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      set.add(jsonArray.getString(i));
    }
    return set;
  }


  public static ArrayList<Integer> JSONArrayToArrayListInteger(JSONArray jsonArray) throws JSONException {
    ArrayList<Integer> list = new ArrayList<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      list.add(jsonArray.getInt(i));
    }
    return list;
  }


  public static ImmutableSet<Integer> JSONArrayToImmutableSetInteger(JSONArray json) throws JSONException {
    Set<Integer> set = new HashSet<>();

    if (json == null) {
      return ImmutableSet.copyOf(set);
    }

    for (int i = 0; i < json.length(); i++) {
      final Integer integer = new Integer(json.getString(i));
      set.add(integer);
    }

    return ImmutableSet.copyOf(set);
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


  public static Set<String> JSONArrayToSetString(JSONArray json) throws JSONException {
    Set<String> set = new HashSet<>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(json.getString(i));
    }

    return set;
  }


  public static Map<String, ResultValue> JSONObjectToMap(JSONObject json) throws JSONException {
    Map<String, ResultValue> result = new HashMap<>();
    Iterator<?> keyIter = json.keys();
    while (keyIter.hasNext()) {
      String key = (String) keyIter.next();
      result.put(key, new ResultValue(JSONUtils.JSONArrayToResultValue(json.getJSONArray(key))));
    }
    return result;
  }


  public static boolean JSONArrayContains(Object object, JSONArray jsonArray) throws JSONException {
    if (object != null) {
      for (int i = 0; i < jsonArray.length(); i++) {
        if (object.equals(jsonArray.get(i))) {
          return true;
        }
      }
    }
    return false;
  }
}
