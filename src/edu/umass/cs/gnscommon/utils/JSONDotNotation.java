/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnscommon.utils;

import edu.umass.cs.gnsserver.main.GNS;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class JSONDotNotation {
  
  private static boolean debuggingEnabled = false;
  
  public static boolean putWithDotNotation(JSONObject destination, String key, Object value) throws JSONException {
    //GNS.getLogger().info("###fullkey=" + key + " dest=" + destination);
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      //GNS.getLogger().info("###subkey=" + subKey);
      Object subDestination = destination.opt(subKey);
      if (subDestination == null) {
        destination.put(subKey, new JSONObject());
        subDestination = destination.get(subKey);
        // FIXME: could also allow JSONArray here if the subkey is in integer
      } else if (!(subDestination instanceof JSONObject)) {
        return false;
      }
      return putWithDotNotation((JSONObject) subDestination, key.substring(indexOfDot + 1), value);
    } else {
      destination.put(key, value);
      //GNS.getLogger().info("###write=" + key + "->" + value);
      return true;
    }
  }

  public static Object getWithDotNotation(String key, Object json) throws JSONException {
    if (debuggingEnabled) {
      GNS.getLogger().info("###fullkey=" + key + " json=" + json);
    }
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      if (debuggingEnabled) {
        GNS.getLogger().info("###subkey=" + subKey);
      }
      Object subBson = ((JSONObject) json).get(subKey);
      if (subBson == null) {
        if (debuggingEnabled) {
          GNS.getLogger().info("### " + subKey + " is null");
        }
        throw new JSONException(subKey + " is null");
      }
      try {
        return getWithDotNotation(key.substring(indexOfDot + 1), subBson);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = ((JSONObject) json).get(key);
      if (debuggingEnabled) {
        GNS.getLogger().info("###result=" + result);
      }
      return result;
    }
  }

  public static boolean containsFieldDotNotation(String key, Object json) {
    try {
      return getWithDotNotation(key, json) != null;
    } catch (JSONException e) {
      return false; // normal negative result when the key isn't present
    }
  }

   public static void main(String[] args) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("name", "frank");
    json.put("occupation", "rocket scientist");
    json.put("location", "work");
    json.put("ip address", "127.0.0.1");
    json.put("friends", new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
    JSONObject subJson = new JSONObject();
    subJson.put("sammy", "green");
    JSONObject subsubJson = new JSONObject();
    subsubJson.put("right", "seven");
    subsubJson.put("left", "eight");
    subJson.put("sally", subsubJson);
    json.put("flapjack", subJson);
    putWithDotNotation(json, "flapjack.sally.right", "crank");
    System.out.println(json);
    putWithDotNotation(json, "flapjack.sammy", new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")));
    System.out.println(json);
    
    System.out.println(getWithDotNotation("flapjack.sally.right", json));
    
    JSONObject moreJson = new JSONObject();
    moreJson.put("name", "dog");
    moreJson.put("flyer", "shattered");
    moreJson.put("crash", new ArrayList<String>(Arrays.asList("Tango", "Sierra", "Alpha")));
    putWithDotNotation(json, "flapjack", moreJson);
    System.out.println(json);

    
    System.out.println(getWithDotNotation("flapjack.crash", json));

  }
  
}
