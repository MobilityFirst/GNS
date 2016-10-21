/* Copyright (1c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnscommon.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class JSONDotNotation {

  private static final Logger LOG = Logger.getLogger(JSONDotNotation.class.getName());

  /**
   * @return Logger used by most of the client support package.
   */
  public static final Logger getLogger() {
    return LOG;
  }

  /**
   *
   * @param destination
   * @param key
   * @param value
   * @return true if we were able to put the value
   * @throws JSONException
   */
  public static boolean putWithDotNotation(JSONObject destination, String key, Object value) throws JSONException {
    JSONDotNotation.getLogger().log(Level.FINE, "###fullkey={0} dest={1}", 
            new Object[]{key, destination});
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      JSONDotNotation.getLogger().log(Level.FINE, "###subkey={0}", subKey);
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
      JSONDotNotation.getLogger().log(Level.FINE, "###write={0}->{1}", new Object[]{key, value});
      return true;
    }
  }

  /**
   *
   * @param key
   * @param json
   * @return the value
   * @throws JSONException
   */
  public static Object getWithDotNotation(String key, Object json) throws JSONException {
    JSONDotNotation.getLogger().log(Level.FINE, "CLASS IS " + json.getClass());
    JSONDotNotation.getLogger().log(Level.FINE, "###fullkey={0} json={1}", new Object[]{key, json});
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      JSONDotNotation.getLogger().log(Level.FINE, "###subkey={0}", subKey);

      Object subJSON = ((JSONObject) json).get(subKey);
      if (subJSON == null) {
        JSONDotNotation.getLogger().log(Level.FINE, "### {0} is null", subKey);
        throw new JSONException(subKey + " is null");
      }
      try {
        return getWithDotNotation(key.substring(indexOfDot + 1), subJSON);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = ((JSONObject) json).get(key);
      JSONDotNotation.getLogger().log(Level.FINE, "###result={0}", result);
      return result;
    }
  }
  
  public static Object removeWithDotNotation(String key, Object json) throws JSONException {
    JSONDotNotation.getLogger().log(Level.FINE, "CLASS IS " + json.getClass());
    JSONDotNotation.getLogger().log(Level.FINE, "###fullkey={0} json={1}", new Object[]{key, json});
    if (key.contains(".")) {
      int indexOfDot = key.indexOf(".");
      String subKey = key.substring(0, indexOfDot);
      JSONDotNotation.getLogger().log(Level.FINE, "###subkey={0}", subKey);

      Object subJSON = ((JSONObject) json).get(subKey);
      if (subJSON == null) {
        JSONDotNotation.getLogger().log(Level.FINE, "### {0} is null", subKey);
        throw new JSONException(subKey + " is null");
      }
      try {
        return removeWithDotNotation(key.substring(indexOfDot + 1), subJSON);
      } catch (JSONException e) {
        throw new JSONException(subKey + "." + e.getMessage());
      }
    } else {
      Object result = ((JSONObject) json).remove(key);
      JSONDotNotation.getLogger().log(Level.FINE, "###result={0}", result);
      return result;
    }
  }

  /**
   *
   * @param key
   * @param json
   * @return true if the value is found
   */
  public static boolean containsFieldDotNotation(String key, Object json) {
    try {
      return getWithDotNotation(key, json) != null;
    } catch (JSONException e) {
      return false; // normal negative result when the key isn't present
    }
  }

  // Test Code

  /**
   *
   * @param args
   * @throws JSONException
   */
  public static void main(String[] args) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("name", "frank");
    json.put("occupation", "rocket scientist");
    json.put("location", "work");
    json.put("ip address", "127.0.0.1");
    json.put("friends", new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
    JSONObject subJson = new JSONObject();
    subJson.put("sammy", "green");
    JSONObject subsubJson = new JSONObject();
    subsubJson.put("right", "seven");
    subsubJson.put("left", "eight");
    subJson.put("sally", subsubJson);
    json.put("flapjack", subJson);
    putWithDotNotation(json, "flapjack.sally.right", "crank");
    System.out.println(json);
    putWithDotNotation(json, "flapjack.sammy", new ArrayList<>(Arrays.asList("One", "Ready", "Frap")));
    System.out.println(json);

    System.out.println(getWithDotNotation("flapjack.sally.right", json));

    JSONObject moreJson = new JSONObject();
    moreJson.put("name", "dog");
    moreJson.put("flyer", "shattered");
    moreJson.put("crash", new ArrayList<>(Arrays.asList("Tango", "Sierra", "Alpha")));
    putWithDotNotation(json, "flapjack", moreJson);
    System.out.println(json);

    System.out.println(getWithDotNotation("flapjack.crash", json));
    System.out.println(removeWithDotNotation("flapjack.crash", json));
    
    System.out.println(getWithDotNotation("flapjack", json));

  }

}
