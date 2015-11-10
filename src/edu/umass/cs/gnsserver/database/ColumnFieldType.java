/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.database;

import com.mongodb.DBObject;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Types that a column field can take on.
 * 
 * See also {@link JSONUtils}
 * 
 * SEEMS LIKE THIS COULD PROBABLY ALSO BE DONE 
 * BETTER USING THE JSON LIB.
 * 
 * @author Abhigyan with touchups by Westy.
 */
public enum ColumnFieldType {

  /**
   * Column type that is a Boolean.
   */
  BOOLEAN,

  /**
   * Column type that is a Integer.
   */
  INTEGER,

  /**
   * Column type that is a String.
   */
  STRING,

  /**
   * Column type that is a set of Integers.
   */
  SET_INTEGER,

  /**
   * Column type that is a set of Strings.
   */
  SET_STRING,

  /**
   * Column type that is a NodeID.
   */
  SET_NODE_ID_STRING,

  /**
   * Column type that is a list of Integer.
   */
  LIST_INTEGER,

  /**
   * Column type that is a list of Strings.
   */
  LIST_STRING,

  /**
   * Column type that a map of user values.
   */
  VALUES_MAP,

//  /**
//   * Votes column type
//   */
//  VOTES_MAP,
//
//  /**
//   * Stats storage 
//   */
//  STATS_MAP,

  /**
   * Column type which is a JSON Object.
   */
  USER_JSON // never stored in a system field
  ;

  // 

  /**
   *
   * @param hashMap
   * @param dbObject
   * @param fields
   */
  public static void populateHashMap(HashMap<ColumnField, Object> hashMap, DBObject dbObject, ArrayList<ColumnField> fields) {
//        System.out.println("Object read ---> " +dbObject);
    if (fields != null) {
      for (ColumnField field : fields) {
        Object fieldValue = dbObject.get(field.getName());
        if (fieldValue == null) {
          hashMap.put(field, null);
        } else {
          String value = fieldValue.toString();
          switch (field.type()) {
            case BOOLEAN:
              hashMap.put(field, Boolean.parseBoolean(value));
              break;
            case INTEGER:
              hashMap.put(field, Integer.parseInt(value));
              break;
            case STRING:
              hashMap.put(field, value);
              break;
            case SET_INTEGER:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToSetInteger(new JSONArray(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for SET_INTEGER: " + e);
                e.printStackTrace();
              }
              break;
            case SET_STRING:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToSetString(new JSONArray(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for SET_STRING: " + e);
                e.printStackTrace();
              }
              break;
            case SET_NODE_ID_STRING:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToSetNodeIdString(new JSONArray(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for SET_STRING: " + e);
                e.printStackTrace();
              }
              break;
            case LIST_INTEGER:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToArrayListInteger(new JSONArray(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for LIST_INTEGER: " + e);
                e.printStackTrace();
              }
              break;
            case LIST_STRING:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToArrayListString(new JSONArray(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for LIST_STRING: " + e);
                e.printStackTrace();
              }
              break;
            case VALUES_MAP:
              try {
                hashMap.put(field, new ValuesMap(new JSONObject(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for VALUES_MAP: " + e);
                e.printStackTrace();
              }
              break;
//            case VOTES_MAP:
//              try {
//                hashMap.put(field, JSONUtils.toIntegerMap(new JSONObject(value)));
//              } catch (JSONException e) {
//                GNS.getLogger().severe("Problem populating hash map for VOTES_MAP: " + e);
//                e.printStackTrace();
//              }
//              break;
//            case STATS_MAP:
//              try {
//                hashMap.put(field, JSONUtils.toStatsMap(new JSONObject(value)));
//              } catch (JSONException e) {
//                GNS.getLogger().severe("Problem populating hash map for STATS_MAP: " + e);
//                e.printStackTrace();
//              }
//              break;
            default:
              GNS.getLogger().severe("Exception Error Unknown type " + field + "value = " + value);
              break;
          }
        }
      }
    }
  }
}
