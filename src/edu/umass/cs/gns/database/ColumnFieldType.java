package edu.umass.cs.gns.database;

import com.mongodb.DBObject;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/2/13
 * Time: 1:13 AM
 * To change this template use File | Settings | File Templates.
 */
public enum ColumnFieldType {

  BOOLEAN,
  INTEGER,
  STRING,
  SET_INTEGER,
  SET_STRING,
  SET_NODE_ID_STRING,
  LIST_INTEGER,
  LIST_STRING,
  VALUES_MAP,
  VOTES_MAP,
  STATS_MAP,
  USER_JSON // NOT FULLY SUPPORTED YET
  ;

  // THIS COULD PROBABLY ALSO BE DONE USING THE JSON LIB.

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
            case VOTES_MAP:
              try {
                hashMap.put(field, JSONUtils.toIntegerMap(new JSONObject(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for VOTES_MAP: " + e);
                e.printStackTrace();
              }
              break;
            case STATS_MAP:
              try {
                hashMap.put(field, JSONUtils.toStatsMap(new JSONObject(value)));
              } catch (JSONException e) {
                GNS.getLogger().severe("Problem populating hash map for STATS_MAP: " + e);
                e.printStackTrace();
              }
              break;
            default:
              GNS.getLogger().severe("Exception Error Unknown type " + field + "value = " + value);
              break;
          }
        }
      }
    }
  }
}
