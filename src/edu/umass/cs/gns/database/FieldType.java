package edu.umass.cs.gns.database;

import com.mongodb.DBObject;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/2/13
 * Time: 1:13 AM
 * To change this template use File | Settings | File Templates.
 */
public enum FieldType {
  BOOLEAN,
  INTEGER,
  STRING,
  SET_INTEGER,
  LIST_INTEGER,
  LIST_STRING,
  VALUES_MAP,
  VOTES_MAP,
  STATS_MAP;
  
  // THIS COULD PROBABLY ALSO BE DONE USING THE JSON LIB.
  /**
   *
   * @param hashMap
   * @param dbObject
   * @param fields
   */
  public static void populateHashMap(HashMap<Field, Object> hashMap, DBObject dbObject, ArrayList<Field> fields) {
//        System.out.println("Object read ---> " +dbObject);
    if (fields != null) {
      for (Field field : fields) {
        Object fieldValue = dbObject.get(field.getName());
        if (fieldValue == null) {
          hashMap.put(field, null); //.add(null);
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
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              }
              break;
            case LIST_INTEGER:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToArrayListInteger(new JSONArray(value)));
              } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              }
              break;
            case LIST_STRING:
              try {
                hashMap.put(field, JSONUtils.JSONArrayToArrayList(new JSONArray(value)));
              } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              }
              break;
            case VALUES_MAP:
              try {
                hashMap.put(field, new ValuesMap(new JSONObject(value)));
              } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              }
              break;
            case VOTES_MAP:
              try {
                hashMap.put(field, JSONUtils.toIntegerMap(new JSONObject(value)));
              } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              }
              break;
            case STATS_MAP:
              try {
                hashMap.put(field, JSONUtils.toStatsMap(new JSONObject(value)));
              } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
