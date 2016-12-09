package edu.umass.cs.gnsclient.jsonassert.comparator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * Utility class that contains Json manipulation methods
 */
public final class JSONCompareUtil {
    private static final Integer INTEGER_ONE = 1;

    private JSONCompareUtil() {}

  /**
   *
   * @param array
   * @param uniqueKey
   * @return object map
   * @throws JSONException
   */
  public static Map<Object,JSONObject> arrayOfJsonObjectToMap(JSONArray array, String uniqueKey) throws JSONException {
        Map<Object, JSONObject> valueMap = new HashMap<>();
        for(int i = 0 ; i < array.length() ; ++i) {
            JSONObject jsonObject = (JSONObject)array.get(i);
            Object id = jsonObject.get(uniqueKey);
            valueMap.put(id, jsonObject);
        }
        return valueMap;
    }

  /**
   *
   * @param expected
   * @return the key
   * @throws JSONException
   */
  public static String findUniqueKey(JSONArray expected) throws JSONException {
        // Find a unique key for the object (id, name, whatever)
        JSONObject o = (JSONObject)expected.get(0); // There's at least one at this point
        for(String candidate : getKeys(o)) {
            if (isUsableAsUniqueKey(candidate, expected)) return candidate;
        }
        // No usable unique key :-(
        return null;
    }

    /**
     * {@code candidate} is usable as a unique key if every element in the
     * {@code array} is a JSONObject having that key, and no two values are the same.
   * @param candidate
   * @param array
   * @return true if candidate is usable as a unique key
   * @throws org.json.JSONException
     */
    public static boolean isUsableAsUniqueKey(String candidate, JSONArray array) throws JSONException {
        Set<Object> seenValues = new HashSet<Object>();
        for (int i = 0 ; i < array.length() ; i++) {
            Object item = array.get(i);
            if (item instanceof JSONObject) {
                JSONObject o = (JSONObject) item;
                if (o.has(candidate)) {
                    Object value = o.get(candidate);
                    if (isSimpleValue(value) && !seenValues.contains(value)) {
                        seenValues.add(value);
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

  /**
   *
   * @param expected
   * @return the list of objects
   * @throws JSONException
   */
  public static List<Object> jsonArrayToList(JSONArray expected) throws JSONException {
        List<Object> jsonObjects = new ArrayList<Object>(expected.length());
        for(int i = 0 ; i < expected.length() ; ++i) {
            jsonObjects.add(expected.get(i));
        }
        return jsonObjects;
    }

  /**
   *
   * @param array
   * @return true if all values are simple
   * @throws JSONException
   */
  public static boolean allSimpleValues(JSONArray array) throws JSONException {
        for(int i = 0 ; i < array.length() ; ++i) {
            if (!isSimpleValue(array.get(i))) {
                return false;
            }
        }
        return true;
    }

  /**
   *
   * @param o
   * @return true if it is a simple value
   */
  public static boolean isSimpleValue(Object o) {
        return !(o instanceof JSONObject) && !(o instanceof JSONArray);
    }

  /**
   *
   * @param array
   * @return true if all values are JSONObjects
   * @throws JSONException
   */
  public static boolean allJSONObjects(JSONArray array) throws JSONException {
        for(int i = 0 ; i < array.length() ; ++i) {
            if (!(array.get(i) instanceof JSONObject)) {
                return false;
            }
        }
        return true;
    }

  /**
   *
   * @param array
   * @return true if all values are JSONArrays
   * @throws JSONException
   */
  public static boolean allJSONArrays(JSONArray array) throws JSONException {
        for(int i = 0 ; i < array.length() ; ++i) {
            if (!(array.get(i) instanceof JSONArray)) {
                return false;
            }
        }
        return true;
    }

  /**
   *
   * @param jsonObject
   * @return the keys
   */
  public static Set<String> getKeys(JSONObject jsonObject) {
        Set<String> keys = new TreeSet<String>();
        Iterator<?> iter = jsonObject.keys();
        while(iter.hasNext()) {
            keys.add((String)iter.next());
        }
        return keys;
    }

  /**
   *
   * @param prefix
   * @param key
   * @return the string
   */
  public static String qualify(String prefix, String key) {
        return "".equals(prefix) ? key : prefix + "." + key;
    }

  /**
   *
   * @param key
   * @param uniqueKey
   * @param value
   * @return the key
   */
  public static String formatUniqueKey(String key, String uniqueKey, Object value) {
        return key + "[" + uniqueKey + "=" + value + "]";
    }

  /**
   *
   * @param <T>
   * @param coll
   * @return the map
   */
  public static <T> Map<T, Integer> getCardinalityMap(final Collection<T> coll) {
        Map<T, Integer> count = new HashMap<>();
        for (T item : coll) {
            Integer c = (count.get(item));
            if (c == null) {
                count.put(item, INTEGER_ONE);
            } else {
                count.put(item, c.intValue() + 1);
            }
        }
        return count;
    }
}
