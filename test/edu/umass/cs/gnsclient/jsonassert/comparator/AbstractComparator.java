package edu.umass.cs.gnsclient.jsonassert.comparator;

import edu.umass.cs.gnsclient.jsonassert.JSONCompareResult;
import static edu.umass.cs.gnsclient.jsonassert.comparator.JSONCompareUtil.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;

/**
 * This class provides a skeletal implementation of the {@link JSONComparator}
 * interface, to minimize the effort required to implement this interface.
 */
public abstract class AbstractComparator implements JSONComparator {

    /**
     * Compares JSONObject provided to the expected JSONObject, and returns the results of the comparison.
     *
     * @param expected Expected JSONObject
     * @param actual   JSONObject to compare
   * @return 
     * @throws JSONException
     */
    @Override
    public final JSONCompareResult compareJSON(JSONObject expected, JSONObject actual) throws JSONException {
        JSONCompareResult result = new JSONCompareResult();
        compareJSON("", expected, actual, result);
        return result;
    }

    /**
     * Compares JSONArray provided to the expected JSONArray, and returns the results of the comparison.
     *
     * @param expected Expected JSONArray
     * @param actual   JSONArray to compare
   * @return 
     * @throws JSONException
     */
    @Override
    public final JSONCompareResult compareJSON(JSONArray expected, JSONArray actual) throws JSONException {
        JSONCompareResult result = new JSONCompareResult();
        compareJSONArray("", expected, actual, result);
        return result;
    }

  /**
   *
   * @param prefix
   * @param expected
   * @param actual
   * @param result
   */
  protected void checkJsonObjectKeysActualInExpected(String prefix, JSONObject expected, JSONObject actual, JSONCompareResult result) {
        Set<String> actualKeys = getKeys(actual);
        for (String key : actualKeys) {
            if (!expected.has(key)) {
                result.unexpected(prefix, key);
            }
        }
    }

  /**
   *
   * @param prefix
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
  protected void checkJsonObjectKeysExpectedInActual(String prefix, JSONObject expected, JSONObject actual, JSONCompareResult result) throws JSONException {
        Set<String> expectedKeys = getKeys(expected);
        for (String key : expectedKeys) {
            Object expectedValue = expected.get(key);
            if (actual.has(key)) {
                Object actualValue = actual.get(key);
                compareValues(qualify(prefix, key), expectedValue, actualValue, result);
            } else {
                result.missing(prefix, key);
            }
        }
    }

  /**
   *
   * @param key
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
  protected void compareJSONArrayOfJsonObjects(String key, JSONArray expected, JSONArray actual, JSONCompareResult result) throws JSONException {
        String uniqueKey = findUniqueKey(expected);
        if (uniqueKey == null || !isUsableAsUniqueKey(uniqueKey, actual)) {
            // An expensive last resort
            recursivelyCompareJSONArray(key, expected, actual, result);
            return;
        }
        Map<Object, JSONObject> expectedValueMap = arrayOfJsonObjectToMap(expected, uniqueKey);
        Map<Object, JSONObject> actualValueMap = arrayOfJsonObjectToMap(actual, uniqueKey);
        for (Object id : expectedValueMap.keySet()) {
            if (!actualValueMap.containsKey(id)) {
                result.missing(formatUniqueKey(key, uniqueKey, id), expectedValueMap.get(id));
                continue;
            }
            JSONObject expectedValue = expectedValueMap.get(id);
            JSONObject actualValue = actualValueMap.get(id);
            compareValues(formatUniqueKey(key, uniqueKey, id), expectedValue, actualValue, result);
        }
        for (Object id : actualValueMap.keySet()) {
            if (!expectedValueMap.containsKey(id)) {
                result.unexpected(formatUniqueKey(key, uniqueKey, id), actualValueMap.get(id));
            }
        }
    }

  /**
   *
   * @param key
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
  protected void compareJSONArrayOfSimpleValues(String key, JSONArray expected, JSONArray actual, JSONCompareResult result) throws JSONException {
        Map<Object, Integer> expectedCount = JSONCompareUtil.getCardinalityMap(jsonArrayToList(expected));
        Map<Object, Integer> actualCount = JSONCompareUtil.getCardinalityMap(jsonArrayToList(actual));
        for (Object o : expectedCount.keySet()) {
            if (!actualCount.containsKey(o)) {
                result.missing(key + "[]", o);
            } else if (!actualCount.get(o).equals(expectedCount.get(o))) {
                result.fail(key + "[]: Expected " + expectedCount.get(o) + " occurrence(s) of " + o
                        + " but got " + actualCount.get(o) + " occurrence(s)");
            }
        }
        for (Object o : actualCount.keySet()) {
            if (!expectedCount.containsKey(o)) {
                result.unexpected(key + "[]", o);
            }
        }
    }

  /**
   *
   * @param key
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
  protected void compareJSONArrayWithStrictOrder(String key, JSONArray expected, JSONArray actual, JSONCompareResult result) throws JSONException {
        for (int i = 0; i < expected.length(); ++i) {
            Object expectedValue = expected.get(i);
            Object actualValue = actual.get(i);
            compareValues(key + "[" + i + "]", expectedValue, actualValue, result);
        }
    }

    // This is expensive (O(n^2) -- yuck), but may be the only resort for some cases with loose array ordering, and no
    // easy way to uniquely identify each element.
    // This is expensive (O(n^2) -- yuck), but may be the only resort for some cases with loose array ordering, and no
    // easy way to uniquely identify each element.

  /**
   *
   * @param key
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
    protected void recursivelyCompareJSONArray(String key, JSONArray expected, JSONArray actual,
                                               JSONCompareResult result) throws JSONException {
        Set<Integer> matched = new HashSet<Integer>();
        for (int i = 0; i < expected.length(); ++i) {
            Object expectedElement = expected.get(i);
            boolean matchFound = false;
            for (int j = 0; j < actual.length(); ++j) {
                Object actualElement = actual.get(j);
                if (matched.contains(j) || !actualElement.getClass().equals(expectedElement.getClass())) {
                    continue;
                }
                if (expectedElement instanceof JSONObject) {
                    if (compareJSON((JSONObject) expectedElement, (JSONObject) actualElement).passed()) {
                        matched.add(j);
                        matchFound = true;
                        break;
                    }
                } else if (expectedElement instanceof JSONArray) {
                    if (compareJSON((JSONArray) expectedElement, (JSONArray) actualElement).passed()) {
                        matched.add(j);
                        matchFound = true;
                        break;
                    }
                } else if (expectedElement.equals(actualElement)) {
                    matched.add(j);
                    matchFound = true;
                    break;
                }
            }
            if (!matchFound) {
                result.fail(key + "[" + i + "] Could not find match for element " + expectedElement);
                return;
            }
        }
    }
}
