package edu.umass.cs.gnsclient.jsonassert.comparator;

import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareResult;
import static edu.umass.cs.gnsclient.jsonassert.comparator.JSONCompareUtil.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * This class is the default json comparator implementation.
 * Comparison is performed according to  {@link JSONCompareMode} that is passed as constructor's argument.
 */
public class DefaultComparator extends AbstractComparator {

    JSONCompareMode mode;

  /**
   *
   * @param mode
   */
  public DefaultComparator(JSONCompareMode mode) {
        this.mode = mode;
    }

  /**
   *
   * @param prefix
   * @param expected
   * @param actual
   * @param result
   * @throws JSONException
   */
  @Override
    public void compareJSON(String prefix, JSONObject expected, JSONObject actual, JSONCompareResult result)
            throws JSONException {
        // Check that actual contains all the expected values
        checkJsonObjectKeysExpectedInActual(prefix, expected, actual, result);

        // If strict, check for vice-versa
        if (!mode.isExtensible()) {
            checkJsonObjectKeysActualInExpected(prefix, expected, actual, result);
        }
    }

  /**
   *
   * @param prefix
   * @param expectedValue
   * @param actualValue
   * @param result
   * @throws JSONException
   */
  @Override
    public void compareValues(String prefix, Object expectedValue, Object actualValue, JSONCompareResult result)
            throws JSONException {
        if (expectedValue instanceof Number && actualValue instanceof Number) {
            if (((Number)expectedValue).doubleValue() != ((Number)actualValue).doubleValue()) {
                result.fail(prefix, expectedValue, actualValue);
            }
        } else if (expectedValue.getClass().isAssignableFrom(actualValue.getClass())) {
            if (expectedValue instanceof JSONArray) {
                compareJSONArray(prefix, (JSONArray) expectedValue, (JSONArray) actualValue, result);
            } else if (expectedValue instanceof JSONObject) {
                compareJSON(prefix, (JSONObject) expectedValue, (JSONObject) actualValue, result);
            } else if (!expectedValue.equals(actualValue)) {
                result.fail(prefix, expectedValue, actualValue);
            }
        } else {
            result.fail(prefix, expectedValue, actualValue);
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
  @Override
    public void compareJSONArray(String prefix, JSONArray expected, JSONArray actual, JSONCompareResult result)
            throws JSONException {
        if (expected.length() != actual.length()) {
            result.fail(prefix + "[]: Expected " + expected.length() + " values but got " + actual.length());
            return;
        } else if (expected.length() == 0) {
            return; // Nothing to compare
        }

        if (mode.hasStrictOrder()) {
            compareJSONArrayWithStrictOrder(prefix, expected, actual, result);
        } else if (allSimpleValues(expected)) {
            compareJSONArrayOfSimpleValues(prefix, expected, actual, result);
        } else if (allJSONObjects(expected)) {
            compareJSONArrayOfJsonObjects(prefix, expected, actual, result);
        } else {
            // An expensive last resort
            recursivelyCompareJSONArray(prefix, expected, actual, result);
        }
    }
}
