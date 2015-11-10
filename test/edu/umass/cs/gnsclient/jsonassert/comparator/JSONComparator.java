package edu.umass.cs.gnsclient.jsonassert.comparator;

import edu.umass.cs.gnsclient.jsonassert.JSONCompareResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Interface for comparison handler.
 *
 * @author <a href="mailto:aiveeen@gmail.com">Ivan Zaytsev</a>
 *         2013-01-04
 */
public interface JSONComparator {

    JSONCompareResult compareJSON(JSONObject expected, JSONObject actual) throws JSONException;

    JSONCompareResult compareJSON(JSONArray expected, JSONArray actual) throws JSONException;

    void compareJSON(String prefix, JSONObject expected, JSONObject actual, JSONCompareResult result) throws JSONException;

    void compareValues(String prefix, Object expectedValue, Object actualValue, JSONCompareResult result) throws JSONException;

    void compareJSONArray(String prefix, JSONArray expected, JSONArray actual, JSONCompareResult result) throws JSONException;
}
