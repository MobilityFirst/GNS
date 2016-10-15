package edu.umass.cs.gnsclient.jsonassert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Bean for holding results from JSONCompare.
 */
public class JSONCompareResult {
    private boolean _success;
    private StringBuilder _message;
    private String _field;
    private Object _expected;
    private Object _actual;
    private final List<FieldComparisonFailure> _fieldFailures = new ArrayList<FieldComparisonFailure>();

    /**
     * Default constructor.
     */
    public JSONCompareResult() {
        this(true, null);
    }

    private JSONCompareResult(boolean success, String message) {
        _success = success;
        _message = new StringBuilder(message == null ? "" : message);
    }

    /**
     * Did the comparison pass?
     * @return True if it passed
     */
    public boolean passed() {
        return _success;
    }

    /**
     * Did the comparison fail?
     * @return True if it failed
     */
    public boolean failed() {
        return !_success;
    }

    /**
     * Result message
     * @return String explaining why if the comparison failed
     */
    public String getMessage() {
        return _message.toString();
    }

    /**
     * Get the list of failures on field comparisons
   * @return 
     */
    public List<FieldComparisonFailure> getFieldFailures() {
        return Collections.unmodifiableList(_fieldFailures);
    }

    /**
     * Actual field value
     * 
     * @return a {@code JSONObject}, {@code JSONArray} or other {@code Object}
     *         instance, or {@code null} if the comparison did not fail on a
     *         particular field
     * @deprecated Superseded by {@link #getFieldFailures()}
     */
    public Object getActual() {
        return _actual;
    }
    
    /**
     * Expected field value
     * 
     * @return a {@code JSONObject}, {@code JSONArray} or other {@code Object}
     *         instance, or {@code null} if the comparison did not fail on a
     *         particular field
     * @deprecated Superseded by {@link #getFieldFailures()}
     */
    public Object getExpected() {
        return _expected;
    }
    
    /**
     * Check if comparison failed on any particular fields
   * @return 
     */
    public boolean isFailureOnField() {
        return !_fieldFailures.isEmpty();
    }

    /**
     * Dot-separated path the the field that failed comparison
     * 
     * @return a {@code String} instance, or {@code null} if the comparison did
     *         not fail on a particular field
     * @deprecated Superseded by {@link #getFieldFailures()}
     */
    public String getField() {
        return _field;
    }
    
  /**
   *
   * @param message
   */
  public void fail(String message) {
        _success = false;
        if (_message.length() == 0) {
            _message.append(message);
        } else {
            _message.append(" ; ").append(message);
        }
    }

    /**
     * Identify that the comparison failed
     * @param field Which field failed
     * @param expected Expected result
     * @param actual Actual result
   * @return 
     */
    public JSONCompareResult fail(String field, Object expected, Object actual) {
        _fieldFailures.add(new FieldComparisonFailure(field, expected, actual));
        this._field = field;
        this._expected = expected;
        this._actual = actual;
        fail(formatFailureMessage(field, expected, actual));
        return this;
    }

    /**
     * Identify that the comparison failed
     * @param field Which field failed
     * @param exception exception containing details of match failure
   * @return 
     */
    public JSONCompareResult fail(String field, ValueMatcherException exception) {
    	fail(field + ": " + exception.getMessage(), exception.getExpected(), exception.getActual());
        return this;
    }

    private String formatFailureMessage(String field, Object expected, Object actual) {
        return field
                + "\nExpected: "
                + describe(expected)
                + "\n     got: "
                + describe(actual)
                + "\n";
    }

  /**
   *
   * @param field
   * @param expected
   * @return a JSONCompareResult
   */
  public JSONCompareResult missing(String field, Object expected) {
        fail(formatMissing(field, expected));
        return this;
    }

    private String formatMissing(String field, Object expected) {
        return field
                + "\nExpected: "
                + describe(expected)
                + "\n     but none found\n";
    }

  /**
   *
   * @param field
   * @param value
   * @return JSONCompareResult
   */
  public JSONCompareResult unexpected(String field, Object value) {
        fail(formatUnexpected(field, value));
        return this;
    }

    private String formatUnexpected(String field, Object value) {
        return field
                + "\nUnexpected: "
                + describe(value)
                + "\n";
    }

    private static String describe(Object value) {
        if (value instanceof JSONArray) {
            return "a JSON array";
        } else if (value instanceof JSONObject) {
            return "a JSON object";
        } else {
            return value.toString();
        }
    }

    @Override
    public String toString() {
        return _message.toString();
    }
}
