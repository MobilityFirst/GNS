package edu.umass.cs.gnsclient.jsonassert;

/**
 * Models a failure when comparing two fields.
 */
public class FieldComparisonFailure {
    private final String _field;
    private final Object _expected;
    private final Object _actual;

  /**
   *
   * @param field
   * @param expected
   * @param actual
   */
  public FieldComparisonFailure(String field, Object expected, Object actual) {
        this._field = field;
        this._expected = expected;
        this._actual = actual;
    }

  /**
   *
   * @return the field
   */
  public String getField() {
        return _field;
    }

  /**
   *
   * @return the expected
   */
  public Object getExpected() {
        return _expected;
    }

  /**
   *
   * @return the actual
   */
  public Object getActual() {
        return _actual;
    }
}
