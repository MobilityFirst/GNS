package edu.umass.cs.gnsclient.jsonassert;

import edu.umass.cs.gnsclient.jsonassert.comparator.JSONComparator;
import java.text.MessageFormat;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * <p>
 * A value matcher for arrays. This operates like STRICT_ORDER array match,
 * however if expected array has less elements than actual array the matching
 * process loops through the expected array to get expected elements for the
 * additional actual elements. In general the expected array will contain a
 * single element which is matched against each actual array element in turn.
 * This allows simple verification of constant array element components and
 * coupled with RegularExpressionValueMatcher can be used to match specific
 * array element components against a regular expression pattern. As a convenience to reduce syntactic complexity of expected string, if the
 * expected object is not an array, a one element expected array is created
 * containing whatever is provided as the expected value.</p>
 *
 * <p>
 * Some examples of typical usage idioms listed below.</p>
 *
 * <p>
 * Assuming JSON to be verified is held in String variable ARRAY_OF_JSONOBJECTS and contains:</p>
 *
 * <code>{a:[{background:white,id:1,type:row}, {background:grey,id:2,type:row}, {background:white,id:3,type:row}, {background:grey,id:4,type:row}]}</code>
 *
 * <p>
 * then:</p>
 *
 * <p>
 * To verify that the 'id' attribute of first element of array 'a' is '1':</p>
 *
 * <code>
 * JSONComparator comparator = new DefaultComparator(JSONCompareMode.LENIENT);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator, 0));<br>
 * JSONAssert.assertEquals("{a:[{id:1}]}", ARRAY_OF_JSONOBJECTS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * To simplify complexity of expected JSON string, the value <code>"a:[{id:1}]}"</code> may be replaced by <code>"a:{id:1}}"</code></p>
 *
 * <p>
 * To verify that the 'type' attribute of second and third elements of array 'a' is 'row':</p>
 *
 * <code>
 * JSONComparator comparator = new DefaultComparator(JSONCompareMode.LENIENT);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator, 1, 2));<br>
 * JSONAssert.assertEquals("{a:[{type:row}]}", ARRAY_OF_JSONOBJECTS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * To verify that the 'type' attribute of every element of array 'a' is 'row':</p>
 *
 * <code>
 * JSONComparator comparator = new DefaultComparator(JSONCompareMode.LENIENT);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator));<br>
 * JSONAssert.assertEquals("{a:[{type:row}]}", ARRAY_OF_JSONOBJECTS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * To verify that the 'background' attribute of every element of array 'a' alternates between 'white' and 'grey' starting with first element 'background' being 'white':</p>
 *
 * <code>
 * JSONComparator comparator = new DefaultComparator(JSONCompareMode.LENIENT);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator));<br>
 * JSONAssert.assertEquals("{a:[{background:white},{background:grey}]}", ARRAY_OF_JSONOBJECTS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * Assuming JSON to be verified is held in String variable ARRAY_OF_JSONARRAYS and contains:</p>
 *
 * <code>{a:[[6,7,8], [9,10,11], [12,13,14], [19,20,21,22]]}</code>
 *
 * <p>
 * then:</p>
 *
 * <p>
 * To verify that the first three elements of JSON array 'a' are JSON arrays of length 3:</p>
 *
 * <code>
 * JSONComparator comparator = new ArraySizeComparator(JSONCompareMode.STRICT_ORDER);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator, 0, 2));<br>
 * JSONAssert.assertEquals("{a:[[3]]}", ARRAY_OF_JSONARRAYS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * NOTE: simplified expected JSON strings are not possible in this case as ArraySizeComparator does not support them.</p>
 *
 * <p>
 * To verify that the second elements of JSON array 'a' is a JSON array whose first element has the value 9:</p>
 *
 * <code>
 * Customization innerCustomization = new Customization("a[1]", new ArrayValueMatcher&lt;Object&gt;(comparator, 0));<br>
 * JSONComparator comparator = new CustomComparator(JSONCompareMode.LENIENT, innerCustomization);<br>
 * Customization customization = new Customization("a", new ArrayValueMatcher&lt;Object&gt;(comparator, 1));<br>
 * JSONAssert.assertEquals("{a:[[9]]}", ARRAY_OF_JSONARRAYS, new CustomComparator(JSONCompareMode.LENIENT, customization));
 * </code>
 *
 * <p>
 * To simplify complexity of expected JSON string, the value <code>"{a:[[9]]}"</code> may be replaced by <code>"{a:[9]}"</code> or <code>"{a:9}"</code></p>
 *
 * @author Duncan Mackinder
 * @param <T>
 *
 */
public class ArrayValueMatcher<T> implements LocationAwareValueMatcher<T> {

  private final JSONComparator comparator;
  private final int from;
  private final int to;

  /**
   * Create ArrayValueMatcher to match every element in actual array against
   * elements taken in sequence from expected array, repeating from start of
   * expected array if necessary.
   *
   * @param comparator
   * comparator to use to compare elements
   */
  public ArrayValueMatcher(JSONComparator comparator) {
    this(comparator, 0, Integer.MAX_VALUE);
  }

  /**
   * Create ArrayValueMatcher to match specified element in actual array
   * against first element of expected array.
   *
   * @param comparator
   * comparator to use to compare elements
   * @param index
   * index of the array element to be compared
   */
  public ArrayValueMatcher(JSONComparator comparator, int index) {
    this(comparator, index, index);
  }

  /**
   * Create ArrayValueMatcher to match every element in specified range
   * (inclusive) from actual array against elements taken in sequence from
   * expected array, repeating from start of expected array if necessary.
   *
   * @param comparator
   * comparator to use to compare elements
   * @param from
   * @param to
   * @from first element in actual array to compared
   * @to last element in actual array to compared
   */
  public ArrayValueMatcher(JSONComparator comparator, int from, int to) {
    assert comparator != null : "comparator null";
    assert from >= 0 : MessageFormat.format("from({0}) < 0", from);
    assert to >= from : MessageFormat.format("to({0}) < from({1})", to,
            from);
    this.comparator = comparator;
    this.from = from;
    this.to = to;
  }

  /**
   *
   * @param o1
   * @param o2
   * @return true if equal
   */
  @Override
  /*
	 * NOTE: method defined as required by ValueMatcher interface but will never
	 * be called so defined simply to indicate match failure
   */
  public boolean equal(T o1, T o2) {
    return false;
  }

  /**
   *
   * @param prefix
   * @param actual
   * @param expected
   * @param result
   * @return true if equal
   */
  @Override
  public boolean equal(String prefix, T actual, T expected, JSONCompareResult result) {
    if (!(actual instanceof JSONArray)) {
      throw new IllegalArgumentException("ArrayValueMatcher applied to non-array actual value");
    }
    try {
      JSONArray actualArray = (JSONArray) actual;
      JSONArray expectedArray = expected instanceof JSONArray ? (JSONArray) expected : new JSONArray(new Object[]{expected});
      int first = Math.max(0, from);
      int last = Math.min(actualArray.length() - 1, to);
      int expectedLen = expectedArray.length();
      for (int i = first; i <= last; i++) {
        String elementPrefix = MessageFormat.format("{0}[{1}]", prefix, i);
        Object actualElement = actualArray.get(i);
        Object expectedElement = expectedArray.get((i - first) % expectedLen);
        comparator.compareValues(elementPrefix, expectedElement, actualElement, result);
      }
      // any failures have already been passed to result, so return true
      return true;
    } catch (JSONException e) {
      return false;
    }
  }

}
