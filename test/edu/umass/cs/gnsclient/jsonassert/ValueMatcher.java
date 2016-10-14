package edu.umass.cs.gnsclient.jsonassert;

/**
 *
 * @author westy
 * @param <T>
 */
public interface ValueMatcher<T> {

  /**
   *
   * @param o1
   * @param o2
   * @return true if equal
   */
  boolean equal(T o1, T o2);

}
