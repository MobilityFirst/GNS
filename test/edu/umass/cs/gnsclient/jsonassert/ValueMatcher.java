package edu.umass.cs.gnsclient.jsonassert;

public interface ValueMatcher<T> {

    boolean equal(T o1, T o2);

}
