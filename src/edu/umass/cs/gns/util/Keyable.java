package edu.umass.cs.gns.util;
/**
@author V. Arun
 */
public interface Keyable<KeyType extends Comparable<KeyType>> {
	public KeyType getKey();
}
