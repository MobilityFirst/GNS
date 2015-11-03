package edu.umass.cs.utils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author arun
 * 
 * @param <K>
 * @param <V>
 *
 */
public interface Diskable<K, V> {
	/**
	 * This method should return only after successfully persisting the
	 * key,value pair, otherwise it should throw an exception.
	 * 
	 * @param toCommit
	 * @return Set of successfully committed keys.
	 * 
	 * @throws IOException
	 */
	public Set<K> commit(Map<K, V> toCommit) throws IOException;

	/**
	 * @param key
	 * @return Value corresponding to key in the persistent store.
	 * @throws IOException
	 */
	public V restore(K key) throws IOException;
}
