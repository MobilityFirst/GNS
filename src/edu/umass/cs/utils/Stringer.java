package edu.umass.cs.utils;

/**
 * @author arun
 *
 * The purpose of this class is to wrap different objects into 
 * this class so that the toString method can later give us a 
 * string if needed. This optimizes logging because the logger
 * won't actually call the toString method unless the log level
 * actually demands it.
 */
public class Stringer {
	final byte[] data;

	/**
	 * @param data 
	 */
	public Stringer(byte[] data) {
		this.data = data;
	}

	public String toString() {
		return new String(data);
	}
}