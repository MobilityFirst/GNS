package edu.umass.cs.utils;

import java.util.Arrays;
import java.util.HashSet;

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
	final Object data;

	/**
	 * @param data 
	 */
	public Stringer(byte[] data) {
		this.data = data;
	}
	/**
	 * @param data
	 */
	public Stringer(Object data) {
		this.data = data;
	}

	public String toString() {
		if(data instanceof byte[])
			return new String((byte[])data);
		else if(data instanceof Integer[]) 
			return (new HashSet<Integer>(Arrays.asList((Integer[])data))).toString();
		return data.toString();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		byte[] buf = "hello world".getBytes();
		System.out.println(new Stringer(buf));
		Integer[] intArray = {23, 43, 56};
		System.out.println(new Stringer(intArray));
	}
}