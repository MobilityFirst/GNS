/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */
package edu.umass.cs.utils;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 *         Various generic static utility methods.
 */
@SuppressWarnings("javadoc")
public class Util {

	private static Logger log = Logger.getLogger(Util.class.getName());

	public static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
	public static final double ALPHA = 0.05; // sample weight

	public static final String df(double d) {
		return decimalFormat.format(d);
	}

	public static final String ms(double d) {
		return decimalFormat.format(d) + "ms";
	} // milli to microseconds
	public static final String mu(double d) {
		return decimalFormat.format(d * 1000) + "us";
	} // milli to microseconds
	public static final String nmu(double d) {
		return decimalFormat.format(d / 1000.0) + "us";
	} // milli to microseconds

	public static final double movingAverage(double sample,
			double historicalAverage, double alpha) {
		return (1 - alpha) * ((double) historicalAverage) + alpha
				* ((double) sample);
	}

	public static final double movingAverage(double sample,
			double historicalAverage) {
		return movingAverage(sample, historicalAverage, ALPHA);
	}

	public static final double movingAverage(long sample,
			double historicalAverage) {
		return movingAverage((double) sample, historicalAverage);
	}

	public static final double movingAverage(long sample,
			double historicalAverage, double alpha) {
		return movingAverage((double) sample, historicalAverage, alpha);
	}

	public static String refreshKey(String id) {
		return (id.toString() + (int) (Math.random() * Integer.MAX_VALUE));
	}

	public static boolean oneIn(int n) {
		return Math.random() < 1.0 / Math.max(1, n) ? true : false;
	}

	public static int roundToInt(double d) {
		return (int) Math.round(d);
	}

	public static void assertAssertionsEnabled() {
		boolean assertOn = false;
		// *assigns* true if assertions are on.
		assert assertOn = true;
		if (!assertOn) {
			throw new RuntimeException(
					"Asserts not enabled; enable assertions using the '-ea' JVM option");
		}
	}

	public static String prefix(String str, int prefixLength) {
		if (str == null || str.length() <= prefixLength) {
			return str;
		}
		return str.substring(0, prefixLength);
	}

	public static Set<Integer> arrayToIntSet(int[] array) {
		TreeSet<Integer> set = new TreeSet<Integer>();
		for (int i = 0; i < array.length; i++) {
			set.add(array[i]);
		}
		return set;
	}

	public static Set<String> setToStringSet(Set<?> set) {
		Set<String> result = new HashSet<String>();
		for (Object id : set) {
			result.add(id.toString());
		}
		return result;
	}

	// will throw exception if any string is not parseable as integer
	public static Set<Integer> stringSetToIntegerSet(Set<String> set) {
		Set<Integer> intIDs = new HashSet<Integer>();
		for (String s : set)
			intIDs.add(Integer.valueOf(s));
		return intIDs;
	}

	public static int[] setToIntArray(Set<Integer> set) {
		int[] array = new int[set.size()];
		int i = 0;
		for (int id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static Object[] setToNodeIdArray(Set<?> set) {
		Object[] array = new Object[set.size()];
		int i = 0;
		for (Object id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static Integer[] setToIntegerArray(Set<Integer> set) {
		Integer[] array = new Integer[set.size()];
		int i = 0;
		for (Integer id : set) {
			array[i++] = id;
		}
		return array;
	}

	public static int[] stringToIntArray(String string) {
		string = string.replaceAll("\\[", "").replaceAll("\\]", "")
				.replaceAll("\\s", "");
		String[] tokens = string.split(",");
		int[] array = new int[tokens.length];
		for (int i = 0; i < array.length; i++) {
			array[i] = Integer.parseInt(tokens[i]);
		}
		return array;
	}

	public static Set<String> stringToStringSet(String string)
			throws JSONException {
		JSONArray jsonArray = new JSONArray(string);
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < jsonArray.length(); i++)
			set.add(jsonArray.getString(i));
		return set;
	}

	// to test the json-smart parser
	public static JSONObject toJSONObject(String s) throws JSONException {
		net.minidev.json.JSONObject sjson = (net.minidev.json.JSONObject) net.minidev.json.JSONValue
				.parse(s);
		JSONObject json = new JSONObject();
		for (String key : sjson.keySet()) {
			Object obj = sjson.get(key);
			if (obj instanceof Collection<?>)
				json.put(key, new JSONArray(obj.toString()));
			else
				json.put(key, obj);
		}
		return json;
	}

	public static Integer[] intToIntegerArray(int[] array) {
		if (array == null) {
			return null;
		} else if (array.length == 0) {
			return new Integer[0];
		}
		Integer[] retarray = new Integer[array.length];
		int i = 0;
		for (int member : array) {
			retarray[i++] = member;
		}
		return retarray;
	}

	public static Set<String> arrayOfIntToStringSet(int[] array) {
		Set<String> set = new HashSet<String>();
		for (Integer member : array) {
			set.add(member.toString());
		}
		return set;
	}

	public static String arrayOfIntToString(int[] array) {
		String s = "";
		for (int i = 0; i < array.length; i++) {
			s += array[i];
			s += (i < array.length - 1 ? "," : "");
		}
		return "[" + s + "]";
	}

	public static boolean contains(int member, int[] array) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] == member) {
				return true;
			}
		}
		return false;
	}

	public static Set<String> arrayOfObjectsToStringSet(Object[] array) {
		Set<String> set = new HashSet<String>();
		for (Object member : array) {
			set.add(member.toString());
		}
		return set;
	}

	// FIXME: Is there a sublinear method to return a random member from a set?
	public static Object selectRandom(Collection<?> set) {
		int random = (int) (Math.random() * set.size());
		Iterator<?> iterator = set.iterator();
		Object randomNode = null;
		for (int i = 0; i <= random && iterator.hasNext(); i++) {
			randomNode = iterator.next();
		}
		return randomNode;
	}

	public static InetSocketAddress getInetSocketAddressFromString(String s) {
		s = s.replaceAll("[^0-9.:]", "");
		String[] tokens = s.split(":");
		if (tokens.length < 2) {
			return null;
		}
		return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
	}

	public static String toJSONString(Collection<?> collection) {
		JSONArray jsonArray = new JSONArray(collection);
		return jsonArray.toString();
	}

	public static String[] jsonToStringArray(String jsonString)
			throws JSONException {
		JSONArray jsonArray = new JSONArray(jsonString);
		String[] stringArray = new String[jsonArray.length()];
		for (int i = 0; i < jsonArray.length(); i++) {
			stringArray[i] = jsonArray.getString(i);
		}
		return stringArray;
	}

	public static String toJSONString(int[] array) {
		return toJSONString(Util.arrayOfIntToStringSet(array));
	}

	public static ArrayList<Integer> JSONArrayToArrayListInteger(
			JSONArray jsonArray) throws JSONException {
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < jsonArray.length(); i++) {
			list.add(jsonArray.getInt(i));
		}
		return list;
	}

	public void assertEnabled() {
		try {
			assert (false);
		} catch (Exception e) {
			return;
		}
		throw new RuntimeException("Asserts not enabled; exiting");
	}

	/*
	 * The methods below return an Object with a toString method so that the
	 * string won't actually get created until its toString method is invoked.
	 * This is useful to optimize logging.
	 */

	public static Object truncate(final String str, final int size) {
		return new Object() {
			@Override
			public String toString() {
				return str == null || str.length() < size ? str
						: str != null ? str.substring(0, size) : null;
			}
		};
	}

	public static Object truncate(final String str, final int prefixSize,
			final int suffixSize) {
		final int size = prefixSize + suffixSize;
		return new Object() {
			@Override
			public String toString() {
				return str == null || str.length() < size ? str
						: str != null ? str.substring(0, prefixSize)
								+ "[...]"
								+ str.substring(str.length() - suffixSize)
								: null;
			}
		};
	}
	
	public static byte[] getAlphanumericAsBytes()  {
		int low = '0', high = 'z';
		byte[] bytes = new byte[high - low + 1];
		for (int i = 0; i < bytes.length; i++)
			bytes[i] = (byte) (low + i);
		return bytes;
	}
	public static byte[] getRandomAlphanumericBytes() {
		byte[] an = Util.getAlphanumericAsBytes();
		byte[] msg = new byte[1024];
		for(int i=0; i<msg.length; i++) 
			msg[i] = an[(int)(Math.random()*an.length)];
		return msg;
	}


	private static Collection<?> truncate(Collection<?> list, int size) {
		if (list.size() <= size)
			return list;
		ArrayList<Object> truncated = new ArrayList<Object>();
		int i = 0;
		for (Object o : list)
			if (i++ < size)
				truncated.add((Object) o);
			else
				break;
		return truncated;
	}

	public static Object truncatedLog(final Collection<?> list, final int size) {
		return new Object() {
			public String toString() {
				return truncate(list, size).toString()
						+ (list.size() <= size ? "" : "...");
			}
		};
	}

	public static Object suicide(String error) {
		log.severe(error);
		new RuntimeException(error).printStackTrace();
		System.exit(1);
		return null; // will never come here
	}

	// transfer from one byte buffer to another without throwing exception
	public static ByteBuffer put(ByteBuffer dst, ByteBuffer src) {
		if (src.remaining() < dst.remaining())
			return dst.put(src);
		byte[] buf = new byte[dst.remaining()];
		src.get(buf);
		return dst.put(buf);
	}

	private static final String CHARSET = "ISO-8859-1";

	public static String sockAddrToEncodedString(InetSocketAddress isa)
			throws UnsupportedEncodingException {
		byte[] address = isa.getAddress().getAddress();
		byte[] buf = new byte[address.length + 2];
		for (int i = 0; i < address.length; i++)
			buf[i] = address[i];
		buf[address.length] = (byte) (isa.getPort() >> 8);
		buf[address.length + 1] = (byte) (isa.getPort() & 255);
		return new String(buf, CHARSET);

	}

	public static InetSocketAddress encodedStringToInetSocketAddress(String str)
			throws UnknownHostException, UnsupportedEncodingException {
		byte[] buf = str.getBytes(CHARSET);
		int port = (int) (buf[buf.length - 2] << 8)
				+ (buf[buf.length - 1] & 255);
		return new InetSocketAddress(InetAddress.getByAddress(Arrays
				.copyOfRange(buf, 0, 4)), port);
	}

	public static byte[] longToBytes(long value) {
		int size = Long.SIZE / 8;
		byte[] buf = new byte[size];
		for (int i = 0; i < size; i++)
			buf[i] = (byte) ((value >> ((size - i - 1) * 8)) & 255);
		return buf;
	}

	public static String longToEncodedString(long value)
			throws UnsupportedEncodingException {
		return new String(longToBytes(value), CHARSET);
	}

	public static long bytesToLong(byte[] bytes) {
		long value = 0;
		for (int i = 0; i < bytes.length; i++)
			value += ((long) (bytes[i] & 255) << ((bytes.length - i - 1) * 8));
		return value;
	}

	public static long encodedStringToLong(String str)
			throws UnsupportedEncodingException {
		return Util.bytesToLong(str.getBytes(CHARSET));
	}

	private static void testToBytesAndBack() throws UnknownHostException,
			UnsupportedEncodingException {
		InetSocketAddress isa = new InetSocketAddress("128.119.235.43", 23451);
		assert (Util.encodedStringToInetSocketAddress(Util
				.sockAddrToEncodedString(isa)).equals(isa));
		int n = 10000;
		for (int i = 0; i < n; i++) {
			long t = (long) (Math.random() * Long.MAX_VALUE);
			byte[] buf = (Util.longToBytes(t));
			assert (t == Util.bytesToLong(buf));
		}
		for (int i = 0; i < n; i++) {
			long value = (long) (Math.random() * Long.MAX_VALUE);
			assert (value == Util.encodedStringToLong(Util
					.longToEncodedString(value)));
		}
		for (int i = 0; i < n; i++) {
			byte[] address = new byte[4];
			for (int j = 0; j < 4; j++)
				address[j] = (byte) (Math.random() * Byte.MAX_VALUE);
			InetSocketAddress sockAddr = new InetSocketAddress(
					InetAddress.getByAddress(address),
					(int) (Math.random() * Short.MAX_VALUE));
			assert (Util.encodedStringToInetSocketAddress(Util
					.sockAddrToEncodedString(sockAddr)).equals(sockAddr));
		}
	}

	public static void main(String[] args) throws UnsupportedEncodingException,
			UnknownHostException {
		Util.assertAssertionsEnabled();
		testToBytesAndBack();

	}
}
