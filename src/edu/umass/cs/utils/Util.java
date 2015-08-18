package edu.umass.cs.utils;

import java.net.InetSocketAddress;
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

	public static final String mu(double d) {
		return decimalFormat.format(d * 1000) + "us";
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
		return Math.random() < 1.0 / n ? true : false;
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

	public static Set<String> nodeIdSetToStringSet(Set<?> set) {
		Set<String> result = new HashSet<String>();
		for (Object id : set) {
			result.add(id.toString());
		}
		return result;
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
								+ "..[snip].."
								+ str.substring(str.length() - suffixSize)
								: null;
			}
		};
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

	public static void main(String[] args) {
		int[] array = {21, -25, 456, 92};
		int n = 10000000;
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++) {
			//String s= (Arrays.toString(array));
			//String s = (Util.arrayOfIntToString(array));
		}
		System.out.println(Arrays.toString(array));
		System.out.println("average_iter_time = " + (System.currentTimeMillis() -t)*1000.0/n);
	}
}
