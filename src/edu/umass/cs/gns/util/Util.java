package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Various generic static utility methods.
 *
 * @author Hardeep Uppal, Westy
 */
public class Util {
	public static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
	public static final double ALPHA = 0.05;

	public static final String df(double d) {return decimalFormat.format(d);}
	public static final String mu(double d) {return decimalFormat.format(d*1000)+"us";} // milli to microseconds
	public static final double movingAverage(double sample, double historicalAverage) {
		return (1-ALPHA)*((double)historicalAverage) + ALPHA*((double)sample);
	}
	public static final double movingAverage(long sample, double historicalAverage) {
		return movingAverage((double)sample, historicalAverage);
	}


	public static int roundToInt(double d) {
		return (int) Math.round(d);
	}

	public static Object createObject(String className, Object ... arguments) {
		Object object;
		Class[] types = new Class[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			types[i] = arguments[i].getClass();
		}
		try {
			Class theClass = Class.forName(className);
			Constructor constructor = theClass.getConstructor(types);
			object = constructor.newInstance(arguments);
			return object;
		} catch (NoSuchMethodException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		} catch (ClassNotFoundException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		} catch (InstantiationException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		} catch (IllegalAccessException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		} catch (IllegalArgumentException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		} catch (InvocationTargetException e) {
			GNS.getLogger().severe("Problem creating instance: " + e);
		}
		return null;
	}

	public static Map<String, String> parseURIQueryString(String query) {
		Map<String, String> result = new HashMap<String, String>();
		QueryStringParser parser = new QueryStringParser(query);
		while (parser.next()) {
			result.put(parser.getName(), parser.getValue());
		}
		return result;
	}

	public static Set<Integer> arrayToSet(int[] array) {
		TreeSet<Integer> set = new TreeSet<Integer>();
		for(int i=0; i<array.length;i++) set.add(array[i]);
		return set;
	}
	public static int[] stringToArray(String string) {
		string = string.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "");
		String[] tokens = string.split(",");
		int[] array = new int[tokens.length];
		for(int i=0; i<array.length;i++) {
			array[i] = Integer.parseInt(tokens[i]);
		}
		return array;
	}
	public static String arrayToString(int[] array) {
		String s="[";
		for(int i=0;i<array.length;i++) {
			s+=array[i];
			s+=(i<array.length-1 ? ",":"]");
		}
		return s;
	}


	static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random(System.currentTimeMillis());

	public static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
		}
		return sb.toString();
	}
}
