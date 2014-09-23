package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Various generic static utility methods.
 *
 * @author V. Arun, Hardeep Uppal, Westy
 */
public class Util {
	public static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
	public static final double ALPHA = 0.05;

	public static final String df(double d) {return decimalFormat.format(d);}
	public static final String mu(double d) {return decimalFormat.format(d*1000)+"us";} // milli to microseconds
	public static final double movingAverage(double sample, double historicalAverage, double alpha) {
		return (1-alpha)*((double)historicalAverage) + alpha*((double)sample);
	}
	public static final double movingAverage(double sample, double historicalAverage) {
		return movingAverage(sample, historicalAverage, ALPHA);
	}
	public static final double movingAverage(long sample, double historicalAverage) {
		return movingAverage((double)sample, historicalAverage);
	}
	public static final double movingAverage(long sample, double historicalAverage, double alpha) {
		return movingAverage((double)sample, historicalAverage, alpha);
	}


	public static int roundToInt(double d) {
		return (int) Math.round(d);
	}
	
	public static String refreshKey(String id) {
		return (id.toString() + (int) (Math.random() * Integer.MAX_VALUE));
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
	public static int[] setToIntArray(Set<Integer> set) {
		int[] array = new int[set.size()];
		int i=0; for(int id : set) array[i++] = id;
		return array;
	}
	public static Integer[] setToIntegerArray(Set<Integer> set) {
		Integer[] array = new Integer[set.size()];
		int i=0; for(Integer id : set) array[i++] = id;
		return array;
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
	public static Integer[] objectToIntegerArray(Object[] objects) {
		if(objects==null) return null;
		else if(objects.length==0) return new Integer[0];
		Integer[] array = new Integer[objects.length];
		int i=0;
		for(Object obj : objects) {
			array[i++] = (Integer)obj;
		}
		return array;
	}
	public static Integer[] intToIntegerArray(int[] array) {
		if(array==null) return null;
		else if(array.length==0) return new Integer[0];
		Integer[] retarray = new Integer[array.length];
		int i=0;
		for(int member : array) {
			retarray[i++] = member; 
		}
		return retarray;
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
