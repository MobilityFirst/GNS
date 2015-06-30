package edu.umass.cs.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author arun
 *
 */
public class MyLogger extends Logger {
	
	/**
	 * 
	 */
	public static final String[] FORMAT = {
		"{0}", 
		"{0} {1}",
		"{0} {1} {2}",
		"{0} {1} {2} {3}",
		"{0} {1} {2} {3} {4}",
		"{0} {1} {2} {3} {4} {5}",
		"{0} {1} {2} {3} {4} {5} {6}",
		"{0} {1} {2} {3} {4} {5} {6} {7}",
		"{0} {1} {2} {3} {4} {5} {6} {7} {8}",
		"{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}",
		"{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}",
	};

	public static MyLogger getLogger(String name) {
		return new MyLogger(Logger.getLogger(name));
	}
	protected MyLogger(Logger logger) {
		super(logger.getName(), logger.getResourceBundleName());
		this.setParent(logger); // needed to inherit logger's properties
	}
	/**
	 * @param objects
	 */
	public void info(Object... objects) {
		log(Level.INFO, format(objects), objects);
	}
	/**
	 * @param objects
	 */
	public void fine(Object... objects) {
		log(Level.FINE, format(objects), objects);
	}
	/**
	 * @param objects
	 */
	public void finer(Object... objects) {
		log(Level.FINER, format(objects), objects);
	}
	/**
	 * @param objects
	 */
	public void finest(Object... objects) {
		log(Level.FINEST, format(objects), objects);
	}
	/**
	 * @param objects
	 */
	public void warning(Object... objects) {
		log(Level.WARNING, format(objects), objects);
	}
	/**
	 * @param objects
	 */
	public void severe(Object... objects) {
		log(Level.SEVERE, format(objects), objects);
	}

	private static String format(Object... objects) {
		if(objects.length <= FORMAT.length) return FORMAT[objects.length-1];
		String s="";
		for(int i=0; i<objects.length; i++) s+= "{"+i+"} ";
		return s;
	}
}
