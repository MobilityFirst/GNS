package edu.umass.cs.wiki;

/**
 * @author arun
 *
 */

public enum WikiConstants {
	
	/**
	 * Default single-node local properties file.
	 */
	GNSSERVER_1LOCAL_PROPERTIES_FILE("conf/gnsserver.1local.properties"), 
	
	/**
	 * Default properties file.
	 */
	DEFAULT_PROPERTIES_FILE(GNSSERVER_1LOCAL_PROPERTIES_FILE);
	
	final Object name;
	
	WikiConstants(Object name) {
		this.name = name;
	}
	
	/**
	 * @return Stringified name.
	 */
	public String getString() {
		return this.name.toString();
	}
}
