package edu.umass.cs.wiki;

import edu.umass.cs.gnsserver.main.GNSConfig;

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
	DEFAULT_PROPERTIES_FILE(GNSSERVER_1LOCAL_PROPERTIES_FILE),
	
	/**
	 * Email verification option.
	 */
	EMAIL_VERIFICATION_OPTION(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION)
	;
	
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
