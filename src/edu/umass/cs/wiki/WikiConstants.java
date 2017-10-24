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
	DEFAULT_PROPERTIES_FILE(GNSSERVER_1LOCAL_PROPERTIES_FILE.getConstantValue()),
	
	/**
	 * Email verification option.
	 */
	EMAIL_VERIFICATION_OPTION(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION)
	;
	
	final Object value;
	
	WikiConstants(Object value) {
		this.value = value;
	}
	
	/**
	 * @return The string value corresponding to the name
	 */
	public String getConstantValue() {
		return this.value.toString();
	}

    /**
     * @return Stringified name.
     */
    public String getConstantName() {
        return this.name();
    }

}