package edu.umass.cs.wiki;

import edu.umass.cs.gnsserver.main.GNSConfig;



public enum WikiConstants {
	

	GNSSERVER_1LOCAL_PROPERTIES_FILE("conf/gnsserver.1local.properties"), 
	

	DEFAULT_PROPERTIES_FILE(GNSSERVER_1LOCAL_PROPERTIES_FILE),
	

	EMAIL_VERIFICATION_OPTION(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION)
	;
	
	final Object name;
	
	WikiConstants(Object name) {
		this.name = name;
	}
	

	public String getString() {
		return this.name.toString();
	}
}
