package edu.umass.cs.gigapaxos;

import java.io.IOException;

import edu.umass.cs.utils.Config;

/**
 * @author arun
 * 
 *         A container class for storing gigapaxos config parameters as an enum.
 */
public class PaxosConfig {
	/**
	 * Default file name for gigapaxos config parameters.
	 */
	public static final String DEFAULT_GIGAPAXOS_CONFIG_FILE = "gigapaxos.properties";
	/**
	 * Gigapaxos config file information can be specified using
	 * -DgigapaxosConfig=<filename> as a JVM argument.
	 */
	public static final String GIGAPAXOS_CONFIG_FILE_KEY = "gigapaxosConfig";

	/**
	 * Loads from a default file or file name specified as a system property.
	 */
	public static void load() {
		try {
			Config.register(PC.class, GIGAPAXOS_CONFIG_FILE_KEY,
					DEFAULT_GIGAPAXOS_CONFIG_FILE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * All gigapaxos config parameters that can be specified via a configuration
	 * file.
	 */
	public static enum PC implements Config.DefaultValueEnum {
		/**
		 * Verbose debugging and request instrumentation
		 */
		DEBUG(false),
		/**
		 * True means no persistent logging
		 */
		DISABLE_LOGGING(false), ;

		final Object defaultValue;

		PC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}
	}
}
