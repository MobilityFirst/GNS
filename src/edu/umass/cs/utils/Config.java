package edu.umass.cs.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author arun
 *
 */
public class Config extends Properties {
	private static final Logger log = Logger.getLogger(Config.class.getName());

	/**
	 * 
	 */
	private static final long serialVersionUID = 4637861931101543278L;

	/**
	 *
	 */
	public static interface DefaultValueEnum {
		/**
		 * @return Default value of this object.
		 */
		public Object getDefaultValue();
	};

	/**
	 * An interface that makes it convenient to use commons-CLI style option
	 * while defining all fields as an enum at one place.
	 * 
	 */
	public static interface CLIOption extends DefaultValueEnum {

		/**
		 * @return String name of the option.
		 */
		public String getOptionName();

		/**
		 * @return String argument name if it takes one.
		 */
		public String getArgName();

		/**
		 * @return True if it takes an argument.
		 */
		public boolean hasArg();

		/**
		 * @return Description of the option.
		 */
		public String getDescriptio();
	};

	/**
	 * Returns a {@link Config} object with properties read from
	 * {@code configFile}
	 * 
	 * @param configFile
	 * @throws IOException
	 */
	Config(String configFile) throws IOException {
		this.readConfigFile(configFile);
	}

	Config() {
	}

	private static final HashMap<Class<?>, Config> configMap = new HashMap<Class<?>, Config>();

	/**
	 * Registering an enum type with a config file is convenient to statically
	 * retrieve configuration values, i.e., without having to pass a Config
	 * object around.
	 * 
	 * @param type
	 *            The declaring class of the enum type.
	 * @param configFile
	 * @return Previously registered config object if any for this type.
	 * @throws IOException
	 * 
	 */
	public static Config register(Class<?> type, String configFile)
			throws IOException {
		try {
			return configMap.put(type, new Config(configFile));
		} catch (IOException ioe) {
			// we still use defaults
			configMap.put(type, new Config());
			log.warning(Config.class.getSimpleName() + " unable to find file "
					+ configFile + "; using default values for type " + type);
			throw ioe;
		}
	}

	/**
	 * Will first look for the file name specified in systemProperty and then
	 * try the configFile.
	 * 
	 * @param type
	 * @param defaultConfigFile
	 * @param systemPropertyKey
	 * @return Previously registered config object if any for this type.
	 * @throws IOException
	 */
	public static Config register(Class<?> type, String systemPropertyKey,
			String defaultConfigFile) throws IOException {
                String configFile = System.getProperty(systemPropertyKey) != null 
                  ? System.getProperty(systemPropertyKey) :
                  defaultConfigFile;
		try {
                  return configMap.put(type, new Config(configFile));
		} catch (IOException ioe) {
			// we still use defaults
			configMap.put(type, new Config());
			log.warning(Config.class.getSimpleName() + " unable to find file "
					+ configFile + "; using default values for type "
					+ type);
			throw ioe;
		}
	}

	/**
	 * @param type
	 * @return Config object registered for type.
	 */
	public static Config getConfig(Class<?> type) {
		return configMap.get(type);
	}

	/**
	 * @param field
	 * @return The configuration value of field if its type was previously
	 *         registered. If it was not previously registered, an exception
	 *         will be thrown.
	 */
	public static Object getGlobal(Enum<?> field) {
		assert (configMap.get(field.getDeclaringClass()).get(field) != null) : field
				+ " : "
				+ configMap.get(field.getDeclaringClass())
				+ " : "
				+ configMap.get(field.getDeclaringClass())
						.get(field);
		if (configMap.containsKey(field.getDeclaringClass()))
			return configMap.get(field.getDeclaringClass()).get(field);
		throw new RuntimeException("No matching "
				+ Config.class.getSimpleName() + " registered for field "
				+ field);
	}

	/**
	 * @param field
	 * @return Boolean config parameter.
	 */
	public static boolean getGlobalBoolean(Enum<?> field) {
		return Boolean.valueOf(getGlobal(field).toString().trim());
	}

	/**
	 * @param field
	 * @return Integer config parameter.
	 */
	public static int getGlobalInt(Enum<?> field) {
		return Integer.valueOf(getGlobal(field).toString().trim());
	}

	/**
	 * @param field
	 * @return Long config parameter.
	 */
	public static long getGlobalLong(Enum<?> field) {
		return Long.valueOf(getGlobal(field).toString().trim());
	}

	/**
	 * @param field
	 * @return Double config parameter.
	 */
	public static double getGlobalDouble(Enum<?> field) {
		return Double.valueOf(getGlobal(field).toString().trim());
	}

	/**
	 * @param field
	 * @return Short config parameter.
	 */
	public static short getGlobalShort(Enum<?> field) {
		return Short.valueOf(getGlobal(field).toString().trim());
	}

	/**
	 * @param field
	 * @return String config parameter.
	 */
	public static String getGlobalString(Enum<?> field) {
		return (getGlobal(field).toString());
	}

	/**
	 * @param field
	 * @return The configuration value of {@code field}.
	 */
	public Object get(Enum<?> field) {
		if (this.containsKey(field.toString()))
			return this.get(field.toString());
		else
			return ((Config.DefaultValueEnum) field).getDefaultValue();
	}

	/**
	 * @param field
	 * @return Boolean value of field.
	 */
	public boolean getBoolean(Enum<?> field) {
		return Boolean.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Integer value of field.
	 */
	public int getInt(Enum<?> field) {
		return Integer.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public long getLong(Enum<?> field) {
		return Long.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public short getShort(Enum<?> field) {
		return Short.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public String getString(Enum<?> field) {
		return (get(field).toString());
	}

	private void readConfigFile(String configFile) throws IOException {
		InputStream is = new FileInputStream(configFile);
		this.load(is);
		for (Object prop : this.keySet()) {
			log.fine("Set property " + prop + "="
					+ this.getProperty(prop.toString()));
		}
	}

	private static enum Fields implements Config.DefaultValueEnum {
		FIRST(1), SECOND("Monday"), THIRD(30000000000L);

		final Object value;

		Fields(Object value) {
			this.value = value;
		}

		public Object getDefaultValue() {
			return value;
		}

	};

	private static void testMethod1() {
		System.out.println("Default value of " + Fields.THIRD + " is "
				+ Fields.THIRD.getDefaultValue()
				+ " and the configured value is "
				+ Config.getGlobal(Fields.THIRD));
	}

	private static void testMethod2() throws IOException {
		assert (Fields.SECOND.getDefaultValue().equals("Monday"));
		Config config = new Config(
				"/Users/arun/GNS/src/edu/umass/cs/utils/config.properties");
		System.out.println(Fields.class);

		// asserting global config matches specific config
		for (Fields field : Fields.values())
			assert (Config.getGlobal(field).equals(config.get(field)));

		// specific field value assertions
		assert (Fields.FIRST.getDefaultValue().toString().equals(Config
				.getGlobal(Fields.FIRST)));
		assert (Fields.SECOND.getDefaultValue().toString().toUpperCase()
				.equals(config.get(Fields.SECOND)));
		assert (!Fields.THIRD.getDefaultValue().toString()
				.equals(config.get(Fields.THIRD)));
	}

	/**
	 * Example of defining an enum that can be used to define commons-cli
	 * options.
	 * 
	 */
	static enum OptionFields implements CLIOption {
		// not optionable
		FIRST(23),

		// optionable without an argName
		SECOND(Boolean.TRUE, "secondFlag", true, "a flag that determines blah"),

		// optionable with an argName
		THIRD("THREE", "thirdOption", false, "the third rock", "file"),

		// optionable with argName=value
		NINTH("9", "ninthOption", false, "ninth option used for all else",
				"file", true);

		final Object defaultValue;
		final String optionName;
		final boolean hasArg;
		final String description;
		final String argName;
		final boolean withValueSeparator;

		OptionFields(Object defaultValue) {
			this(defaultValue, null, false, null, null);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description) {
			this(defaultValue, optionName, hasArg, description, null);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description, String argName) {
			this(defaultValue, optionName, hasArg, description, argName, false);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description, String argName, boolean withValueSeparator) {
			this.defaultValue = defaultValue;
			this.optionName = optionName;
			this.hasArg = hasArg;
			this.description = description;
			this.argName = argName;
			this.withValueSeparator = withValueSeparator;
		}

		@Override
		public String getOptionName() {
			return this.optionName;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		@Override
		public String getArgName() {
			return this.argName;
		}

		@Override
		public boolean hasArg() {
			return this.hasArg;
		}

		@Override
		public String getDescriptio() {
			return this.description;
		}
	};

	/**
	 * @return Local logger.
	 */
	public static Logger getLogger() {
		return log;
	}

	/**
	 * Runtime usage of Config through explicit object instance creation.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Util.assertAssertionsEnabled();
		Config.register(Fields.class,
				"/Users/arun/GNS/src/edu/umass/cs/utils/config.properties");
		testMethod1(); // static usage option
		testMethod2(); // object instance option

		System.out.println("[success]");
	}
}
