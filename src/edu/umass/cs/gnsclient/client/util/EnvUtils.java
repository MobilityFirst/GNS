package edu.umass.cs.gnsclient.client.util;


/**
 * This class contains utility methods to set up environment parameters for
 * GNS clients or servers. These need to be consistent with gigapaxos startup
 * scripts.
 *
 * @author arun
 */
public class EnvUtils {
	public static final String HOME = System.getProperty("user.home");
	public static final String GNS_HOME = HOME + "/" + "GNS" + "/";

	static {
		setProperties();
	}

	public static enum DefaultClientProps {
		GIGAPAXOS_CONFIG("gigapaxosConfig", "gnsclient.1local.properties",
			true),

		// needed only for admin clients
		KEYSTORE("javax.net.ssl.keyStore", "keyStore.jks", true),

		// needed only for admin clients
		KEYSTORE_PASSWORD("javax.net.ssl.keyStorePassword", "qwerty"),

		TRUSTSTORE("javax.net.ssl.trustStore", "trustStore.jks", true),

		TRUSTSTORE_PASSWORD("javax.net.ssl.trustStorePassword", "qwerty"),

		CLIENT_KEY_DB("clientKeyDB", "clientKeyDB");


		final String key;
		final String value;
		final boolean isFile;

		DefaultClientProps(String key, String value, boolean isConfFile) {
			this.key = key; this.value = value; this.isFile = isConfFile;
		}

		DefaultClientProps(String key, String value) {
			this(key, value, false);
		}

		public String getkey() {
			return key;
		}
	}

	private static void setProperties() {
		for (DefaultClientProps prop : DefaultClientProps.values()) {
			setProperty(prop.key, prop.isFile ? getPath(prop.value) : prop
				.value);
		}
	}

	private static String getPath(String value) {
		return GNS_HOME + "conf/" + value;
	}


	private static void setProperty(String key, String defaultVal) {
		String val = System.getProperty(key);
		if (val == null) System.setProperty(key, defaultVal);
	}
}
