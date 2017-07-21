/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsserver.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.URL;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.console.commands.Select;
import edu.umass.cs.gnsserver.extensions.sanitycheck.NullSanityCheck;
import edu.umass.cs.gnsserver.gnsapp.AbstractSelector;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.utils.Config;

/**
 * @author arun, westy
 *
 * Contains config parameters for the nameservers and logging
 * functionality. This file should be used for server-only properties.
 * Client properties are {@link GNSClientConfig}.
 */
public class GNSConfig {

  /**
   * The GNS Config.
   */
  public static enum GNSC implements Config.ConfigurableEnum,
          Config.Disableable {
    /**
     * Enables secret key communication that is ~180x faster at signing and
     * ~8x faster at verification. True by default as there is no reason to
     * not support it at the server. Except that as of Fall 2016 the iOS
     * client doesn't currently support this.
     */
    ENABLE_SECRET_KEY(true),
    /**
     * Uses DiskMapRecords if enabled.
     */
    ENABLE_DISKMAP(true),
    /**
     * Completely turns off mongo or other persistent database provided
     * DiskMap is also enabled.
     */
    IN_MEMORY_DB(false),
    /**
     * If enabled, the GNS will cache and return the same value for reads.
     *
     * Code-breaking if enabled. Meant only for instrumentation.
     */
    EXECUTE_NOOP_ENABLED(false),
    /**
     * True means testing mode, which allows some unsafe operations. Default
     * is false for production mode.
     */
    TESTING_MODE(false),
    /**
     * Commands older than this value (send by a client more than this
     * interval ago) will be rejected by the server.
     */
    STALE_COMMAND_INTERVAL_IN_MINUTES(30),
    /**
     * The default port used by mongo. 27017 is the default mongo uses.
     */
    //
    // REMOTE QUERY TIMEOUTS
    //
    /**
     * The timeout for synchronous reads in Remote Query.
     */
    REPLICA_READ_TIMEOUT(5000),
    /**
     * The timeout for synchronous writes in Remote Query.
     */
    REPLICA_UPDATE_TIMEOUT(8000),
    /**
     * The timeout for synchronous queries to a reconfigurator in Remote
     * Query.
     */
    RECON_TIMEOUT(4000),
    /**
     * The timeout for select queries.
     */
    /* FIXME: arun: need to determine this timeout systematically, not an ad
		 * hoc constant. */
    SELECT_REQUEST_TIMEOUT(5000),
    /**
     *
     */
    MONGO_PORT(27017),
    /**
     * The class used to represent NoSQL records.
     */
    NOSQL_RECORDS_CLASS("edu.umass.cs.gnsserver.database.MongoRecords"),
    //
    // ACCOUNT GUIDS
    //
    /**
     * The maximum number of subguids allowed in an account guid. The upper
     * limit on this is currently dictated by mongo's 16MB document limit.
     * https://docs.mongodb.org/manual/reference/limits/#bson-documents
     */
    ACCOUNT_GUID_MAX_SUBGUIDS(300000),
    /**
     * The maximum number of HRN aliases allowed for a guid.
     */
    ACCOUNT_GUID_MAX_ALIASES(100),
    //
    // EMAIL VERIFICATION
    //
    /**
     * If enabled, email verification will be used when account GUIDs are
     * created.
     *
     */
    ENABLE_EMAIL_VERIFICATION(true),
    /**
     * The amount of time an email verification code is valid.
     */
    EMAIL_VERIFICATION_TIMEOUT_IN_HOURS(24),
    /**
     * The name of the application that is used when sending a verification
     * email.
     */
    APPLICATION_NAME("an application"),
    /**
     * The name of the email reply to that is used when sending a
     * verification email. Should be a valid email address.
     */
    SUPPORT_EMAIL("admin@gns.name"),
    /**
     * The name of the email that is used to log into the email relay server
     * when sending a verification email if the local emailer fails. Should
     * be a valid email address.
     */
    ADMIN_EMAIL("admin@gns.name"),
    /**
     * The the email account password that is used when sending a
     * verification email if the local emailer fails. Should be the empty
     * string if you don't want to use the relay emailer.
     */
    ADMIN_PASSWORD(""),
    /**
     * Does the verification email include text about validating using the
     * CLI.
     */
    INCLUDE_CLI_NOTIFICATION(false),
    /**
     * A url that will lookup status for the application when passed a HRN.
     */
    STATUS_URL("http://127.0.0.1/status?alias="),
    //
    // HTTP Service
    //
    /**
     * The URL path used by the HTTP server.
     */
    HTTP_SERVER_GNS_URL_PATH("GNS"),
    //
    // LOCAL NAME SERVER SETUP
    //
    /**
     * Set to "all" or a node id if you want to start an instance of the
     * LocalNameServer when the app starts.
     */
    LOCAL_NAME_SERVER_NODES(NONE),
    //
    // Domain Name Service
    //
    /**
     * For the DNS service set to "all" or a node id if you want to start
     * the DNS server on the respective nodes when the app starts.
     */
    DNS_SERVER_NODES(NONE),
    /**
     * Specifies the IP address to send DNS queries to. Does not apply if
     * {@link GNSC#DNS_GNS_ONLY} is set to true.
     */
    DNS_UPSTREAM_SERVER_IP("8.8.8.8"),
    /**
     * For the DNS service set to true if you want the DNS server to not
     * lookup records using DNS (will only lookup records in the GNS).
     */
    DNS_GNS_ONLY(false),
    /**
     *  If the DNS server is running as a managed DNS server, this value is true. If it's a recursive local DNS server, then
     *   set it to false.
     */
    IS_MANAGED_DNS(false),
    /**
     * For the DNS service the name of the GNS server to forward GNS
     * requests.
     */
    GNS_SERVER_IP(NONE),
    /**
     * For the DNS service set to true if you want the DNS server to forward
     * requests to DNS and GNS servers.
     */
    DNS_ONLY(false),
    //
    // Contect Name Service
    //
    /**
     * If set to true enables update forwarding to CNS
     */
    ENABLE_CNS(false),
    /**
     * Ip address:port of one node of CNS. If ENABLE_CNS is set to true then
     * this option should definitely be set.
     */
    CNS_NODE_ADDRESS(NONE),
    /**
     * The alias of the private key in the java keyStore.
     */
    PRIVATE_KEY_ALIAS("node100"),
    /**
     * Server Admin port offset relative to reconfigurator port.
     */
    // Make sure this is different than all the other offsets.
    SERVER_ADMIN_PORT_OFFSET(197),
    /**
     * Collating Admin port offset relative to reconfigurator port.
     */
    // Make sure this is different than all the other offsets.
    COLLATING_ADMIN_PORT_OFFSET(297),
    /**
     * Turn off active code handling. Default is true.
     * Temporary - The use of this will go away at some point.
     */
    DISABLE_ACTIVE_CODE(true),
    /**
     * The class name to use for doing sanity checks while updating GNS
     * record. Must extend {@link edu.umass.cs.gnsserver.extensions.sanitycheck.AbstractSanityCheck}
     */
    SANITY_CHECKER(NullSanityCheck.class.getName()),
    
    /**
     * This macro specifies the reconfigure on active change policy
     * for GUIDs. {@link edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.ReconfigureUponActivesChange} 
     * enum specifies the set of values. 
     * The default value is {@link edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.ReconfigureUponActivesChange#DEFAULT},
     * which means the GUIDs are not reconfigured on change of actives.
     */
    RECONFIGURE_ON_ACTIVE_CHANGE_POLICY(ReconfigurationRecord.ReconfigureUponActivesChange.DEFAULT),
    
    /**
     * Class name of select implementation.
     */
    ABSTRACT_SELECTOR(Select.class.getCanonicalName()),
    ;

    final Object defaultValue;
    final boolean unsafeTestingOnly;

    GNSC(Object defaultValue) {
      this(defaultValue, false);
    }

    GNSC(Object defaultValue, boolean testingOnly) {
      this.defaultValue = defaultValue;
      this.unsafeTestingOnly = testingOnly;
    }

    /**
     *
     *
     * @return the default value
     */
    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }

    private static Class<?> noSqlRecordsclass = getNoSqlRecordsClass();

    /**
     * @return DB class
     */
    public static Class<?> getNoSqlRecordsClass() {
      if (noSqlRecordsclass == null) {
        // arun: in-memory DB => DiskMap
        noSqlRecordsclass = getClassSuppressExceptions(Config
                .getGlobalBoolean(GNSC.ENABLE_DISKMAP)
                || Config.getGlobalBoolean(GNSConfig.GNSC.IN_MEMORY_DB) ? "edu.umass.cs.gnsserver.database.DiskMapRecords"
                : Config.getGlobalString(GNSC.NOSQL_RECORDS_CLASS));
      }
      return noSqlRecordsclass;
    }

    private static Class<?> getClassSuppressExceptions(String className) {
      Class<?> clazz = null;
      try {
        if (className != null && !"null".equals(className)) {
          clazz = Class.forName(className);
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      return clazz;
    }

    // FIXME: a better default name?
    /**
     *
     * @return the config file key
     */
    @Override
    public String getConfigFileKey() {
      return "gigapaxosConfig";
    }

    /**
     *
     * @return the default config file
     */
    @Override
    public String getDefaultConfigFile() {
      return "gns.server.properties";
    }

    @Override
    public boolean isDisabled() {
      return UNSAFE_TESTING ? false : this.unsafeTestingOnly;
    }
  }

  /**
   * Default value used for some servers and ip addresses.
   */
  public static final String NONE = "_not_specified_";

  private static boolean UNSAFE_TESTING = false;

  private final static Logger LOGGER = Logger.getLogger(GNSConfig.class.getName());

  /**
   * Returns the master GNS logger.
   *
   * @return the master GNS logger
   */
  public static Logger getLogger() {
    return LOGGER;
  }

  /**
   * The default active replica server port number. Currently unused because
   * of old, hacky way of computing port numbers.
   */
  public static final int DEFAULT_ACTIVE_REPLICA_PORT = 24403;

  /**
   * The default reconfigurator server port number.
   */
  public static final int DEFAULT_RECONFIGURATOR_PORT = 2178;

  /**
   * Attempts to look for a MANIFEST file in that contains the Build-Version
   * attribute.
   *
   * @return a build version
   */
  public static String readBuildVersion() {
    String result = null;
    Enumeration<URL> resources = null;
    try {
      resources = GNSConfig.class.getClassLoader().getResources(
              "META-INF/MANIFEST.MF");
    } catch (IOException E) {
      // handle
    }
    if (resources != null) {
      while (resources.hasMoreElements()) {
        try {
          Manifest manifest = new Manifest(resources.nextElement()
                  .openStream());
          // check that this is your manifest and do what you need or
          // get the next one
          Attributes attr = manifest.getMainAttributes();
          result = attr.getValue("Build-Version");
        } catch (IOException E) {
          // handle
        }
      }
    }
    return result;
  }

  /**
   * @return Private key from java keyStore
   * @throws KeyStoreException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   * @throws IOException
   * @throws UnrecoverableKeyException
   */
  public static final PrivateKey getPrivateKey() throws KeyStoreException,
          NoSuchAlgorithmException, CertificateException, IOException,
          UnrecoverableKeyException {
    String keyStoreFile = System.getProperty("javax.net.ssl.keyStore");
    String keyStorePassword = System
            .getProperty("javax.net.ssl.keyStorePassword");
    FileInputStream is = new FileInputStream(keyStoreFile);

    KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
    keystore.load(is, keyStorePassword.toCharArray());
    String alias = Config.getGlobalString(GNSC.PRIVATE_KEY_ALIAS);
    Key key = keystore.getKey(alias, keyStorePassword.toCharArray());
    if (key instanceof PrivateKey) {
      return (PrivateKey) key;
    }
    return null;
  }

  private static String getPrivateKeyAsString() {
    try {
      return String.format("%040x", new BigInteger(1, getPrivateKey()
              .getEncoded())).substring(0, 32);
    } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static final String EXPOSED_SECRET = "EXPOSED_SECRET";
  private static String internalOpSecret = EXPOSED_SECRET;

  /**
   * @return Secret used for internal operations.
   */
  public static final String getInternalOpSecret() {
    String keyStoreFile = System.getProperty("javax.net.ssl.keyStore");
    if (keyStoreFile == null) {
      return internalOpSecret;
    }
    try {
      if (!internalOpSecret.equals(EXPOSED_SECRET)) {
        return internalOpSecret;
      } else // one-time only
      {
        internalOpSecret = getPrivateKeyAsString();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return internalOpSecret;
  }
  
	private static AbstractSelector selector = null;

	/**
	 * @return Select implementation.
	 */
	public synchronized static final AbstractSelector getSelector() {
		if (selector != null)
			return selector;
		// else
		Class<?> clazz = null;
		try {
			clazz = (Class.forName(Config
					.getGlobalString(GNSConfig.GNSC.ABSTRACT_SELECTOR)));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		if (clazz != null)
			try {
				selector = (AbstractSelector) (clazz.getConstructor()
						.newInstance());
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				getLogger()
						.log(Level.WARNING,
								"{0} unable to instantiate selector {1}; using default selector",
								new Object[] {
										GNSConfig.class.getName(),
										Config.getGlobalString(GNSConfig.GNSC.ABSTRACT_SELECTOR) });
				e.printStackTrace();
			}
		if (selector == null)
			try {
				selector = (AbstractSelector) (edu.umass.cs.gnsserver.gnsapp.Select.class
						.getConstructor().newInstance());
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		// default Select has default constructor
		assert(selector!=null);
		return selector;
	}
}
