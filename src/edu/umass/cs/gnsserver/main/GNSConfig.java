
package edu.umass.cs.gnsserver.main;

import java.io.FileInputStream;
import java.io.IOException;
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
import java.util.logging.Logger;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;


public class GNSConfig {


  public static enum GNSC implements Config.ConfigurableEnum,
          Config.Disableable {

    ENABLE_SECRET_KEY(true),

    ENABLE_DISKMAP(true),

    IN_MEMORY_DB(false),

    EXECUTE_NOOP_ENABLED(false),

    //@Deprecated
    //ACCOUNT_VERIFICATION_SECRET(EXPOSED_SECRET),


    TESTING_MODE(false),

    ADMIN_FILE("conf/admin.file"),

    STALE_COMMAND_INTERVAL_IN_MINUTES(30),

    //
    // REMOTE QUERY TIMEOUTS
    //

    REPLICA_READ_TIMEOUT(5000),

    REPLICA_UPDATE_TIMEOUT(8000),

    RECON_TIMEOUT(4000),


    SELECT_REQUEST_TIMEOUT(5000),


    MONGO_PORT(27017),

    NOSQL_RECORDS_CLASS("edu.umass.cs.gnsserver.database.MongoRecords"),
    //
    // ACCOUNT GUIDS
    //

    ACCOUNT_GUID_MAX_SUBGUIDS(300000),

    ACCOUNT_GUID_MAX_ALIASES(100),
    //
    // EMAIL VERIFICATION
    //

    ENABLE_EMAIL_VERIFICATION(true),

    EMAIL_VERIFICATION_TIMEOUT_IN_HOURS(24),

    //FIXME:  - currently only used by the ACS; will be disabled soon
    //@Deprecated // DO NOT USE; will be going away shortly
    //ENABLE_EMAIL_VERIFICATION_SALT(true),

    APPLICATION_NAME("an application"),

    SUPPORT_EMAIL("admin@gns.name"),

    ADMIN_EMAIL("admin@gns.name"),

    ADMIN_PASSWORD(""),

    INCLUDE_CLI_NOTIFICATION(false),

    STATUS_URL("http://127.0.0.1/status?alias="),
    //
    // HTTP Service
    //

    HTTP_SERVER_CLEAR_PORT(8080),

    HTTP_SERVER_SECURE_PORT(9080),

    HTTP_SERVER_GNS_URL_PATH("GNS"),
    //
    // LOCAL NAME SERVER SETUP
    //

    LOCAL_NAME_SERVER_NODES(NONE),
    //
    // Domain Name Service
    //

    DNS_SERVER_NODES(NONE),

    DNS_GNS_ONLY(false),

    GNS_SERVER_IP(NONE),

    DNS_ONLY(false),
    //
    // Contect Name Service
    //

    ENABLE_CNS(false),

    CNS_NODE_ADDRESS(NONE),

    PRIVATE_KEY_ALIAS("node100"),

    USE_OLD_ACL_MODEL(false),

    DISABLE_MULTI_SERVER_HTTP(false),

    DISABLE_ACTIVE_CODE(true);

    final Object defaultValue;
    final boolean unsafeTestingOnly;

    GNSC(Object defaultValue) {
      this(defaultValue, false);
    }

    GNSC(Object defaultValue, boolean testingOnly) {
      this.defaultValue = defaultValue;
      this.unsafeTestingOnly = testingOnly;
    }


    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }

    private static Class<?> noSqlRecordsclass = getNoSqlRecordsClass();


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

    @Override
    public String getConfigFileKey() {
      return "gigapaxosConfig";
    }


    @Override
    public String getDefaultConfigFile() {
      return "gns.server.properties";
    }

    @Override
    public boolean isDisabled() {
      return UNSAFE_TESTING ? false : this.unsafeTestingOnly;
    }
  }


  public static final String NONE = "_not_specified_";

  private static boolean UNSAFE_TESTING = false;

  private final static Logger LOGGER = Logger.getLogger(GNSConfig.class.getName());


  public static Logger getLogger() {
    return LOGGER;
  }


  public static final int DEFAULT_ACTIVE_REPLICA_PORT = 24403;


  public static final int DEFAULT_RECONFIGURATOR_PORT = 2178;


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

  private static final String getPrivateKeyAsString() {
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
}
