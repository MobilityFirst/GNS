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

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
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
   *
   */
  public static enum GNSC implements Config.ConfigurableEnum {

    /**
     * Enables secret key communication that is ~180x faster at signing and
     * ~8x faster at verification. True by default as there is no reason to
     * not support it at the server.
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
     * A secret shared between the server and a trusted client in order to circumvent
     * account verification. Must be changed using properties file if manual
     * verification is disabled.
     *
     * Security requirements:
     *
     * (1) The security of the account verification depends on the secrecy of
     * this secret, so the default value must be changed via the properties
     * file in a production setting.
     *
     * (2) SERVER_AUTH SSL must be enabled between clients and servers.
     */
    VERIFICATION_SECRET("EXPOSED_SECRET"),
    /**
     * Secret inserted into commands that normally need authentication
     * but are being issued by a trusted internal server. We need this
     * because a server can not generate a signature on behalf of a client
     * because only the client has the private key, so if a client issues a
     * multi-step transactional operation that needs to be conducted by an
     * active replica server, the server needs to use this mechanism.
     *
     * Security requirements:
     *
     * (1) The security of most everything in the GNS depends on the secrecy of
     * this secret, so the default value must be changed via the properties
     * file in a production setting.
     *
     * (2) MUTUAL_AUTH SSL must be enabled between the servers.
     *
     * TODO: We might as well set this at bootstrap time to a hash of the
     * contents of keyStore.jks as the contents of that file are meant to
     * be secret anyway and the two have the exact same trust relationship.
     */
    INTERNAL_OP_SECRET("EXPOSED_SECRET"),
    /**
     * This file contains secrets for authenticating admin commands issued
     * by a trusted client.
     *
     * Security requirements:
     *
     * (1) The contents of this file or the
     *
     * (2) The security of administrative settings (e.g., quote limits)
     * depends on the secrecy of this secret, so the default values of
     * secrets must be changed via the properties file in a production
     * setting.
     *
     * (3) SERVER_AUTH SSL must be enabled between clients and servers.
     */
    ADMIN_FILE("conf/admin.file"),
    /**
     * Commands older than this value (send by a client more than this
     * interval ago) will be rejected by the server.
     */
    STALE_COMMAND_INTERVAL_IN_MINUTES(30),
    /**
     * The default port used by mongo. 27017 is the default mongo uses.
     */
    MONGO_PORT(27017),
    /**
     * The class used to represent NoSQL records.
     */
    NOSQL_RECORDS_CLASS("edu.umass.cs.gnsserver.database.MongoRecords"),
    /**
     * If enabled, email verfication will be used when account guids are created.
     */
    ENABLE_EMAIL_VERIFICATION(true),
    /**
     * The amount of time an email verification code is valid.
     */
    EMAIL_VERIFICATION_TIMEOUT_IN_HOURS(24),
    /**
     * If enabled, email salt will be added to the EMAIL_VERIFICATION code.
     * This is needed so we can disable salting in the case where we're using email verification and
     * shared secret verification simultaneously.
     */
    ENABLE_EMAIL_VERIFICATION_SALT(true),
    /**
     * Disables the use of the local emailer when sending verification messages.
     * Needed for hosts where the emailer is badly configured and eats outgoing email.
     */
    DONT_TRY_LOCAL_EMAIL(false),
    /**
     * The name of the application that is used when sending a verification email.
     */
    APPLICATION_NAME("an application"),
    /**
     * The name of the email reply to that is used when sending a verification email.
     * Should be a valid email address.
     */
    SUPPORT_EMAIL("admin@gns.name"),
    /**
     * The name of the email reply to that is used when sending a verification email.
     * Should be a valid email address.
     */
    SUPPORT_PASSWORD("deadDOG8"),
    /**
     * Does the verification email include text about validating using the CLI.
     */
    INCLUDE_CLI_NOTIFICATION(false),
    /**
     * Set to "all" or a node id if you want to start an instance of the LocalNameServer when the app starts.
     */
    LOCAL_NAME_SERVER_NODES("none"),
    /**
     * For the DNS service set to "all" or a node id if you want to start the DNS server when the app starts.
     */
    DNS_SERVER_NODES("none"),
    /**
     * For the DNS service set to true if you want the DNS server to not lookup
     * records using DNS (will only lookup records in the GNS).
     */
    DNS_GNS_ONLY(false),
    /**
     * For the DNS service the name of the GNS server to forward GNS requests.
     */
    GNS_SERVER_IP("none"),
    /**
     * For the DNS service set to true if you want the DNS server to forward requests to DNS and GNS servers.
     */
    DNS_ONLY(false);

    final Object defaultValue;

    GNSC(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }

    /**
     *
     * @return true if email verfication will be used when account guids are created
     */
    public static boolean isEmailAuthenticationEnabled() {
      return Config.getGlobalBoolean(GNSC.ENABLE_EMAIL_VERIFICATION);
    }

    /**
     *
     * @return true if email salt will be added to the EMAIL_VERIFICATION code.
     */
    public static boolean isEmailAuthenticationSaltEnabled() {
      return Config.getGlobalBoolean(GNSC.ENABLE_EMAIL_VERIFICATION_SALT);
    }

    /**
     *
     * @return true if the use of the local mailer is disabled when sending verification messages
     */
    public static boolean isDontTryLocalEmail() {
      return Config.getGlobalBoolean(GNSC.DONT_TRY_LOCAL_EMAIL);
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
                || // in-memory DB force-implies DiskMap
                Config.getGlobalBoolean(GNSConfig.GNSC.IN_MEMORY_DB) ? "edu.umass.cs.gnsserver.database.DiskMapRecords"
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
  }

  /* FIXME: arun: some parameters below are not relevant any more and need to
	 * go. I removed the ones not being using static analysis. */
  /**
   * The default starting port.
   */
  public static final int DEFAULT_STARTING_PORT = 24400;
  /**
   * The URL path used by the HTTP server.
   */
  public static final String GNS_URL_PATH = "GNS";
  // Useful for testing with resources in conf/testCodeResources if using
  // "import from build file in IDE". Better way to do this?
  /**
   * Hack.
   */
  public static final String WESTY_GNS_DIR_PATH = "/Users/westy/Documents/Code/GNS";
  /**
   * The maximum number of HRN aliases allowed for a guid.
   */
  public static int MAXALIASES = 100;
  /**
   * The maximum number of subguids allowed in an account guid. The upper
   * limit on this is currently dictated by mongo's 16MB document limit.
   * https://docs.mongodb.org/manual/reference/limits/#bson-documents
   */
  public static int MAXGUIDS = 300000;

  // This is designed so we can run multiple NSs on the same host if needed
  /**
   * Master port types.
   */
  public enum PortType {
    /**
     * Port used to send requests to an active replica.
     */
    ACTIVE_REPLICA_PORT(0),
    /**
     * Port used to requests to a reconfigurator replica.
     */
    RECONFIGURATOR_PORT(1),
    // Reordered these so they work with the new GNSApp
    /**
     * Port used to send requests to a name server.
     */
    NS_TCP_PORT(3), // TCP port at name servers
    /**
     * Port used to send admin requests to a name server.
     */
    NS_ADMIN_PORT(4),
    // sub ports
    /**
     * Port used to send requests to a command pre processor.
     */
    CCP_PORT(6),
    /**
     * Port used to send admin requests to a command pre processor.
     */
    CCP_ADMIN_PORT(7);

    //
    int offset;

    PortType(int offset) {
      this.offset = offset;
    }

    /**
     * Returns the max port offset.
     *
     * @return an int
     */
    public static int maxOffset() {
      int result = 0;
      for (PortType p : values()) {
        if (p.offset > result) {
          result = p.offset;
        }
      }
      return result;
    }

    /**
     * Returns the offset for this port.
     *
     * @return an int
     */
    public int getOffset() {
      return offset;
    }
  }

  /**
   * Controls whether email verification is enabled.
   */
  public static boolean enableEmailAccountVerification = true;
  /**
   * Controls whether signature verification is enabled.
   */
  public static boolean enableSignatureAuthentication = true;
  /**
   * Default query timeout in ms. How long we wait before retransmitting a
   * query.
   */
  public static int DEFAULT_QUERY_TIMEOUT = 2000;
  /**
   * Maximum query wait time in milliseconds. After this amount of time a
   * negative response will be sent back to a client indicating that a record
   * could not be found.
   */
  public static int DEFAULT_MAX_QUERY_WAIT_TIME = 16000; // was 10

  private final static Logger LOG = Logger.getLogger(GNSConfig.class
          .getName());

  /**
   * The default reconfigurator server port number.
   */
  public static final int DEFAULT_RECONFIGURATOR_PORT = 2178;

  /**
   * Returns the master GNS logger.
   *
   * @return the master GNS logger
   */
  public static Logger getLogger() {
    return LOG;
  }

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
}
