package edu.umass.cs.gns.main;

import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;

import edu.umass.cs.gns.util.Logging;
import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

/**
 * Contains config parameters common to name servers and local name servers, and logging functionality.
 */
public class GNS {

  public static final int DEFAULT_TTL_SECONDS = 0;
  
  public static final int STARTINGPORT = 24400;
  // FIXME: clean this up
  public static final int DEFAULT_LNS_TCP_PORT = 24398;
  public static final int DEFAULT_LNS_PING_PORT = 24397;
  public static final int DEFAULT_LNS_ADMIN_PORT = 24396;
  public static final int DEFAULT_LNS_ADMIN_RESPONSE_PORT = 24395;
  public static final int DEFAULT_LNS_ADMIN_DUMP_RESPONSE_PORT = 24394;
  public static final int LOCAL_FIRST_NODE_PORT = 35000;
  public static final String GNS_URL_PATH = "GNS";
  
  public static final int CLIENTPORT = 24399;
  

  // This is designed so we can run LNS and NS on the same host if needed
  public enum PortType {

    NS_TCP_PORT(0), // TCP port at name servers
    @Deprecated
    LNS_TCP_PORT(1), // TCP port at local name servers
    NS_UDP_PORT(2), // UDP port at local name servers
    @Deprecated
    LNS_UDP_PORT(3), // UDP port at local name servers
    NS_ADMIN_PORT(4),
    @Deprecated
    LNS_ADMIN_PORT(5),
    ADMIN_PORT(-1), // selects either NS_ADMIN_PORT or LNS_ADMIN_PORT
    @Deprecated
    LNS_ADMIN_RESPONSE_PORT(6),
    @Deprecated
    LNS_ADMIN_DUMP_RESPONSE_PORT(7),
    NS_PING_PORT(8),
    @Deprecated
    LNS_PING_PORT(9),
    @Deprecated
    LNS_DBCLIENT_PORT(10),
    PING_PORT(-1); // selects either NS_PING_PORT or DEFAULT_LNS_PING_PORT
    
    //
    int offset;

    PortType(int offset) {
      this.offset = offset;
    }

    public static int maxOffset() {
      int result = 0;
      for (PortType p : values()) {
        if (p.offset > result) {
          result = p.offset;
        }
      }
      return result;
    }

    public int getOffset() {
      return offset;
    }
  }
  public static boolean enableEmailAccountAuthentication = true;
  public static boolean enableSignatureVerification = true;
  /**
   * Number of primary nameservers. Default is 3 *
   */
  public static final int DEFAULT_NUM_PRIMARY_REPLICAS = 3;
  public static int numPrimaryReplicas = DEFAULT_NUM_PRIMARY_REPLICAS;
  public static final ReplicationFrameworkType DEFAULT_REPLICATION_FRAMEWORK = ReplicationFrameworkType.LOCATION;
  /**
   * default query timeout in ms.
   */
  public static int DEFAULT_QUERY_TIMEOUT = 2000;
  /**
   * maximum query wait time in milliseconds
   */
  public static int DEFAULT_MAX_QUERY_WAIT_TIME = 10000; // currently 10 seconds
  
  // THINK CAREFULLY BEFORE CHANGING THESE... THEY CAN CLOG UP YOUR CONSOLE AND GENERATE HUGE LOG FILES
  // IF YOU WANT MORE FINE GRAINED USE OF THESE IT IS SUGGESTED THAT YOU OVERRIDE THEM ON THE COMMAND LINE
  // OR IN A CONFIG FILE
  /**
   * Logging level for main logger
   */
  public static String fileLoggingLevel = "INFO"; // should be INFO for production use
  /**
   * Console output level for main logger
   */
  public static String consoleOutputLevel = "INFO"; //should be INFO for production use
  /**
   * Logging level for stat logger
   */
  public static String statFileLoggingLevel = "FINE"; // leave this at a more verbose level, but really should be INFO for production use
  /**
   * Console output level for stat logger
   */
  public static String statConsoleOutputLevel = "WARNING";  // don't send these to the console normally
  //
  //
  private final static Logger LOGGER = Logger.getLogger(GNS.class.getName());
  public static boolean initRun = false;

  public static Logger getLogger() {
    if (!initRun) {
      System.out.println("Setting Logger console level to " + consoleOutputLevel + " and file level to " + fileLoggingLevel);
      Logging.setupLogger(LOGGER, consoleOutputLevel, fileLoggingLevel, "log" + "/gns.xml");
      initRun = true;
    }
    return LOGGER;
  }
  private final static Logger STAT_LOGGER = Logger.getLogger("STAT_" + GNS.class.getName());
  public static boolean initStatRun = false;

  public static Logger getStatLogger() {

    if (!initStatRun) {
      // don't send these to the console normally
      System.out.println("Setting STAT Logger console level to " + statConsoleOutputLevel + " and file level to " + statFileLoggingLevel);
      Logging.setupLogger(STAT_LOGGER, statConsoleOutputLevel, statFileLoggingLevel, "log" + "/gns_stat.xml");
      initStatRun = true;
    }
    return STAT_LOGGER;
  }

  /**
   * Attempts to look for a MANIFEST file in that contains the Build-Version attribute.
   * 
   * @return 
   */
  public static String readBuildVersion() {
    String result = null;
    try {
      Class clazz = GNS.class;
      String className = clazz.getSimpleName() + ".class";
      String classPath = clazz.getResource(className).toString();
      if (classPath.startsWith("jar")) {
        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1)
                + "/META-INF/MANIFEST.MF";
        Manifest manifest = new Manifest(new URL(manifestPath).openStream());
        Attributes attr = manifest.getMainAttributes();
        result = attr.getValue("Build-Version");
      }
    } catch (IOException e) {
    }
    return result;
  }
}
