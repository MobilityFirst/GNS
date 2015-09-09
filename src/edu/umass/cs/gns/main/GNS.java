package edu.umass.cs.gns.main;

import edu.umass.cs.gns.util.Logging;
import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

/**
 * Contains config parameters for the nameservers and logging functionality.
 */
public class GNS {

  /**
   * The default TTL.
   */
  public static final int DEFAULT_TTL_SECONDS = 0;
  
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
  public static final String ARUN_GNS_DIR_PATH = "/Users/arun/GNS/";
  /**
   * Hack.
   */
  public static final String WESTY_GNS_DIR_PATH = "/Users/westy/Documents/Code/GNS";
  /**
   * The maximum number of HRN aliases allowed for a guid.
   */
  public static int MAXALIASES = 100;
  /**
   * The maximum number of subguids allowed in an account guid.
   */
  public static int MAXGUIDS = 500;
  
  // This is designed so we can run multiple NSs on the same host if needed
  /**
   * Master port types.
   */
  public enum PortType {

    NS_TCP_PORT(0), // TCP port at name servers
    NS_ADMIN_PORT(1),
    NS_PING_PORT(2),
    // sub ports
    ACTIVE_REPLICA_PORT(3),
    RECONFIGURATOR_PORT(4),
    CCP_PORT(5),
    CCP_ADMIN_PORT(6),
    CCP_PING_PORT(7)
    ;
    
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
  /**
   * Controls whether email verification is enabled.
   */
  public static boolean enableEmailAccountVerification = false;
  /**
   * Controls whether signature verification is enabled.
   */
  public static boolean enableSignatureAuthentication = true;
  /**
   * Number of primary nameservers. Default is 3 *
   */
  public static final int DEFAULT_NUM_PRIMARY_REPLICAS = 3;
  public static int numPrimaryReplicas = DEFAULT_NUM_PRIMARY_REPLICAS;
  /**
   * Default query timeout in ms. How long we wait before retransmitting a query.
   */
  public static int DEFAULT_QUERY_TIMEOUT = 2000;
  /**
   * Maximum query wait time in milliseconds. After this amount of time
   * a negative response will be sent back to a client indicating that a 
   * record could not be found.
   */
  public static int DEFAULT_MAX_QUERY_WAIT_TIME = 16000; // was 10
  
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

  private final static Logger LOGGER = Logger.getLogger(GNS.class.getName());
  public static boolean initRun = false;

  /**
   * Returns the master GNS logger.
   * 
   * @return the master GNS logger
   */
  public static Logger getLogger() {
    if (!initRun) {
      System.out.println("Setting Logger console level to " + consoleOutputLevel + " and file level to " + fileLoggingLevel);
      Logging.setupLogger(LOGGER, consoleOutputLevel, fileLoggingLevel, "log" + "/gns.xml");
      initRun = true;
    }
    return LOGGER;
  }

  /**
   * Attempts to look for a MANIFEST file in that contains the Build-Version attribute.
   * 
   * @return a build version
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
