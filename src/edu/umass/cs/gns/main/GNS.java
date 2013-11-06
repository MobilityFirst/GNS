package edu.umass.cs.gns.main;

import edu.umass.cs.gns.util.Logging;
import java.util.logging.Logger;

public class GNS {

  public static final int DEFAULTTTLINSECONDS = 0;
  
  public static int startingPort = 24400;
  
  public static final String INTERNAL_PREFIX = "_GNS_";
  
  public static final String GNS_URL_PATH = "GNS";
  
  /**
   * Creates a GNS field that is hidden from the user.
   * 
   * @param string
   * @return 
   */
  public static String makeInternalField(String string) {
    return INTERNAL_PREFIX + string;
  }
  
  /**
   * Returns true if field is a GNS field that is hidden from the user.
   * 
   * @param key
   * @return 
   */
  public static boolean isInternalField(String key) {
    return key.startsWith(INTERNAL_PREFIX);
  }
  
  // This is designed so we can run LNS and NS on the same host if needed
  public enum PortType {
    NS_TCP_PORT(0), // TCP port at name servers
    LNS_TCP_PORT(1), // TCP port at local name servers
    LNS_UDP_PORT(2), // UDP port at local name servers
    NS_ADMIN_PORT(3),
    LNS_ADMIN_PORT(4),
    LNS_ADMIN_RESPONSE_PORT(5),
    LNS_ADMIN_DUMP_RESPONSE_PORT(6);
    
    int offset;
    
    PortType(int offset) {
      this.offset = offset;
    }
    
    public static int maxOffset() {
      int result = 0;
      for (PortType p: values()) {
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
   * Number of primary nameservers. Default is 3 *
   */
  public static final int DEFAULTNUMPRIMARYREPLICAS = 3;
  public static int numPrimaryReplicas = DEFAULTNUMPRIMARYREPLICAS;
  public static boolean USELEGACYDNS = false;
  /**
   * default query timeout in ms.
   */
  public static int DEFAULT_QUERY_TIMEOUT = 2000;
  /**
   * maximum query wait time in milliseconds
   */
  public static int DEFAULT_MAX_QUERY_WAIT_TIME = 10000; // 100000 is too damn long... changed to 5 seconds - Westy
  /**
   * default number of transmissions
   */
  public static int DEFAULT_NUMBER_OF_TRANSMISSIONS = 3;
  
  /**
   * Logging level for main logger
   */
  public static String fileLoggingLevel = "INFO";
  
  /**
   * Console output level for main logger
   */
  public static String consoleOutputLevel = "INFO";

  /**
   * Logging level for stat logger
   */
  public static String statFileLoggingLevel = "INFO";
  
  /**
   * Console output level for stat logger
   */
  public static String statConsoleOutputLevel = "INFO";
  
  private final static Logger LOGGER = Logger.getLogger(GNS.class.getName());
  public static boolean initRun = false;
  
  public static Logger getLogger() {
    if (!initRun) {
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
    	Logging.setupLogger(STAT_LOGGER, statConsoleOutputLevel, statFileLoggingLevel, "log" + "/gns_stat.xml");
    	initStatRun = true;
    }
    return STAT_LOGGER;
  }
  


}
