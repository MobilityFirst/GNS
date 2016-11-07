package edu.umass.cs.gnsserver.main;

public class OldHackyConstants {

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
	//FIXME: The owner of this should move it into GNSConfig
	  /**
	   * How long (in seconds) to blacklist active code.
	   */
	  public static long activeCodeBlacklistSeconds = 10;
	//FIXME: Do this have an equivalent in gigapaxos we can use.
	  /**
	   * Determines the number of replicas based on ratio of lookups to writes.
	   * Used by {@link LocationBasedDemandProfile}.
	   */
	  public static double normalizingConstant = 0.5;
	//FIXME: The owner of this should move it into GNSConfig
	  /**
	   * Enable active code.
	   */
	  public static boolean enableActiveCode = false;
	//FIXME: The owner of this should move it into GNSConfig
	  /**
	   * Number of active code worker.
	   */
	  public static int activeCodeWorkerCount = 1;
	//FIXME: Remove this.
	  /**
	   * The default starting port.
	   */
	  public static final int DEFAULT_STARTING_PORT = 24400;
	//FIXME: Do this have an equivalent in gigapaxos we can use.
	  /**
	   * The minimum number of replicas. Used by {@link LocationBasedDemandProfile}.
	   */
	  public static int minReplica = 3;
	//FIXME: Do this have an equivalent in gigapaxos we can use.
	  /**
	   * The maximum number of replicas. Used by {@link LocationBasedDemandProfile}.
	   */
	  public static int maxReplica = 100;
	/**
	   * Controls whether signature verification is enabled.
	   */
	  public static boolean enableSignatureAuthentication = true;

}
