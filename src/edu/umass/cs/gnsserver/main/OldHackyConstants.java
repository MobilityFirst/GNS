package edu.umass.cs.gnsserver.main;

public class OldHackyConstants {

  // This is designed so we can run multiple NSs on the same host if needed
  /**
   * Master port types.
   */
  @Deprecated
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
  @Deprecated
  public static final int DEFAULT_STARTING_PORT = 24400;

}
