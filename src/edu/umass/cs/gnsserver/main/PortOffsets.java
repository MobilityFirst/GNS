package edu.umass.cs.gnsserver.main;

import edu.umass.cs.utils.Config;

public class PortOffsets {
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
    /**
     * Port used to send admin requests to a name server.
     */
    SERVER_ADMIN_PORT(Config.getGlobalInt(GNSConfig.GNSC.SERVER_ADMIN_PORT_OFFSET)),
    /**
     * Port used to collect returning admin requests from the servers.
     */
    COLLATING_ADMIN_PORT(Config.getGlobalInt(GNSConfig.GNSC.COLLATING_ADMIN_PORT_OFFSET));

    //
    int offset;

    PortType(int offset) {
      this.offset = offset;
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

}
