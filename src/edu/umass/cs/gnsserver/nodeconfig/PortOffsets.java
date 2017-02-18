/*
 * Copyright (C) 2017
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

/**
 * Port offsets.
 */
public enum PortOffsets {
  /**
   * Port used to send admin requests to a name server.
   */
  SERVER_ADMIN_PORT(Config.getGlobalInt(GNSConfig.GNSC.SERVER_ADMIN_PORT_OFFSET)),
  /**
   * Port used to collect returning admin requests from the servers.
   */
  COLLATING_ADMIN_PORT(Config.getGlobalInt(GNSConfig.GNSC.COLLATING_ADMIN_PORT_OFFSET));

  int offset;

  PortOffsets(int offset) {
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
