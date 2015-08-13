/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nodeconfig;

import edu.umass.cs.gns.util.NetworkUtils;
import java.net.UnknownHostException;

/**
 * A tuple of NodeId and hostname.
 *
 */
public class HostSpec {

  private final String id;
  private final String name;
  private final String externalIP;
  private final Integer startPort;

  public HostSpec(String id, String name, String externalIP, Integer startPort) {
    if ("127.0.0.1".equals(name) || "localhost".equals(name)) {
      try {
        name = NetworkUtils.getLocalHostLANAddress().getHostAddress();
      } catch (UnknownHostException e) {
        // punt if we can't get it
      }
    }

    this.id = id;
    this.name = name;
    this.externalIP = externalIP;
    this.startPort = startPort;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getExternalIP() {
    return externalIP;
  }

  public Integer getStartPort() {
    return startPort;
  }

}
