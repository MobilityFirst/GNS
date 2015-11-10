/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gnsserver.utils.NetworkUtils;
import java.net.UnknownHostException;

/**
 * A tuple of NodeId, hostname, ip and start port.
 *
 */
public class HostSpec {

  private final String id;
  private final String name;
  private final String externalIP;
  private final Integer startPort;

  /**
   * Returns a HostSpec instance.
   * 
   * @param id
   * @param name
   * @param externalIP
   * @param startPort
   */
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

  /**
   * Return the id.
   * 
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * Return the name.
   * 
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Return the external ip.
   * 
   * @return the external ip
   */
  public String getExternalIP() {
    return externalIP;
  }

  /**
   * Return the start port.
   * 
   * @return the start port
   */
  public Integer getStartPort() {
    return startPort;
  }

}
