/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nodeconfig;

/**
 * A tuple of NodeId and hostname.
 * @param <NodeIDType>
 */
public class HostSpec<NodeIDType> {
  private final NodeIDType id;
  private final String name;
  private final String externalIP;
  private final Integer startPort;

  public HostSpec(NodeIDType id, String name, String externalIP, Integer startPort) {
    this.id = id;
    this.name = name;
    this.externalIP = externalIP;
    this.startPort = startPort;
  }

  public NodeIDType getId() {
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
