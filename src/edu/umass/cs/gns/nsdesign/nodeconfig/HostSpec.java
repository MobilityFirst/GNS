/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

/**
 * A tuple of NodeId and hostname.
 * @param <NodeIDType>
 */
public class HostSpec<NodeIDType> {
  private final NodeIDType id;
  private final String name;
  private final Integer startPort;

  public HostSpec(String id, String name, Integer startPort) {
    this.id = (NodeIDType) id;
    this.name = name;
    this.startPort = startPort;
  }

  public NodeIDType getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Integer getStartPort() {
    return startPort;
  }
  
}
