/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.awt.geom.Point2D;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Information about instances that have started
 */
public class HostInfo {
  public static final NodeId<String> NULL_ID = GNSNodeConfig.INVALID_NAME_SERVER_ID;
  private final NodeId<String> id;
  private final String hostname;
  private NodeId<String> nsId;
  private boolean createLNS;
  private final Point2D location;

  public HostInfo(String hostname, NodeId<String> nsId, boolean createLNS, Point2D location) {
    this.id = NULL_ID;
    this.hostname = hostname;
    this.nsId = nsId;
    this.createLNS = createLNS;
    this.location = location;
  }

  // Older style constructor
  @Deprecated
  public HostInfo(NodeId<String> id, String hostname, Point2D location) {
    this.id = id;
    this.hostname = hostname;
    this.location = location;
    this.nsId = NULL_ID;
    this.createLNS = false;
  }
  
  @Deprecated
  public NodeId<String> getId() {
    return id;
  }

  public String getHostname() {
    return hostname;
  }
  
  public String getHostIP() throws UnknownHostException{
    return InetAddress.getByName(hostname).getHostAddress();
  }

  public NodeId<String> getNsId() {
    return nsId;
  }

  public boolean isCreateLNS() {
    return createLNS;
  }

  public void createLNS(boolean createLNS) {
    this.createLNS = createLNS;
  }

  public Point2D getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "HostInfo{" + "id=" + id + ", hostname=" + hostname + ", nsId=" + nsId + ", createLNS=" + createLNS + ", location=" + location + '}';
  }
  
}
