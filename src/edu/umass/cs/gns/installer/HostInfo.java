/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.installer;

import java.awt.geom.Point2D;

/**
 * Information about instances that have started
 */
class HostInfo {
  private final int id;
  private final String hostname;
  private final String ip;
  private final Point2D location;

  public HostInfo(int id, String hostname, String ip, Point2D location) {
    this.id = id;
    this.hostname = hostname;
    this.ip = ip;
    this.location = location;
  }

  public int getId() {
    return id;
  }

  public String getHostname() {
    return hostname;
  }

  public String getIp() {
    return ip;
  }

  public Point2D getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "InstanceInfo{" + "id=" + id + ", hostname=" + hostname + ", ip=" + ip + ", location=" + location + '}';
  }
  
}
