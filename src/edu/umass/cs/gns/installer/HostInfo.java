/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.util.GEOLocator;
import java.awt.geom.Point2D;

/**
 * Information about hosts we're running the GNS on.
 */
class HostInfo {
  private int id;
  private String ip;
  private Point2D location;

   public HostInfo(int id, String ip) {
    this.id = id;
    this.ip = ip;
    this.location =  GEOLocator.lookupIPLocation(ip);
  }
 
  public int getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public Point2D getLocation() {
    return location;
  }
  
  

  @Override
  public String toString() {
    return "InstanceInfo{" + "id=" + id + ", ip=" + ip + ", location=" + location + '}';
  }
  
}
