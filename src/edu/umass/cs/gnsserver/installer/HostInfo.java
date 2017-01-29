
package edu.umass.cs.gnsserver.installer;

import java.awt.geom.Point2D;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class HostInfo {
  private final String id;
  private final String hostname;
  private String nsId;
  private boolean createLNS;
  private final Point2D location;


  public HostInfo(String hostname, String nsId, boolean createLNS, Point2D location) {
    this.id = null;
    this.hostname = hostname;
    this.nsId = nsId;
    this.createLNS = createLNS;
    this.location = location;
  }

  // Older style constructor


    public HostInfo(String id, String hostname, Point2D location) {
    this.id = id;
    this.hostname = hostname;
    this.location = location;
    this.nsId = null;
    this.createLNS = false;
  }
  

  public String getId() {
    return id;
  }


  public String getHostname() {
    return hostname;
  }
  

  public String getHostIP() throws UnknownHostException{
    return InetAddress.getByName(hostname).getHostAddress();
  }


  public String getNsId() {
    return nsId;
  }


  public boolean isCreateLNS() {
    return createLNS;
  }


  public void setCreateLNS(boolean createLNS) {
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
