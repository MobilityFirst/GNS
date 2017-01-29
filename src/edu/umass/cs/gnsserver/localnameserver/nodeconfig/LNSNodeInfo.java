
package edu.umass.cs.gnsserver.localnameserver.nodeconfig;

import edu.umass.cs.utils.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class LNSNodeInfo {


  private final Object id;

  private final Object activeReplicaID;

  private final Object reconfiguratorID;


  private InetAddress ipAddress = null;


  private final String ipAddressString;


  private final String externalIP;


  private final int startingPortNumber;


  private long pingLatency;

  private final double latitude;

  private final double longitude;


  public LNSNodeInfo(Object id, Object activeReplicaID, Object reconfiguratorID,
          String ipAddressString, String externalIP, int startingPortNumber,
          long pingLatency, double latitude, double longitude) {

    this.id = id;
    this.activeReplicaID = activeReplicaID;
    this.reconfiguratorID = reconfiguratorID;
    this.ipAddressString = ipAddressString;
    this.externalIP = externalIP;
    this.startingPortNumber = startingPortNumber;
    this.pingLatency = pingLatency;
    this.latitude = latitude;
    this.longitude = longitude;
  }


  public Object getId() {
    return id;
  }


  public Object getActiveReplicaID() {
    return activeReplicaID;
  }


  public Object getReconfiguratorID() {
    return reconfiguratorID;
  }


  public synchronized InetAddress getIpAddress() {
    // Lookup IP address on first access.
    if (ipAddress == null) {
      try {
        // arun
        ipAddress = Util.getInetAddressFromString(ipAddressString);//InetAddress.getByName(ipAddressString);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return ipAddress;
  }


  public String getExternalIP() {
    return externalIP;
  }


  public int getStartingPortNumber() {
    return startingPortNumber;
  }


  public synchronized long getPingLatency() {
    return pingLatency;
  }


  public synchronized void setPingLatency(long pingLatency) {
    this.pingLatency = pingLatency;
  }


  public double getLatitude() {
    return latitude;
  }


  public double getLongitude() {
    return longitude;
  }

  @Override
  public String toString() {
    return "HostInfo{" + "id=" + id.toString() + ", ipAddress=" + getIpAddress() + ", startingPortNumber="
            + startingPortNumber + ", pingLatency=" + pingLatency + ", latitude=" + latitude + ", longitude=" + longitude + '}';
  }

}
