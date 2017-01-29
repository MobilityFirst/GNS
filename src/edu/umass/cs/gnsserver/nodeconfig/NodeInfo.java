
package edu.umass.cs.gnsserver.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import edu.umass.cs.utils.Util;


public class NodeInfo<NodeIDType> {


  private final NodeIDType id;

  private final NodeIDType activeReplicaID;

  private final NodeIDType reconfiguratorID;


  private InetAddress ipAddress = null;


  private final String ipAddressString;


  private InetAddress externalIPAddress = null;

  private final String externalIPString;


  private final int startingPortNumber;


  private long pingLatency;

  private final double latitude;

  private final double longitude;


  public NodeInfo(NodeIDType id, NodeIDType activeReplicaID, NodeIDType reconfiguratorID,
          String ipAddressString, String externalIP, int startingPortNumber,
          long pingLatency, double latitude, double longitude) {

    this.id = id;
    this.activeReplicaID = activeReplicaID;
    this.reconfiguratorID = reconfiguratorID;
    this.ipAddressString = ipAddressString;
    this.externalIPString = externalIP;
    this.startingPortNumber = startingPortNumber;
    this.pingLatency = pingLatency;
    this.latitude = latitude;
    this.longitude = longitude;
  }


  public NodeIDType getId() {
    return id;
  }


  public NodeIDType getActiveReplicaID() {
    return activeReplicaID;
  }


  public NodeIDType getReconfiguratorID() {
    return reconfiguratorID;
  }


  public synchronized InetAddress getIpAddress() {
    if (ipAddress == null) {
      try {
    	  // arun: coz InetAddress produces a String it can't recognize 
        ipAddress = Util.getInetAddressFromString(ipAddressString);//InetAddress.getByName(ipAddressString);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return ipAddress;
  }


  public synchronized InetAddress getExternalIPAddress() {
    if (externalIPAddress == null) {
      try {
        if (externalIPString != null) {
        	// arun
          externalIPAddress = Util.getInetAddressFromString(externalIPString);//InetAddress.getByName(externalIPString);
        } else {
          externalIPAddress = getIpAddress();
        }
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return externalIPAddress;
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
    return "NodeInfo{" + "id=" + id + ", activeReplicaID=" + activeReplicaID + ", reconfiguratorID=" + reconfiguratorID + ", ipAddress=" + ipAddress + ", ipAddressString=" + ipAddressString + ", externalIP=" + externalIPString + ", startingPortNumber=" + startingPortNumber + ", pingLatency=" + pingLatency + '}';
  }

}
