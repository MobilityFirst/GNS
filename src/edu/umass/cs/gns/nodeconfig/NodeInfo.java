package edu.umass.cs.gns.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The NodeInfo class is used to represent nodes in a GNSNodeConfig.
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class NodeInfo<NodeIDType> {

  /**
   * Id of the name server *
   */
  private final NodeIDType id;
  /**
   * The id of the activeReplica part of the name server.
   */
  private final NodeIDType activeReplicaID;
  /**
   * The id of the reconfigurator part of the name server.
   */
  private final NodeIDType reconfiguratorID;

  /**
   * IP address of the name server
   */
  private InetAddress ipAddress = null;

  /**
   * IP address of the name server - should be a host name
   */
  private final String ipAddressString;

  /**
   * External IP address of the name server
   */
  private InetAddress externalIPAddress = null;
  /**
   * External IP address of the name server - should be in dot format
   */
  private final String externalIPString;

  /**
   * Starting port number
   */
  private final int startingPortNumber;

  /**
   * RTT latency between the this node and node with nodeID = id. This field in updated
   * periodically by running pings during system deployment.
   */
  private long pingLatency;
  /**
   * Latitude of this nameserver *
   */
  private final double latitude;
  /**
   * Longitude of this nameserver *
   */
  private final double longitude;

  /**
   * ***********************************************************
   * Constructs a NameServerInfo with the give parameter
   *
   * @param id Name server id
   * @param activeReplicaID id of the activeReplica part of the name server.
   * @param reconfiguratorID id of the reconfigurator part of the name server
   * @param ipAddressString ip address as a string
   * @param externalIP
   * @param startingPortNumber first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver in milleseconds
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
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

  /**
   * Returns the top-level node id of this node;
   *
   * @return
   */
  public NodeIDType getId() {
    return id;
  }

  /**
   * Returns the ActiveReplica id for this node.
   *
   * @return
   */
  public NodeIDType getActiveReplicaID() {
    return activeReplicaID;
  }

  /**
   * Returns the Reconfigurator id for this node.
   *
   * @return
   */
  public NodeIDType getReconfiguratorID() {
    return reconfiguratorID;
  }

  /**
   * Return the ip address.
   * 
   * @return the ip address
   */
  public synchronized InetAddress getIpAddress() {
    if (ipAddress == null) {
      try {
        ipAddress = InetAddress.getByName(ipAddressString);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return ipAddress;
  }

  /**
   * Return the external ip address.
   * 
   * @return the external ip address
   */
  public synchronized InetAddress getExternalIPAddress() {
    if (externalIPAddress == null) {
      try {
        if (externalIPString != null) {
          externalIPAddress = InetAddress.getByName(externalIPString);
        } else {
          externalIPAddress = getIpAddress();
        }
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return externalIPAddress;
  }

  /**
   * Return the starting port number.
   * 
   * @return the starting port number
   */
  public int getStartingPortNumber() {
    return startingPortNumber;
  }

  /**
   * Returns ping latency in milleseconds
   *
   * @return pingLatency (ms)
   */
  public synchronized long getPingLatency() {
    return pingLatency;
  }

  /**
   * Set the ping latency
   * 
   * @param pingLatency in milleseconds
   */
  public synchronized void setPingLatency(long pingLatency) {
    this.pingLatency = pingLatency;
  }

  /**
   * Return the latitude.
   * 
   * @return the latitude
   */
  public double getLatitude() {
    return latitude;
  }

  /**
   * Return the longitude.
   * 
   * @return the longitude
   */
  public double getLongitude() {
    return longitude;
  }

  @Override
  public String toString() {
    return "NodeInfo{" + "id=" + id + ", activeReplicaID=" + activeReplicaID + ", reconfiguratorID=" + reconfiguratorID + ", ipAddress=" + ipAddress + ", ipAddressString=" + ipAddressString + ", externalIP=" + externalIPString + ", startingPortNumber=" + startingPortNumber + ", pingLatency=" + pingLatency + '}';
  }

}
