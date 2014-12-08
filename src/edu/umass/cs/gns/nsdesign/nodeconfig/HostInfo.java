package edu.umass.cs.gns.nsdesign.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ***********************************************************
 *
 * @author  Westy
 * @param <NodeIDType>
 ***********************************************************
 */
public class HostInfo<NodeIDType> {

  /**
   * Id of the name server *
   */
  private final NodeIDType id;
  /**
   * IP address of the name server *
   */
  private InetAddress ipAddress = null;

  /**
   * IP address of the name server *
   */
  private final String ipAddressString;

  /**
   * Starting port number *
   */
  private final int startingPortNumber;

  /**
   * RTT latency between the this node and node with nodeID = id. This field in updated
   * periodically by running pings during system deployment. During testing GNS, we emulate
   * wide-area latencies between nodes, and in that case, this is the delay that we emulate
   * between the nodes.
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
   * @param ipAddressString
   * @param startingPortNumber first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver in milleseconds
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
  public HostInfo(NodeIDType id, String ipAddressString, int startingPortNumber, long pingLatency, double latitude, double longitude) {

    this.id = id;
    this.ipAddressString = ipAddressString;
//    this.ipAddress = ipAddress;
    this.startingPortNumber = startingPortNumber;
    this.pingLatency = pingLatency;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public NodeIDType getId() {
    return id;
  }

  public synchronized InetAddress getIpAddress() {
    // Abhigyan: lookup IP address on first access.
    // this is done to help experiments on PlanetLab with a few 100 nodes.
    // A DNS lookup for few hundred names when starting a node takes a long time (1 sec/node) in some cases.
    if (ipAddress == null) {
      try {
        ipAddress = InetAddress.getByName(ipAddressString);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return ipAddress;
  }

  public int getStartingPortNumber() {
    return startingPortNumber;
  }
  /**
   * Returns ping latency in milleseconds
   * Ping latency is a
   * @return pingLatency (ms)
   */
  public synchronized long getPingLatency() {
    return pingLatency;
  }

  public synchronized void updatePingLatency(long pingLatency) {
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
