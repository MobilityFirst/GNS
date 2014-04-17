package edu.umass.cs.gns.util;

import java.net.InetAddress;

/**
 * ***********************************************************
 *
 * @author Hardeep Uppal, Abhigyan
 ***********************************************************
 */
public class HostInfo {

  /**
   * Id of the name server *
   */
  private final int id;
  /**
   * IP address of the name server *
   */
  private final InetAddress ipAddress;
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
   * @param ipAddress Name server IP address
   * @param startingPortNumber first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver in milleseconds
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
  public HostInfo(int id, InetAddress ipAddress, int startingPortNumber, long pingLatency, double latitude, double longitude) {

    this.id = id;
    this.ipAddress = ipAddress;
    this.startingPortNumber = startingPortNumber;
    this.pingLatency = pingLatency;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public int getId() {
    return id;
  }

  public InetAddress getIpAddress() {
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
    return "HostInfo{" + "id=" + id + ", ipAddress=" + ipAddress + ", startingPortNumber="
            + startingPortNumber + ", pingLatency=" + pingLatency + ", latitude=" + latitude + ", longitude=" + longitude + '}';
  }
  
}
