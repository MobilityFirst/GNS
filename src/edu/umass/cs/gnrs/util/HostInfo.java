package edu.umass.cs.gnrs.util;

import java.net.InetAddress;

/**
 * ***********************************************************
 *
 * @author Hardeep Uppal
 ***********************************************************
 */
public class HostInfo {

  /**
   * Id of the name server *
   */
  private int id;
  /**
   * IP address of the name server *
   */
  private InetAddress ipAddress;
  /**
   * Starting port number *
   */
  private int startingPortNumber;
  /**
   * RTT latency between the local nameserver and this nameserver *
   */
  private double pingLatency;
  /**
   * Latitude of this nameserver *
   */
  private double latitude;
  /**
   * Longitude of this nameserver *
   */
  private double longitude;

  /**
   * ***********************************************************
   * Constructs a NameServerInfo with the give parameter
   *
   * @param id Name server id
   * @param ipAddress Name server IP address
   * @param startingPortNumber first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
  public HostInfo(int id, InetAddress ipAddress, int startingPortNumber, double pingLatency, double latitude, double longitude) {

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

  public double getPingLatency() {
    return pingLatency;
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
