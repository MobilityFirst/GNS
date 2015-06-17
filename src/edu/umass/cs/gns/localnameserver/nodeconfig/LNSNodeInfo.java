/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The NodeInfo class is used to represent nodes in a GNSNodeConfig.
 * 
 * @author  Westy
 */
public class LNSNodeInfo {

  /**
   * Id of the name server *
   */
  private final Object id;
  /**
   * The id of the activeReplica part of the name server.
   */
  private final Object activeReplicaID;
  /**
   * The id of the reconfigurator part of the name server.
   */
  private final Object reconfiguratorID;
  
  /**
   * IP address of the name server *
   */
  private InetAddress ipAddress = null;

  /**
   * IP address of the name server - should be a host name*
   */
  private final String ipAddressString;
  
  /**
   * External IP address of the name server - should be in dot format *
   */
  private final String externalIP;

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
   * @param activeReplicaID id of the activeReplica part of the name server.
   * @param reconfiguratorID id of the reconfigurator part of the name server
   * @param ipAddressString ip address as a string
   * @param startingPortNumber first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver in milleseconds
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
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

  /**
   * Returns the top-level node id of this node;
   * 
   * @return 
   */
  public Object getId() {
    return id;
  }

  /**
   * Returns the ActiveReplica id for this node.
   * 
   * @return 
   */
  public Object getActiveReplicaID() {
    return activeReplicaID;
  }

  /**
   * Returns the Reconfigurator id for this node.
   * @return 
   */
  public Object getReconfiguratorID() {
    return reconfiguratorID;
  }

  public synchronized InetAddress getIpAddress() {
    // Lookup IP address on first access.
    if (ipAddress == null) {
      try {
        ipAddress = InetAddress.getByName(ipAddressString);
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
  /**
   * Returns ping latency in milleseconds
   * Ping latency is a
   * @return pingLatency (ms)
   */
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
