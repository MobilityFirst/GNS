/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The NodeInfo class is used to represent nodes in a LNSNodeConfig.
 *
 * @author Westy
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
   * Constructs a LNSNodeInfo with the given parameters.
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
   * @return top-level node id
   */
  public Object getId() {
    return id;
  }

  /**
   * Returns the ActiveReplica id for this node.
   *
   * @return the ActiveReplica id
   */
  public Object getActiveReplicaID() {
    return activeReplicaID;
  }

  /**
   * Returns the Reconfigurator id for this node.
   *
   * @return the Reconfigurator id
   */
  public Object getReconfiguratorID() {
    return reconfiguratorID;
  }

  /**
   * Returns the ip address for this node.
   * 
   * @return the ip address
   */
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

  /**
   * Returns the external IP for this node.
   * 
   * @return the externalIP
   */
  public String getExternalIP() {
    return externalIP;
  }

  /**
   * Returns the starting port number for this node.
   * 
   * @return starting port number
   */
  public int getStartingPortNumber() {
    return startingPortNumber;
  }

  /**
   * Returns the recorded ping latency in milleseconds.
   *
   * @return pingLatency (ms)
   */
  public synchronized long getPingLatency() {
    return pingLatency;
  }

  /**
   * Sets the recorded ping latency (in milleseconds).
   * 
   * @param pingLatency
   */
  public synchronized void setPingLatency(long pingLatency) {
    this.pingLatency = pingLatency;
  }

  /**
   * Returns the latitude for this node.
   * 
   * @return the latitude
   */
  public double getLatitude() {
    return latitude;
  }

  /**
   * Returns the longitude for this node.
   * 
   * @return the longitude
   */
  public double getLongitude() {
    return longitude;
  }

  @Override
  public String toString() {
    return "HostInfo{" + "id=" + id.toString() + ", ipAddress=" + getIpAddress() + ", startingPortNumber="
            + startingPortNumber + ", pingLatency=" + pingLatency + ", latitude=" + latitude + ", longitude=" + longitude + '}';
  }

}
