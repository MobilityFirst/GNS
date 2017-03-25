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
package edu.umass.cs.gnsserver.nodeconfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import edu.umass.cs.utils.Util;

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
  @Deprecated
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
  private final int activePort;

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
   * @param activePort first port number of block of ports used for TCP and UDP comms
   * @param pingLatency RTT latency between the local nameserver and this nameserver in milleseconds
   * @param latitude Latitude of the nameserver
   * @param longitude Longitude of the nameserver
   ***********************************************************
   */
  public NodeInfo(NodeIDType id, NodeIDType activeReplicaID, NodeIDType reconfiguratorID,
          String ipAddressString, String externalIP, int activePort,
          long pingLatency, double latitude, double longitude) {

    this.id = id;
    this.activeReplicaID = activeReplicaID;
    this.reconfiguratorID = reconfiguratorID;
    this.ipAddressString = ipAddressString;
    this.externalIPString = externalIP;
    this.activePort = activePort;
    this.pingLatency = pingLatency;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  /**
   * Returns the top-level node id of this node;
   *
   * @return a node id
   */
  public NodeIDType getId() {
    return id;
  }

  /**
   * Returns the ActiveReplica id for this node.
   *
   * @return a node id
   */
  public NodeIDType getActiveReplicaID() {
    return activeReplicaID;
  }

  /**
   * Returns the Reconfigurator id for this node.
   *
   * @return a node id
   */
  @Deprecated
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
        // arun: coz InetAddress produces a String it can't recognize 
        ipAddress = Util.getInetAddressFromString(ipAddressString);//InetAddress.getByName(ipAddressString);
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

  /**
   * Return the active port.
   *
   * @return the active port number
   */
  public int getActivePort() {
    return activePort;
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
    return "NodeInfo{" + "id=" + id + ", activeReplicaID=" + activeReplicaID
            + ", reconfiguratorID=" + reconfiguratorID + ", ipAddress=" + ipAddress
            + ", ipAddressString=" + ipAddressString + ", externalIP=" + externalIPString
            + ", activePort=" + activePort + ", pingLatency=" + pingLatency + '}';
  }

}
