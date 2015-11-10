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
package edu.umass.cs.gnsserver.installer;

import java.awt.geom.Point2D;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Information about instances that have started
 */
public class HostInfo {
  private final String id;
  private final String hostname;
  private String nsId;
  private boolean createLNS;
  private final Point2D location;

  /**
   * Creates a HostInfo instance.
   * 
   * @param hostname
   * @param nsId
   * @param createLNS
   * @param location
   */
  public HostInfo(String hostname, String nsId, boolean createLNS, Point2D location) {
    this.id = null;
    this.hostname = hostname;
    this.nsId = nsId;
    this.createLNS = createLNS;
    this.location = location;
  }

  // Older style constructor

  /**
   * Creates a HostInfo instance.
   * 
   * @param id
   * @param hostname
   * @param location
   */
    public HostInfo(String id, String hostname, Point2D location) {
    this.id = id;
    this.hostname = hostname;
    this.location = location;
    this.nsId = null;
    this.createLNS = false;
  }
  
  /**
   * Returns the id.
   * 
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the hostname.
   * 
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }
  
  /**
   * Returns the ns id.
   * 
   * @return the ns id
   * @throws UnknownHostException
   */
  public String getHostIP() throws UnknownHostException{
    return InetAddress.getByName(hostname).getHostAddress();
  }

  /**
   * Returns the ns id.
   * 
   * @return the ns id
   */
  public String getNsId() {
    return nsId;
  }

  /**
   * Returns the value of CreateLNS.
   * 
   * @return true or false
   */
  public boolean isCreateLNS() {
    return createLNS;
  }

  /**
   * Sets the value of CreateLNS.
   * 
   * @param createLNS
   */
  public void setCreateLNS(boolean createLNS) {
    this.createLNS = createLNS;
  }

  /**
   * Returns the location.
   * 
   * @return the location
   */
  public Point2D getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "HostInfo{" + "id=" + id + ", hostname=" + hostname + ", nsId=" + nsId + ", createLNS=" + createLNS + ", location=" + location + '}';
  }
  
}
