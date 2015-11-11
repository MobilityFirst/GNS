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

import edu.umass.cs.gnscommon.utils.NetworkUtils;
import java.net.UnknownHostException;

/**
 * A tuple of NodeId, hostname, ip and start port.
 *
 */
public class HostSpec {

  private final String id;
  private final String name;
  private final String externalIP;
  private final Integer startPort;

  /**
   * Returns a HostSpec instance.
   * 
   * @param id
   * @param name
   * @param externalIP
   * @param startPort
   */
  public HostSpec(String id, String name, String externalIP, Integer startPort) {
    if ("127.0.0.1".equals(name) || "localhost".equals(name)) {
      try {
        name = NetworkUtils.getLocalHostLANAddress().getHostAddress();
      } catch (UnknownHostException e) {
        // punt if we can't get it
      }
    }

    this.id = id;
    this.name = name;
    this.externalIP = externalIP;
    this.startPort = startPort;
  }

  /**
   * Return the id.
   * 
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * Return the name.
   * 
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Return the external ip.
   * 
   * @return the external ip
   */
  public String getExternalIP() {
    return externalIP;
  }

  /**
   * Return the start port.
   * 
   * @return the start port
   */
  public Integer getStartPort() {
    return startPort;
  }

}
