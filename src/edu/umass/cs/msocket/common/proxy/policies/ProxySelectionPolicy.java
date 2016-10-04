/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket.common.proxy.policies;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import edu.umass.cs.msocket.proxy.location.ProxyStatusInfo;

/**
 * This class defines options to specify the user preference when choosing a
 * proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class ProxySelectionPolicy implements Serializable
{
  private static final long serialVersionUID = -7302230484039609299L;

  /** List of proxy addresses to be filled by the implementation */
  List<InetSocketAddress>   proxyAddresses;

  /**
   * Ordered list of Proxy addresses according to the current policy
   * 
   * @return List of InetSocketAddress or null if no proxy has been found
   */
  public List<InetSocketAddress> getProxyAddresses()
  {
    return proxyAddresses;
  }

  /**
   * Get a list of IPs for proxies in a proxy group according to the current
   * proxy selection policy. This method is called by MServerSocket.
   * 
   * @return list of proxy IP addresses (eventually empty)
   * @throws Exception if an error occurs while getting the new proxy
   */
  public abstract List<InetSocketAddress> getNewProxy() throws Exception;

  /**
   * Returns true if the policy can find available proxies
   * 
   * @return true if proxies are currently available through this policy
   */
  public abstract boolean hasAvailableProxies();

  /**
   * Server side implementation of the policy. Return the IPs of the selected
   * proxies based upon the policy. This is called by the location service.
   * 
   * @param proxies the current status of active proxies in the system
   * @param acceptedSocket client socket that was accepted by the location
   *          service
   * @return ordered list of IP:port of proxies selected by the policy
   */
  public abstract List<String> getProxyIPs(List<ProxyStatusInfo> proxies, Socket acceptedSocket);

}