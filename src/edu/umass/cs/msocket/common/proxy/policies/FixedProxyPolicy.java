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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.proxy.location.ProxyStatusInfo;

/**
 * This class defines a Fixed Proxy Policy where the user can specify himself a
 * list of IP addresses of well-known proxies to use. The list will be provided
 * as is to {@link MServerSocket} in the same order as the one given in the
 * constructor.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FixedProxyPolicy extends ProxySelectionPolicy
{
  private static final long serialVersionUID = -8702157104039467313L;

  /**
   * Creates a new <code>FixedProxyPolicy</code> object
   * 
   * @param proxies List of InetSocketAddress of known proxies
   */
  public FixedProxyPolicy(List<InetSocketAddress> proxies)
  {
    proxyAddresses = proxies;
  }

  /**
   * @see edu.umass.cs.msocket.common.proxy.policies.ProxySelectionPolicy#getNewProxy()
   */
  @Override
  public List<InetSocketAddress> getNewProxy()
  {
    return proxyAddresses;
  }

  @Override
  public boolean hasAvailableProxies()
  {
    return proxyAddresses != null;
  }

  @Override
  public List<String> getProxyIPs(List<ProxyStatusInfo> proxies, Socket acceptedSocket)
  {
    throw new IllegalAccessError("Fixed proxy policies should not be sent to the location service");
  }

}
