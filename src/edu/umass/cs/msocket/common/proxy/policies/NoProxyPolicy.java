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

import edu.umass.cs.msocket.proxy.location.ProxyStatusInfo;

/**
 * This class defines a policy that does not use a Proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class NoProxyPolicy extends ProxySelectionPolicy
{
  private static final long serialVersionUID = 6336245740937888229L;

  @Override
  public List<InetSocketAddress> getNewProxy()
  {
    return null;
  }

  @Override
  public boolean hasAvailableProxies()
  {
    return false;
  }

  @Override
  public List<String> getProxyIPs(List<ProxyStatusInfo> proxies, Socket acceptedSocket)
  {
    throw new IllegalAccessError("No proxy policies should not be sent to the location service");
  }

}
