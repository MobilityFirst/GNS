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
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.proxy.location.ProxyStatusInfo;

/**
 * This class defines a RandomProxyPolicy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class RandomProxyPolicy extends ProxySelectionPolicy
{
  private static final long serialVersionUID = 2559217814049687908L;
  private String            proxyGroupName;
  private int               numProxy;

  /**
   * Creates a new <code>RandomProxyPolicy</code> object that picks up one
   * random proxy out of the given proxy group name.
   * 
   * @param proxyGroupName the proxy group name as registered in the GNS
   * @throws Exception if default GNS credentials can't be found
   */
  public RandomProxyPolicy(String proxyGroupName) throws Exception
  {
    this(proxyGroupName, 1);
  }

  /**
   * Creates a new <code>RandomProxyPolicy</code> object that picks up
   * numProxies random proxies out of the given proxy group name.
   * 
   * @param proxyGroupName the proxy group name as registered in the GNS
   * @param numProxy number of proxies to pick randomly (must be >=1)
   * @param gnsCredentials GNS credentials to use
   */
  public RandomProxyPolicy(String proxyGroupName, int numProxy)
  {
    this.proxyGroupName = proxyGroupName;
    this.numProxy = numProxy;
    
    if (numProxy < 1)
      throw new IllegalArgumentException("Number of proxies must at least be 1");
  }

  @Override
  public List<InetSocketAddress> getNewProxy() throws Exception
  {
    List<InetSocketAddress> result = new LinkedList<InetSocketAddress>();
    //final GuidEntry guidEntry = gnsCredentials.getGuidEntry();
    
    // Lookup proxies in proxy group list
    GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute
    								(GNSCommand.lookupGUID(proxyGroupName));
    
    String groupGuid = commandRes.getResultString();
    
    commandRes = DefaultGNSClient.getGnsClient().execute(GNSCommand.groupGetMembers
    		(groupGuid, DefaultGNSClient.getMyGuidEntry()));
    
    JSONArray members = commandRes.getResultJSONArray();
    
    
    for (int i = 0; i < members.length(); i++)
    { // Add each proxy to the list
      String proxyGuid = members.getString(i);
      
      commandRes = DefaultGNSClient.getGnsClient().execute( GNSCommand.fieldReadArray
    		  (proxyGuid, Constants.SERVICE_TYPE_FIELD, DefaultGNSClient.getMyGuidEntry()) );
      
      // Check first that this member is a proxy and not another service
      String serviceType = commandRes.getResultJSONArray().getString(0);
      
      if (!Constants.PROXY_SERVICE.equals(serviceType))
        continue; // This is not a proxy, ignore
      
      
      commandRes = DefaultGNSClient.getGnsClient().execute(
    		  GNSCommand.fieldReadArray(proxyGuid, 
    		   Constants.PROXY_EXTERNAL_IP_FIELD, DefaultGNSClient.getMyGuidEntry()) );
      
      // Grab the proxy IP address
      String proxyIp = commandRes.getResultJSONArray().getString(0);
      
      String[] parsed = proxyIp.split(":");
      InetSocketAddress addr = new InetSocketAddress(parsed[0], Integer.parseInt(parsed[1]));
      result.add(addr);

      // Did we get enough proxies?
      if (result.size() == numProxy)
        break;
    }
    
    return result;
  }

  @Override
  public boolean hasAvailableProxies()
  {
    try
    {
      return getNewProxy() != null;
    }
    catch (Exception e)
    {
      return false;
    }
  }

  @Override
  public List<String> getProxyIPs(List<ProxyStatusInfo> proxies, Socket acceptedSocket)
  {
    throw new IllegalAccessError
    	("Random proxy policies should not be sent to the location service");
  }
}