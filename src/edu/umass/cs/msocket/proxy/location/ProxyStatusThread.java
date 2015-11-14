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

package edu.umass.cs.msocket.proxy.location;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;




import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import edu.umass.cs.msocket.common.GnsConstants;
import edu.umass.cs.msocket.gns.DefaultGNSClient;

/**
 * This class defines a ProxyStatusThread
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyStatusThread extends Thread
{
  private static final long     REFRESH_TIME_IN_MS = 30000;
  private String                proxyGroupName;
  private boolean               isKilled           = false;
  /**
   * Map of proxy Location -> IP, location being a
   * JSONArray(Arrays.asList(double longitude, double latitude))
   */
  private List<ProxyStatusInfo> proxies;
  private static final Logger   logger             = Logger.getLogger("ProxyLocationService");

  /**
   * Creates a new <code>ProxyStatusThread</code> object
   * 
   * @param proxyGroupName
   * @param gnsCredentials
   * @throws Exception if a GNS error occurs
   */
  public ProxyStatusThread(String proxyGroupName) throws Exception
  {
    this.proxyGroupName = proxyGroupName;

    populateProxyList();
  }

  /**
   * Fetch the list of active proxies and then for each proxy gets its IP and
   * location.
   * 
   * @throws Exception if a GNS error occurs
   */
  protected void populateProxyList() throws Exception
  {
    //final UniversalTcpClient gnsClient = gnsCredentials.getGnsClient();
    //final GuidEntry guidEntry = gnsCredentials.getGuidEntry();
    JSONArray guids;
    try
    {
      guids = DefaultGNSClient.getGnsClient().fieldReadArray(proxyGroupName, GnsConstants.ACTIVE_PROXY_FIELD, DefaultGNSClient.getMyGuidEntry());
    }
    catch (GnsInvalidGuidException e)
    {
      logger
          .info("Cannot read the list of active proxies, this location service is probably still not approved in the proxy group (using GUID "
              + DefaultGNSClient.getMyGuidEntry().getGuid() + ")");
      return;
    }

    // Generate a new list
    List<ProxyStatusInfo> newList = new LinkedList<ProxyStatusInfo>();
    for (int i = 0; i < guids.length(); i++)
    {
      String proxyGuid = guids.getString(i);
      String proxyIP = DefaultGNSClient.getGnsClient().fieldReadArray(proxyGuid, GnsConstants.PROXY_EXTERNAL_IP_FIELD, DefaultGNSClient.getMyGuidEntry()).getString(0);
      JSONArray proxyLoad = DefaultGNSClient.getGnsClient().fieldReadArray(proxyGuid, GnsConstants.PROXY_LOAD, DefaultGNSClient.getMyGuidEntry());
      JSONArray proxyLocation = DefaultGNSClient.getGnsClient().getLocation(proxyGuid, DefaultGNSClient.getMyGuidEntry());
      double lontitude = Double.parseDouble(proxyLocation.getString(0));
      double latitude = Double.parseDouble(proxyLocation.getString(1));
      ProxyStatusInfo proxyInfo = new ProxyStatusInfo(proxyGuid, proxyIP, proxyLoad, lontitude, latitude);
      newList.add(proxyInfo);
    }

    // Make sure the replacement is atomic and does not conflict with
    // concurrent queries
    setProxies(newList);
  }

  /**
   * Returns the proxies value.
   * 
   * @return Returns the proxies.
   */
  public synchronized List<ProxyStatusInfo> getProxies()
  {
    return proxies;
  }

  /**
   * Sets the proxies value.
   * 
   * @param proxies The proxies to set.
   */
  public synchronized void setProxies(List<ProxyStatusInfo> proxies)
  {
    this.proxies = proxies;
  }

  /**
   * Terminate this thread
   */
  public void killIt()
  {
    isKilled = true;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    while (!isKilled)
    {
      try
      {
        Thread.sleep(REFRESH_TIME_IN_MS);
      }
      catch (InterruptedException ignore)
      {
      }
      try
      {
        populateProxyList();
      }
      catch (Exception e)
      {
        logger.log(Level.WARNING, "Error while retrieving list of active proxies", e);
      }
    }
  }
  
}