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

package edu.umass.cs.msocket.proxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.proxy.forwarder.ProxyLoadStatistics;
import edu.umass.cs.msocket.proxy.location.GlobalPosition;

/**
 * This class defines a ProxyPublisher
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyPublisher extends Thread
{
  private String              proxyGroupName;
  private String              proxyName;
  private TimerKeepalive   gnsTimer;
  private boolean             killed = false;
  private SocketAddress       proxySocketAddres;
  private static final Logger logger = Logger.getLogger("GnsProxy");

  /**
   * Creates a new <code>ProxyGnsPublisher</code> object
   * 
   * @param gnsCredentials GNS connection and account GUID to use
   * @param proxyName Entity name this proxy is known as in the GNS
   * @param proxyGroupName Entity name of the group GUID for the proxy group
   *          this proxy belongs to
   * @param socketAddress the address the proxy is currently listening to,
   *          waiting for connections from mServerSockets
   */
  public ProxyPublisher(String proxyName, String proxyGroupName,
      SocketAddress socketAddress)
  {
    this.proxyGroupName = proxyGroupName;
    this.proxyName = proxyName;
    this.proxySocketAddres = socketAddress;
  }

  /**
   * Establish a connection with the GNS, register the proxy in the proxy group,
   * start a monitoring socket and register its IP in the GNS,
   * 
   * @throws Exception
   */
  public void registerProxyInGns() throws Exception
  {
    logger.info("Looking for proxy " + proxyName + " GUID and certificates...");
    GuidEntry myGuid = KeyPairUtils.getGuidEntry(
    		DefaultGNSClient.getDefaultGNSName(), proxyName);

    if (myGuid == null)
    {
      logger.info("No keys found for proxy " + proxyName + ". Generating new GUID and keys");
      GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute(GNSCommand.createGUID
    		  (DefaultGNSClient.getGnsClient().getGNSProvider(), 
    				  DefaultGNSClient.getMyGuidEntry(), proxyName));
      
      myGuid = (GuidEntry) commandRes.getResult();
    }
    logger.info("Proxy has guid " + myGuid.getGuid());

    // Determine our IP
    String sIp = null;
    BufferedReader in;
    InetAddress addr;
    try
    {
      addr = ((InetSocketAddress) proxySocketAddres).getAddress();
      if (addr != null && !addr.isLinkLocalAddress() && !addr.isLoopbackAddress() && !addr.isSiteLocalAddress())
        sIp = addr.getHostAddress();
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to resolve proxy address " + proxySocketAddres, e);
    }

    if (sIp == null)
    {
      logger.warning("Local proxy address (" + proxySocketAddres + ") does not seem to be a public address");
      try
      {
        logger.info("Determining local IP");
        // Determine our external IP address by contacting http://icanhazip.com
        URL whatismyip = new URL("http://icanhazip.com");
        in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
        sIp = in.readLine();
        in.close();
      }
      catch (Exception e)
      {
      }
    }

    ProxyInfo proxyInfo = new ProxyInfo(myGuid.getGuid(), proxyName, sIp);

    try
    {
      // Contact http://freegeoip.net/csv/[IP] to resolve IP address location
      URL locator = new URL("http://freegeoip.net/csv/" + sIp);
      in = new BufferedReader(new InputStreamReader(locator.openStream()));
      String csv = in.readLine();
      in.close();
      // Read location result
      StringTokenizer st = new StringTokenizer(csv, ",");
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read IP");
      st.nextToken(); // IP
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read country code");
      String countryCode = st.nextToken().replace("\"", "");
      proxyInfo.setCountryCode(countryCode);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read country name");
      String countryName = st.nextToken().replace("\"", "");
      proxyInfo.setCountryName(countryName);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read state code");
      String stateCode = st.nextToken().replace("\"", "");
      proxyInfo.setStateCode(stateCode);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read state name");
      String stateName = st.nextToken().replace("\"", "");
      proxyInfo.setStateName(stateName);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read city");
      String city = st.nextToken().replace("\"", "");
      proxyInfo.setCity(city);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read zip");
      String zip = st.nextToken().replace("\"", "");
      proxyInfo.setZipCode(zip);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read latitude");
      String latitudeStr = st.nextToken().replace("\"", "");
      double latitude = Double.parseDouble(latitudeStr);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read longitude");
      String longitudeStr = st.nextToken().replace("\"", "");
      double longitude = Double.parseDouble(longitudeStr);
      proxyInfo.setLatLong(new GlobalPosition(latitude, longitude, 0));
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to locate IP address " + e);
    }

    // Look for the group GUID
    GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute
    		(GNSCommand.lookupGUID(proxyGroupName));
    
    
    String groupGuid = commandRes.getResultString();

    // Check if we are a member of the group
    boolean isVerified = false;
    try
    {
    	commandRes = DefaultGNSClient.getGnsClient().execute
    			(GNSCommand.groupGetMembers(groupGuid, myGuid));
    	
      JSONArray members = commandRes.getResultJSONArray();
      for (int i = 0; i < members.length(); i++)
      {
        if (myGuid.getGuid().equals(members.get(i)))
        {
          isVerified = true;
          break;
        }
      }
    }
    catch (Exception e)
    {
      /*
       * At this point we couldn't get or parse the member list probably because
       * we don't have read access to it. This means we are not a verified
       * member.
       */
      logger.log(Level.INFO,
          "Could not access the proxy group member list because we are not likely a group member yet (" + e + ")");
    }
    
    // Make sure we advertise ourselves as a proxy and make the field readable
    // by everyone
    DefaultGNSClient.getGnsClient().execute(
    		GNSCommand.fieldReplaceOrCreateList(myGuid.getGuid(), Constants.SERVICE_TYPE_FIELD,
            new JSONArray().put(Constants.PROXY_SERVICE), myGuid) );
    
    DefaultGNSClient.getGnsClient().execute(
    		GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, myGuid, 
    				Constants.SERVICE_TYPE_FIELD, null));
    
    // Publish external IP (readable by everyone)
    InetSocketAddress externalIP = (InetSocketAddress) proxySocketAddres;
    
    
    DefaultGNSClient.getGnsClient().execute(GNSCommand.fieldReplaceOrCreateList
    		(myGuid.getGuid(), Constants.PROXY_EXTERNAL_IP_FIELD,
            new JSONArray().put
            (externalIP.getAddress().getHostAddress() + ":" + externalIP.getPort()), myGuid) );
    
    DefaultGNSClient.getGnsClient().execute(
    		GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, myGuid, 
    				Constants.PROXY_EXTERNAL_IP_FIELD, null));
    

    // Update our location if geolocation resolution worked
    if (proxyInfo.getLatLong() != null)
    {
    	DefaultGNSClient.getGnsClient().execute
    	( GNSCommand.setLocation(myGuid, proxyInfo.getLatLong().getLongitude(), 
    			proxyInfo.getLatLong().getLatitude()) );
    }
    
    if (!isVerified)
    {
      logger
          .log(Level.WARNING,
              "This proxy has not been verified yet, it will stay in the unverified list until it gets added to the proxy group");
    }

    gnsTimer = new TimerKeepalive(myGuid, 1000);
    gnsTimer.start();
  }

  /**
   * Publishes a new location for the proxy
   * 
   * @param longitude new proxy longitude
   * @param latitude new proxy latitude
   * @throws Exception if a GNS error occurs
   */
  public void publishNewProxyLocation(double longitude, double latitude) throws Exception
  {
	  DefaultGNSClient.getGnsClient().execute
	  	(GNSCommand.setLocation(DefaultGNSClient.getMyGuidEntry(), longitude, latitude));
  }

  /**
   * Terminate this proxy and its GnsTimerKeepAlive thread
   */
  public void killIt()
  {
    killed = true;
    gnsTimer.killIt();
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    while (!killed)
    {
      try
      {
        Thread.sleep(1000);
        
        DefaultGNSClient.getGnsClient().execute( 
        		GNSCommand.fieldReplaceOrCreateList(DefaultGNSClient.getMyGuidEntry().getGuid(), 
        		Constants.PROXY_LOAD, ProxyLoadStatistics.serializeLoadInformation(), 
        		DefaultGNSClient.getMyGuidEntry()) );
      }
      catch (Exception e)
      {
        logger.log(Level.WARNING, " Error while publishing proxy load in the GNS" + e, e);
      }
    }
  }
  
}