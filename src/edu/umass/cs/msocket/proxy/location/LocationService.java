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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
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
import edu.umass.cs.msocket.proxy.ProxyInfo;

/**
 * This class defines a LocationService
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LocationService extends Thread
{
  private String              proxyGroupName;
  private String              locationServiceName;
  private boolean             killed = false;
  private ProxyStatusThread   statusThread;
  private ProxyInfo           locationServiceInfo;
  private ServerSocket        ss;
  private GuidEntry			   locationServiceGuid;
  private static final Logger logger = Logger.getLogger("LocationService");

  /**
   * Creates a new <code>LocationService</code> object
   * 
   * @param proxyGroupName
   * @param locationServiceName
   * @param gnsCredentials
   */
  public LocationService(String proxyGroupName, String locationServiceName)
  {
    this.proxyGroupName = proxyGroupName;
    this.locationServiceName = locationServiceName;
  }

  /**
   * Establish a connection with the GNS, register the proxy in the proxy group,
   * start a monitoring socket and register its IP in the GNS,
   * 
   * @throws Exception
   */
  public void registerLocationServiceInGns() throws Exception
  {
    logger.info("Looking for location service " + locationServiceName + " GUID and certificates...");
    GuidEntry locationServiceGuid = KeyPairUtils.getGuidEntry(
    		DefaultGNSClient.getDefaultGNSName(), locationServiceName);

    if (locationServiceGuid == null)
    {
      logger.info("No keys found for location service " + locationServiceName 
    		  + ". Generating new GUID and keys");
      
      GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute(GNSCommand.createGUID
    		  (DefaultGNSClient.getGnsClient().getGNSProvider(), 
    				  DefaultGNSClient.getMyGuidEntry(), locationServiceName));
      
      locationServiceGuid = (GuidEntry) commandRes.getResult();
    }
    logger.info("We are guid " + locationServiceGuid.getGuid());
    

    // Determine our IP
    String sIp = null;
    BufferedReader in;
    try
    {
      logger.info("Determining public IP");
      // Determine our external IP address by contacting http://icanhazip.com
      URL whatismyip = new URL("http://icanhazip.com");
      in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
      sIp = in.readLine();
      in.close();
    }
    catch (Exception e)
    {
    }

    locationServiceInfo = new ProxyInfo(locationServiceGuid.getGuid(), locationServiceName, sIp);

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
      locationServiceInfo.setCountryCode(countryCode);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read country name");
      String countryName = st.nextToken().replace("\"", "");
      locationServiceInfo.setCountryName(countryName);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read state code");
      String stateCode = st.nextToken().replace("\"", "");
      locationServiceInfo.setStateCode(stateCode);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read state name");
      String stateName = st.nextToken().replace("\"", "");
      locationServiceInfo.setStateName(stateName);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read city");
      String city = st.nextToken().replace("\"", "");
      locationServiceInfo.setCity(city);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read zip");
      String zip = st.nextToken().replace("\"", "");
      locationServiceInfo.setZipCode(zip);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read latitude");
      String latitudeStr = st.nextToken().replace("\"", "");
      double latitude = Double.parseDouble(latitudeStr);
      if (!st.hasMoreTokens())
        throw new IOException("Failed to read longitude");
      String longitudeStr = st.nextToken().replace("\"", "");
      double longitude = Double.parseDouble(longitudeStr);
      locationServiceInfo.setLatLong(new GlobalPosition(latitude, longitude, 0));
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to locate IP address " + e);
    }
    
    GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute(
    		GNSCommand.lookupGUID(proxyGroupName));
    
    // Look for the group GUID
    String groupGuid = commandRes.getResultString();

    // Check if we are a member of the group
    
    commandRes = DefaultGNSClient.getGnsClient().execute
    				(GNSCommand.groupGetMembers(groupGuid, locationServiceGuid));
    
    JSONArray members = commandRes.getResultJSONArray();
    boolean isVerified = false;
    for (int i = 0; i < members.length(); i++)
    {
      if (locationServiceGuid.getGuid().equals(members.get(i)))
      {
        isVerified = true;
        break;
      }
    }
    
    // Make sure we advertise ourselves as a location service (readable for
    // everyone)
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.fieldReplaceOrCreateList
    		(locationServiceGuid.getGuid(), Constants.SERVICE_TYPE_FIELD,
            new JSONArray().put(Constants.LOCATION_SERVICE), locationServiceGuid) );
    
    
    DefaultGNSClient.getGnsClient().execute( 
    		GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, 
    				locationServiceGuid, Constants.SERVICE_TYPE_FIELD, null) );

    
    // Update our location
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.setLocation
    		(locationServiceGuid, locationServiceInfo.getLatLong().getLongitude(), 
    				locationServiceInfo.getLatLong().getLatitude()) );

    if (!isVerified)
    {
      logger
          .log(
              Level.WARNING,
              "This location service has not been verified yet, it will stay in the unverified list until it gets added to the proxy group");
    }
  }

  /**
   * Terminate this location service and its ProxyStatus thread
   */
  public void killIt()
  {
    killed = true;
    if (ss != null)
      try
      {
        ss.close();
      }
      catch (IOException ignore)
      {
      }
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
        // Create the server socket
        ss = new ServerSocket(0);

        // Publish location service IP in the GNS
        final String ipPort = locationServiceInfo.getIpAddress() + ":" + ss.getLocalPort();
        logger.info("Publishing Location service IP (" + ipPort + ") in GNS.");
        //final GuidEntry guidEntry = gnsCredentials.getGuidEntry();
        
        
        DefaultGNSClient.getGnsClient().execute( GNSCommand.fieldReplaceOrCreateList
        		(locationServiceGuid.getGuid(), Constants.LOCATION_SERVICE_IP,
                new JSONArray().put(ipPort), locationServiceGuid));
        
        DefaultGNSClient.getGnsClient().execute( GNSCommand.aclAdd
        	(AclAccessType.READ_WHITELIST, locationServiceGuid, Constants.LOCATION_SERVICE_IP, null) );
        

        // Start the thread that collect information about proxy status
        statusThread = new ProxyStatusThread(proxyGroupName);
        statusThread.start();

        // Now accepting connections
        while (!ss.isClosed())
        {
          new LocateServiceThread(ss.accept(), this).start();
        }
      }
      catch (Exception e)
      {
        logger.log(Level.WARNING, "Location service failed with error " + e, e);
      }
    }
    if (statusThread != null)
      statusThread.killIt();

    logger.log(Level.INFO, "Location service has been terminated");
  }

  /**
   * Returns the statusThread value.
   * 
   * @return Returns the statusThread.
   */
  public ProxyStatusThread getStatusThread()
  {
    return statusThread;
  }

  /**
   * Returns the proxyInfo value.
   * 
   * @return Returns the proxyInfo.
   */
  public ProxyInfo getLocationServiceInfo()
  {
    return locationServiceInfo;
  }
}