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

package edu.umass.cs.msocket.gns;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyPair;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.json.JSONArray;


import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.common.policies.DefaultProxyPolicy;
import edu.umass.cs.msocket.common.policies.FixedProxyPolicy;
import edu.umass.cs.msocket.common.policies.ProxySelectionPolicy;

/**
 * This class defines the methods used for the integration with the GNS. All
 * interactions to store and retrieve information to/from the GNS are defined.
 * here.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class Integration
{
	private static Logger log = Logger.getLogger(Integration.class.getName());
	private static final Object  gnsLock = new Object();
  /**
   * Register any globally unique Human Readable Name in the GNS and create a
   * field with the same name in that GUID to store the InetSocketAddress
   * information.
   * 
   * @param name Human readable name of the service (needs to be unique
   *          GNS-wide)
   * @param saddr The IP address to store for this service
   * @param credentials The GNS credentials to use, usually the account GUID and
   *          default GNS (if null default GNS credentials are used)
   * @throws IOException
   */
  public static void registerWithGNS(String name, InetSocketAddress saddr) throws IOException
  {
	try
	{
	    //if (credentials == null)
	    //  credentials = GnsCredentials.getDefaultCredentials();
	
	    log.trace("Looking for entity " + name + " GUID and certificates...");
	
	    GuidEntry myGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), name);
	    //final GNSClientCommands gnsClient = credentials.getGnsClient();
	
	    /*
	     * Take a lock on the GNS connection object to prevent concurrent queries to
	     * the GNS on the same connection as the library is not thread-safe from
	     * that standpoint.
	     */
	    synchronized (gnsLock)
	    {
	      if (myGuid == null)
	      {
	        System.out.println("No keys found for service " + name + ". Generating new GUID and keys");
	        // Create a new GUID
	        myGuid = DefaultGNSClient.getGnsClient().guidCreate(DefaultGNSClient.getMyGuidEntry(), name);
	        
	        // save keys in the preference
	        System.out.println("saving keys to local");
	        KeyPairUtils.saveKeyPair(KeyPairUtils.getDefaultGns(), 
	        		myGuid.getEntityName() , myGuid.getGuid(), new KeyPair(myGuid.getPublicKey(), myGuid.getPrivateKey()));
	
	        // storing alias in gns record, need it to find it when we have GUID
	        // from group members
	        DefaultGNSClient.getGnsClient().fieldCreateList
	        		(myGuid.getGuid(), Constants.ALIAS_FIELD, new JSONArray().put(name), myGuid);
	      }
	
	      // Put the IP address in the GNS
	      String ipPort = saddr.getAddress().getHostAddress() + ":" + saddr.getPort();
	      log.trace("Updating " + Constants.SERVER_REG_ADDR + " GNSValue " + ipPort);
	      DefaultGNSClient.getGnsClient().fieldReplaceOrCreateList(myGuid.getGuid(), Constants.SERVER_REG_ADDR, new JSONArray().put(ipPort), myGuid);
	    }
	} catch(Exception ex)
	{
		ex.printStackTrace();
		throw new IOException(ex);
	}
  }

  /**
   * Lookup the IP address(es) of an MServerSocket by its Human Readable Name
   * registered in the GNS.
   * throws UnknownHostException, so that it is similar to
   * exception thrown on DNS failure
   * @param name Human readable name of the MServerSocket
   * @param gnsCredentials GNS credentials to use
   * @return list of IP addresses or null if not found
   * @throws Exception
   */
  public static List<InetSocketAddress> getSocketAddressFromGNS(String name)
      throws UnknownHostException
  {
	 try
	 {
	    log.trace("Retrieving IP of " + name);
	    
	    //if (gnsCredentials == null)
	    //{
	    //  gnsCredentials = GnsCredentials.getDefaultCredentials();
	    //}
	    JSONArray resultArray;
	    synchronized (gnsLock)
	    {
	    	//UniversalTcpClient gnsClient = gnsCredentials.getGnsClient();
		    String guidString = DefaultGNSClient.getGnsClient().lookupGuid(name);
		    log.trace("GUID lookup " + guidString);
		
		    
		    // Read from the GNS
		    resultArray = DefaultGNSClient.getGnsClient().
		    		fieldReadArray(guidString, Constants.SERVER_REG_ADDR, null);
	    }
	    
	    Vector<InetSocketAddress> resultVector = new Vector<InetSocketAddress>();
	    for (int i = 0; i < resultArray.length(); i++)
	    {
	      String str = resultArray.getString(i);
	      log.trace("Value returned from GNS " + str);
	      String[] Parsed = str.split(":");
	      InetSocketAddress socketAddress = new InetSocketAddress(Parsed[0], Integer.parseInt(Parsed[1]));
	      resultVector.add(socketAddress);
	    }
	    return resultVector;
	 } catch(Exception ex)
	 {
		 ex.printStackTrace();
		 throw new UnknownHostException(ex.toString());
	 }
  }

  /**
   * Clear the list contained in the given field name from the GNS.
   * 
   * @param name field name to clear
   * @param gnsCredentials GNS access credentials
   * @throws Exception if a GNS error occurs
   */
  public static void unregisterWithGNS(String name) throws IOException
  {
	try
	{
	    //if (gnsCredentials == null)
	    //  gnsCredentials = GnsCredentials.getDefaultCredentials();
	    //UniversalTcpClient gnsClient = gnsCredentials.getGnsClient();
	    GuidEntry socketGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), name);
	    
	    synchronized (gnsLock)
	    {
	      if(socketGuid != null)
	    	  DefaultGNSClient.getGnsClient().fieldClear
	    	  (socketGuid.getGuid(), Constants.SERVER_REG_ADDR, socketGuid);
	    }
	    log.trace("All fields cleared from GNS for MServerSocket " + name);
	} catch(Exception ex)
	{
		ex.printStackTrace();
		throw new IOException(ex);
	}
  }

  /**
   * Remove a specific IP address from the list of IPs associated with the
   * MServerSocket that has the given Human Readable Name
   * 
   * @param name Human readable name of the MServerSocket
   * @param saddr address to remove from the GNS
   * @param gnsCredentials GNS credentials to use
   * @throws Exception
   */
  public static void unregisterWithGNS(String name, InetSocketAddress saddr)
      throws Exception
  {
    //if (gnsCredentials == null)
    //  gnsCredentials = GnsCredentials.getDefaultCredentials();

    //final GNSClientCommands gnsClient = gnsCredentials.getGnsClient();
    GuidEntry socketGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), name);
    String ipPort = saddr.getAddress().getHostAddress() + ":" + saddr.getPort();

    // If all GNS accesses are synchronized on this object, we shouldn't have a
    // concurrency issue while updating
    synchronized (gnsLock)
    {
      JSONArray currentIPs = DefaultGNSClient.getGnsClient().fieldReadArray(socketGuid.getGuid(), Constants.SERVER_REG_ADDR,
    		  socketGuid);
      
      JSONArray newIPs = new JSONArray();
      int idx = -1;
      for (int i = 0; i < currentIPs.length(); i++)
      {
        if (ipPort.equals(currentIPs.getString(i)))
        {
          idx = i;
          //break;
        }
        else
        {
        	newIPs.put(currentIPs.getString(i));
        }
      }
      if (idx != -1)
      {
    	  //currentIPs.remove(idx);
    	  DefaultGNSClient.getGnsClient().fieldReplaceList(socketGuid.getGuid(), Constants.SERVER_REG_ADDR, newIPs,
    			  socketGuid);
      }
    }
  }

  /**
   * Returns the default proxy policy to use if no policy has been specified
   * @return a default proxy proxy
   * @throws Exception if default GNS credentials cannot be found
   */
  public static ProxySelectionPolicy getDefaultProxyPolicy() throws IOException
  {
	  // server is behind NAT, use proxies
	  if( CommonMethods.isServerBehindNAT() )
	  {
		  InetSocketAddress sockAddr = 
				  new InetSocketAddress(DefaultGNSClient.proxyName, DefaultGNSClient.proxyPort);
		  LinkedList<InetSocketAddress> proxyList = new LinkedList<InetSocketAddress>();
		  proxyList.add(sockAddr);
		  return new FixedProxyPolicy(proxyList);
	  }
	  else // no proxies
	  {
		  try 
		  {
			  return new DefaultProxyPolicy();
		  } catch (Exception ex)
		  {
			  throw new IOException(ex);
		  }
	  }
  }

  /**
   * Get an IP (or list of IPs) for proxies according to the specified proxy
   * selection policy.
   * 
   * @param proxySelectionPolicy the proxy selection policy to use
   * @return list of proxy IP addresses (eventually empty)
   * @throws IOException - Exception typecasted to IOException, don't want applications to change
   * exception handlers.
   */
  public static List<InetSocketAddress> getNewProxy(ProxySelectionPolicy proxySelectionPolicy) throws IOException
  {
	  List<InetSocketAddress> proxies = null;
	  try
	  {
		  proxies = proxySelectionPolicy.getNewProxy();
	  } catch(Exception ex)
	  {
		throw new IOException(ex);  
	  }
    return proxies;
  }

  /**
   * Return the GUID of the given alias
   * throws UnknownHostException, so that it is similar to
   * exception thrown on DNS failure
   * @param alias
   * @param gnsCredentials
   * @return
   * @throws UnknownHostException
   */
  public synchronized static String getGUIDOfAlias(String alias) throws UnknownHostException
  {
	  try
	  {
	    /*if (gnsCredentials == null)
	    {
	      long gnsCredStart = System.currentTimeMillis();
	      gnsCredentials = GnsCredentials.getDefaultCredentials();
	      long gnsCredEnd = System.currentTimeMillis();
	      log.debug("gnsCredentials time"+ (gnsCredEnd - gnsCredStart) );
	    }*/
	
	    //UniversalTcpClient gnsClient = gnsCredentials.getGnsClient();
		  
	    String guidAlias = DefaultGNSClient.getGnsClient().lookupGuid(alias);
	    
	    return guidAlias;
	  } catch(Exception ex)
	  {
		  throw new UnknownHostException(ex.toString());
	  }
  }
  
}