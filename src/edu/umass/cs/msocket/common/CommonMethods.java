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

package edu.umass.cs.msocket.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

import edu.umass.cs.msocket.logger.MSocketLogger;


/**
 * This class implements common methods as static functions, these are accessed
 * from many places. Getting active interface address of a device is implemented
 * here
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CommonMethods
{	
  public static Vector<String> getActiveInterfaceStringAddresses()
  {
    Vector<String> CurrentInterfaceIPs = new Vector<String>();
    try
    {
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
      {
        NetworkInterface intf = en.nextElement();
        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
        {
          InetAddress inetAddress = enumIpAddr.nextElement();
          if (!inetAddress.isLoopbackAddress())
          {
            // FIXME: find better method to get ipv4 address
            String IP = inetAddress.getHostAddress();
            if (IP.contains(":")) // means IPv6
            {
              continue;
            }
            else
            {
              CurrentInterfaceIPs.add(IP);
            }
          }
        }
      }
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
    return CurrentInterfaceIPs;
  }

  public static Vector<InetAddress> getActiveInterfaceInetAddresses()
  {
    Vector<InetAddress> CurrentInterfaceIPs = new Vector<InetAddress>();
    try
    {
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
      {
        NetworkInterface intf = en.nextElement();
        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
        {
          InetAddress inetAddress = enumIpAddr.nextElement();
          if (!inetAddress.isLoopbackAddress())
          {
            // FIXME: find better method to get ipv4 address
            String IP = inetAddress.getHostAddress();
            if (IP.contains(":")) // means IPv6
            {
              continue;
            }
            else
            {
              CurrentInterfaceIPs.add(inetAddress);
            }
          }
        }
      }
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
    return CurrentInterfaceIPs;
  }
  
  /**
   * convert byte[] GUID into String rep of hex, for indexing at proxy 
   * @param a
   * @return
   */
  public static String bytArrayToHex(byte[] a)
  {
    StringBuilder sb = new StringBuilder();

    for (byte b : a)
      sb.append(String.format("%02x", b & 0xff));

    String toBeReturned = sb.toString();
    toBeReturned = toBeReturned.toUpperCase();
    return toBeReturned;
  }
  
  public static byte[] hexStringToByteArray(String s) 
  {
	  int len = s.length();
	  byte[] data = new byte[len / 2];
	  for (int i = 0; i < len; i += 2) 
	  {
		  data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
				  + Character.digit(s.charAt(i+1), 16));
	  }
	  return data;
  }
  
  /**
   * returns the public IP
   * 
   * @return
   * @throws Exception
   */
  public static String returnPublicIP() throws IOException
  {
    // Determine our external IP address by contacting http://icanhazip.com
    String sIp = "";
    int numTry = 3;
    int i = 0;
    do
    {
      try
      {
        URL whatismyip = new URL("http://icanhazip.com");
        BufferedReader in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
        sIp = in.readLine();
        in.close();
        break;
      }
      catch (Exception ex)
      {
        MSocketLogger.getLogger().fine("excp in public IP determine returnPublicIP");
        if (i == numTry)
        {
          throw new IOException(ex);
        }
      }
      i++;
    }
    while (i < numTry);
    return sIp;
  }

  /**
   * determines if server is behind NAT
   * @return
   * @throws Exception
   */
  public static boolean isServerBehindNAT() throws IOException
  {
    Vector<InetAddress> CurrentInterfaceIPs = CommonMethods.getActiveInterfaceInetAddresses();
    boolean isBehindNAT = true;
    String pubIP = returnPublicIP();
    for (int i = 0; i < CurrentInterfaceIPs.size(); i++)
    {
      if (pubIP.equals(CurrentInterfaceIPs.get(i).getHostAddress()))
      {
        isBehindNAT = false;
        break;
      }
    }
    return isBehindNAT;
  }

  /**
   * returns preferably public Ip, or any other local Ip, or null if none
   * 
   * @return
   * @throws Exception
   */
  public static InetAddress returnLocalPublicIP() throws IOException
  {
    Vector<InetAddress> CurrentInterfaceIPs = CommonMethods.getActiveInterfaceInetAddresses();
    
    String pubIP = returnPublicIP();
    for (int i = 0; i < CurrentInterfaceIPs.size(); i++)
    {
      if (pubIP.equals(CurrentInterfaceIPs.get(i).getHostAddress()))
      {
        return CurrentInterfaceIPs.get(i);
      }
    }
    
    if (CurrentInterfaceIPs.size() > 0)
    {
      return CurrentInterfaceIPs.get(0);
    }
    else
    {
      return null;
    }
  }
  
}