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

import org.json.JSONArray;

/**
 * This class defines a ProxyInfo
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyStatusInfo
{
  private String    guid;
  private String    ip;
  private JSONArray load;
  private double    lontitude;
  private double    latitude;

  /**
   * Creates a new <code>ProxyInfo</code> object
   * 
   * @param guid
   * @param proxyIP
   * @param proxyLoad
   * @param lontitude
   * @param latitude
   */
  public ProxyStatusInfo(String guid, String proxyIP, JSONArray proxyLoad, double lontitude, double latitude)
  {
    this.guid = guid;
    this.ip = proxyIP;
    this.load = proxyLoad;
    this.lontitude = lontitude;
    this.latitude = latitude;
  }

  /**
   * Returns the guid value.
   * 
   * @return Returns the guid.
   */
  public String getGuid()
  {
    return guid;
  }

  /**
   * Sets the guid value.
   * 
   * @param guid The guid to set.
   */
  public void setGuid(String guid)
  {
    this.guid = guid;
  }

  /**
   * Returns the ip value.
   * 
   * @return Returns the ip.
   */
  public String getIp()
  {
    return ip;
  }

  /**
   * Sets the ip value.
   * 
   * @param ip The ip to set.
   */
  public void setIp(String ip)
  {
    this.ip = ip;
  }

  /**
   * Returns the load value.
   * 
   * @return Returns the load.
   */
  public JSONArray getLoad()
  {
    return load;
  }

  /**
   * Sets the load value.
   * 
   * @param load The load to set.
   */
  public void setLoad(JSONArray load)
  {
    this.load = load;
  }

  /**
   * Returns the lontitude value.
   * 
   * @return Returns the lontitude.
   */
  public double getLontitude()
  {
    return lontitude;
  }

  /**
   * Sets the lontitude value.
   * 
   * @param lontitude The lontitude to set.
   */
  public void setLontitude(double lontitude)
  {
    this.lontitude = lontitude;
  }

  /**
   * Returns the latitude value.
   * 
   * @return Returns the latitude.
   */
  public double getLatitude()
  {
    return latitude;
  }

  /**
   * Sets the latitude value.
   * 
   * @param latitude The latitude to set.
   */
  public void setLatitude(double latitude)
  {
    this.latitude = latitude;
  }

}
