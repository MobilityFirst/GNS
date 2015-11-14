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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import edu.umass.cs.msocket.proxy.location.GlobalPosition;

/**
 * This class defines a ProxyInfo that contains information such as IP address,
 * location and various status info from a proxy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyInfo
{
  private static final String SEPARATOR = "|";
  private String              guid;
  private String              alias;
  private String              ipAddress;
  private GlobalPosition      latLong;
  private String              city;
  private String              region;
  private String              countryCode;
  private String              countryName;
  private String              stateCode;
  private String              stateName;
  private String              zipCode;
  private Date                startedTime;
  SimpleDateFormat            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * Creates a new <code>ProxyInfo</code> object setting the started time to the
   * current time
   * 
   * @param guid the GUID of the proxy
   * @param proxyName the alias (or human readable name) for the proxy GUID in
   *          the GNS
   * @param ipAddress the public IP address of the proxy
   */
  public ProxyInfo(String guid, String proxyName, String ipAddress)
  {
    this.guid = guid;
    this.alias = proxyName;
    this.ipAddress = ipAddress;
    this.startedTime = new Date(System.currentTimeMillis());
  }

  /**
   * Creates a new <code>ProxyInfo</code> object from its toString() form
   * 
   * @param toStringForm
   * @throws ParseException if the string is malformed
   */
  public ProxyInfo(String toStringForm) throws ParseException
  {
    StringTokenizer st = new StringTokenizer(toStringForm, SEPARATOR);
    guid = st.nextToken();
    alias = st.nextToken();
    ipAddress = st.nextToken();
    double latitude = Double.parseDouble(st.nextToken());
    double longitude = Double.parseDouble(st.nextToken());
    latLong = new GlobalPosition(latitude, longitude, 0);
    city = st.nextToken();
    region = st.nextToken();
    countryCode = st.nextToken();
    countryName = st.nextToken();
    stateCode = st.nextToken();
    stateName = st.nextToken();
    zipCode = st.nextToken();
    startedTime = formatter.parse(st.nextToken());
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    return guid + SEPARATOR + alias + SEPARATOR + ipAddress + SEPARATOR + latLong.getLatitude() + SEPARATOR
        + latLong.getLongitude() + SEPARATOR + city + SEPARATOR + region + SEPARATOR + countryCode + SEPARATOR
        + countryName + SEPARATOR + stateCode + SEPARATOR + stateName + SEPARATOR + zipCode + SEPARATOR
        + formatter.format(startedTime);
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
   * Returns the alias value.
   * 
   * @return Returns the alias.
   */
  public String getAlias()
  {
    return alias;
  }

  /**
   * Sets the alias value.
   * 
   * @param alias The alias to set.
   */
  public void setAlias(String alias)
  {
    this.alias = alias;
  }

  /**
   * Returns the ipAddress value.
   * 
   * @return Returns the ipAddress.
   */
  public String getIpAddress()
  {
    return ipAddress;
  }

  /**
   * Sets the ipAddress value.
   * 
   * @param ipAddress The ipAddress to set.
   */
  public void setIpAddress(String ipAddress)
  {
    this.ipAddress = ipAddress;
  }

  /**
   * Returns the latLong value.
   * 
   * @return Returns the latLong.
   */
  public GlobalPosition getLatLong()
  {
    return latLong;
  }

  /**
   * Sets the latLong value.
   * 
   * @param latLong The latLong to set.
   */
  public void setLatLong(GlobalPosition latLong)
  {
    this.latLong = latLong;
  }

  /**
   * Returns the city value.
   * 
   * @return Returns the city.
   */
  public String getCity()
  {
    return city;
  }

  /**
   * Sets the city value.
   * 
   * @param city The city to set.
   */
  public void setCity(String city)
  {
    this.city = city;
  }

  /**
   * Returns the region value.
   * 
   * @return Returns the region.
   */
  public String getRegion()
  {
    return region;
  }

  /**
   * Sets the region value.
   * 
   * @param region The region to set.
   */
  public void setRegion(String region)
  {
    this.region = region;
  }

  /**
   * Returns the countryCode value.
   * 
   * @return Returns the countryCode.
   */
  public String getCountryCode()
  {
    return countryCode;
  }

  /**
   * Sets the countryCode value.
   * 
   * @param countryCode The countryCode to set.
   */
  public void setCountryCode(String countryCode)
  {
    this.countryCode = countryCode;
  }

  /**
   * Returns the countryName value.
   * 
   * @return Returns the countryName.
   */
  public String getCountryName()
  {
    return countryName;
  }

  /**
   * Sets the countryName value.
   * 
   * @param countryName The countryName to set.
   */
  public void setCountryName(String countryName)
  {
    this.countryName = countryName;
  }

  /**
   * Returns the stateCode value.
   * 
   * @return Returns the stateCode.
   */
  public String getStateCode()
  {
    return stateCode;
  }

  /**
   * Sets the stateCode value.
   * 
   * @param stateCode The stateCode to set.
   */
  public void setStateCode(String stateCode)
  {
    this.stateCode = stateCode;
  }

  /**
   * Returns the stateName value.
   * 
   * @return Returns the stateName.
   */
  public String getStateName()
  {
    return stateName;
  }

  /**
   * Sets the stateName value.
   * 
   * @param stateName The stateName to set.
   */
  public void setStateName(String stateName)
  {
    this.stateName = stateName;
  }

  /**
   * Returns the zipCode value.
   * 
   * @return Returns the zipCode.
   */
  public String getZipCode()
  {
    return zipCode;
  }

  /**
   * Sets the zipCode value.
   * 
   * @param zipCode The zipCode to set.
   */
  public void setZipCode(String zipCode)
  {
    this.zipCode = zipCode;
  }

  /**
   * Returns the startedTime value.
   * 
   * @return Returns the startedTime.
   */
  public Date getStartedTime()
  {
    return startedTime;
  }

  /**
   * Sets the startedTime value.
   * 
   * @param startedTime The startedTime to set.
   */
  public void setStartedTime(Date startedTime)
  {
    this.startedTime = startedTime;
  }

}
