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

/**
 * This class defines all constants to name fields and other entities in the
 * GNS.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class Constants
{
  /**
   * Field used to store the start time of the proxy using its local clock.
   */
  public static final String PROXY_START_TIME          = "START_TIME";

  /**
   * Field used to store the current time of the proxy using its local clock.
   * This is used for failure detection when this field does not make progress
   * anymore.
   */
  public static final String PROXY_CURRENT_TIME        = "CURRENT_TIME";

  /**
   * Field used to store the external public IP address of the proxy
   */
  public static final String PROXY_EXTERNAL_IP_FIELD   = "EXTERNAL_IP";

  /**
   * Field used to store the load of the proxy
   */
  public static final String PROXY_LOAD                = "PROXY_LOAD";

  /**
   * Name used for unverified lists of GUIDs trying to join a group
   */
  public static final String UNVERIFIED                = "UNVERIFIED";

  /**
   * Field where the service type (proxy, watchtog, loation...) is stored.
   */
  public static final String SERVICE_TYPE_FIELD        = "SERVICE_TYPE";

  /**
   * Value for {@link #SERVICE_TYPE_FIELD} for a proxy
   */
  public static final String PROXY_SERVICE             = "PROXY_SERVICE";
  /**
   * Value for {@link #SERVICE_TYPE_FIELD} for a watchdog service
   */
  public static final String WATCHDOG_SERVICE          = "WATCHDOG_SERVICE";
  /**
   * Value for {@link #SERVICE_TYPE_FIELD} for a location service
   */
  public static final String LOCATION_SERVICE          = "LOCATION_SERVICE";

  /**
   * Field where verified active proxies are registered.
   */
  public static final String ACTIVE_PROXY_FIELD        = "ACTIVE_PROXY";

  /**
   * Field where verified inactive proxies are registered.
   */
  public static final String INACTIVE_PROXY_FIELD      = "INACTIVE_PROXY";

  /**
   * Field where verified proxies that are suspected of failure are stored.
   */
  public static final String SUSPICIOUS_PROXY_FIELD    = "SUSPICIOUS_PROXY";

  /**
   * Field where verified active watchdog services are registered.
   */
  public static final String ACTIVE_WATCHDOG_FIELD     = "ACTIVE_WATCHDOG";

  /**
   * Field where verified inactive watchdog services are registered.
   */
  public static final String INACTIVE_WATCHDOG_FIELD   = "INACTIVE_WATCHDOG";

  /**
   * Field where verified proxies that are suspected of failure are stored.
   */
  public static final String SUSPICIOUS_WATCHDOG_FIELD = "SUSPICIOUS_WATCHDOG";

  /**
   * Field where verified active location services are registered.
   */
  public static final String ACTIVE_LOCATION_FIELD     = "ACTIVE_LOCATION";

  /**
   * Field where verified inactive location services are registered.
   */
  public static final String INACTIVE_LOCATION_FIELD   = "INACTIVE_LOCATION";

  /**
   * Field where verified proxies that are suspected of failure are stored.
   */
  public static final String SUSPICIOUS_LOCATION_FIELD = "SUSPICIOUS_LOCATION";

  /**
   * Field storing the IP:port where the Location service can be contacted.
   */
  public static final String LOCATION_SERVICE_IP       = "LOCATION_SERVICE_IP";

  /**
   * Field storing the start time of a service, usually the local time returned
   * by System.currentTimeMillis()
   */
  public static final String START_TIME                = "START_TIME";

  /**
   * Field storing the current time, usually the local time returned by
   * System.currentTimeMillis(). Used by the watchdog service to determine
   * liveness.
   */
  public static final String CURRENT_TIME              = "CURRENT_TIME";

  /**
   * Field storing the time in milliseconds between updates of CURRENT_TIME
   */
  public static final String TIME_REFRESH_INTERVAL     = "TIME_REFRESH_INTERVAL";

  /**
   * stores server's registered addresses as list
   */
  public static final String SERVER_REG_ADDR           = "SERVER_REG_ADDR";

  /**
   * stores the alias of a GUID, required in group membership to find alias
   * using GUID
   */
  public static final String ALIAS_FIELD               = "ALIAS_FIELD";

  /**
   * stores UDP addr of anything, used in storing udp of group member
   */
  public static final String UDP_ADDR_FIELD            = "UDP_ADDR_FIELD";

  /**
   * group name that the member wishes to be in
   */
  public static final String GROUP_FIELD               = "GROUP_FIELD";

  /**
   * Check if the provided list name is a valid list name registered in the GNS
   * as a field of the proxy group
   * 
   * @param listName the list name to check
   * @return true if the name is valid (backed by a field of the same name in
   *         the GNS)
   */
  public static boolean isValidList(String listName)
  {
    return INACTIVE_PROXY_FIELD.equals(listName) || ACTIVE_PROXY_FIELD.equals(listName)
        || SUSPICIOUS_PROXY_FIELD.equals(listName) || INACTIVE_WATCHDOG_FIELD.equals(listName)
        || ACTIVE_WATCHDOG_FIELD.equals(listName) || SUSPICIOUS_WATCHDOG_FIELD.equals(listName)
        || INACTIVE_LOCATION_FIELD.equals(listName) || ACTIVE_LOCATION_FIELD.equals(listName)
        || SUSPICIOUS_LOCATION_FIELD.equals(listName);
  }

}