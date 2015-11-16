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

package edu.umass.cs.msocket.proxy.watchdog;

/**
 * This class defines a ServiceStatus
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public enum ServiceStatus
{
  /**
   * When the service is active and healthy
   */
  STATUS_ACTIVE,
  /**
   * When the service is suspected of failure
   */
  STATUS_SUSPICIOUS,
  /**
   * When the service is stopped or has failed
   */
  STATUS_INACTIVE,
  /**
   * When the status of a service is not known yet
   */
  STATUS_UNKNOWN;

  /**
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString()
  {
    return name();
  }
}
