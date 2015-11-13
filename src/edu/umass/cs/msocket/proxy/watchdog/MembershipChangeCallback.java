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
 * This class defines a MembershipChangeCallback
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface MembershipChangeCallback
{

  /**
   * Called when a member is added to the list. The guid of the member is passed
   * as a parameter. The callback should not be blocking in any way otherwise
   * this will bog down the thread of the scanner.
   * 
   * @param guid guid added to the list
   * @param listName name of the list where the change happened
   */
  public void memberAddedCallback(String guid, String listName);

  /**
   * Called when a member is removed from the list. The guid of the member is
   * passed as a parameter. The callback should not be blocking in any way
   * otherwise this will bog down the thread of the scanner.
   * 
   * @param guid guid removed from the list
   * @param listName name of the list where the change happened
   */
  public void memberRemovedCallback(String guid, String listName);

}
