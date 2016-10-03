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

package edu.umass.cs.msocket;

import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Vector;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class stores the Hashmap of UDP controllers running on different
 * interfaces on a MSocket client side. MSocket client asks for a UDP controller
 * on a interface, if there is already a running one, then this class returns
 * that, otherwise starts a new one and then returns that.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */

public class UDPControllerHashMap
{

  // the static singleton object
  private static UDPControllerHashMap                            UDPControllerHashMapObject = null;
  private static HashMap<InetAddress, FlowIDToControllerMapping> ControllerSocketMap        = null;

  /**
   * @param ControllerIPAddress
   * @param cinfo
   * @throws SocketException
   */
  public static synchronized void registerWithController(InetAddress ControllerIPAddress, ConnectionInfo cinfo)
      throws SocketException
  {
    MSocketLogger.getLogger().fine("RegisterWithController " + ControllerIPAddress.toString());
    createSingleton();

    // UDP socket already there
    if (ControllerSocketMap.containsKey(ControllerIPAddress))
    {
      ControllerSocketMap.get(ControllerIPAddress).addControllerMapping
      		(cinfo.getConnID(), cinfo);
    }
    else
    {
      FlowIDToControllerMapping Obj = new FlowIDToControllerMapping(ControllerIPAddress);
      Obj.addControllerMapping(cinfo.getConnID(), cinfo);
      ControllerSocketMap.put(ControllerIPAddress, Obj);
      // start UDP listening thread
      (new Thread(Obj)).start();
      Obj.startKeepAliveThread();
    }
  }

  /**
   * @param ControllerIPAddress
   * @param cinfo
   * @throws SocketException
   */
  public static synchronized void unregisterWithController(InetAddress ControllerIPAddress, ConnectionInfo cinfo)
  {
    try
    {
      MSocketLogger.getLogger().fine("RegisterWithController " + ControllerIPAddress.toString());
      createSingleton();

      // UDP socekt already there
      if (ControllerSocketMap.containsKey(ControllerIPAddress))
      {
        ControllerSocketMap.get(ControllerIPAddress).removeControllerMapping
        		(cinfo.getConnID());
      }
      else
      {
        MSocketLogger.getLogger().fine("non existent controller IP, shoudl nt happen");
      }
    }
    catch (Exception ex)
    {
      MSocketLogger.getLogger().fine("unregisterWithController excp " + ex.getMessage());
    }
  }

  /**
   * @param ControllerIPAddress
   * @throws SocketException
   */
  public static synchronized void startUDPController(InetAddress ControllerIPAddress) throws SocketException
  {
    MSocketLogger.getLogger().fine("ControllerIPAddress " + ControllerIPAddress.toString());
    createSingleton();

    // UDP socket already there
    if (ControllerSocketMap.containsKey(ControllerIPAddress))
    {

    }
    else
    {
      FlowIDToControllerMapping Obj = new FlowIDToControllerMapping(ControllerIPAddress);
      ControllerSocketMap.put(ControllerIPAddress, Obj);
      // start UDP listening thread
      (new Thread(Obj)).start();
      Obj.startKeepAliveThread();
    }
  }

  /**
   * @param ControllerIPAddress
   * @throws SocketException
   */
  public static synchronized void stopAllUDPControllers()
  {
    createSingleton();
    Vector<FlowIDToControllerMapping> UDPControllers = new Vector<FlowIDToControllerMapping>();
    UDPControllers.addAll(ControllerSocketMap.values());
    int i = 0;
    for (i = 0; i < UDPControllers.size(); i++)
    {
      UDPControllers.get(i).closeUDPController();
    }
    // deleting the state
    UDPControllerHashMapObject = null;
    ControllerSocketMap = null;
  }

  /**
   * @param ControllerIPAddress
   * @param cinfo
   * @throws SocketException
   */
  public static synchronized int getUDPContollerPort(InetAddress ControllerIPAddress) throws SocketException
  {
    if (ControllerIPAddress == null)
      return -1;
    return ControllerSocketMap.get(ControllerIPAddress).getLocalPort();
  }

  /**
   * update controller
   * 
   * @param OldIPAddress
   * @param NewIPAddress
   * @param flowID
   * @throws SocketException
   */
  public static synchronized void updateWithController(InetAddress NewIPAddress, long flowID) throws SocketException
  {
    // FIXME: do null checks
    // FIXME: not very efficeint but not error prone, instead of asking for
    // OldIP address
    for (Object obj : ControllerSocketMap.values())
    {
      FlowIDToControllerMapping FTC = (FlowIDToControllerMapping) obj;
      ConnectionInfo cinfo = FTC.removeControllerMapping(flowID);
      if (cinfo != null)
      {
        // UDP socekt already there
        if (ControllerSocketMap.containsKey(NewIPAddress))
        {
          ControllerSocketMap.get(NewIPAddress).addControllerMapping(flowID, cinfo);
        }
        else
        {
          FlowIDToControllerMapping Obj = new FlowIDToControllerMapping(NewIPAddress);
          Obj.addControllerMapping(flowID, cinfo);
          ControllerSocketMap.put(NewIPAddress, Obj);
          // start UDP listening thread
          (new Thread(Obj)).start();
          Obj.startKeepAliveThread();
        }
        break;
      }
    }
  }

  /**
   * private constructor
   */
  private UDPControllerHashMap()
  {
    ControllerSocketMap = new HashMap<InetAddress, FlowIDToControllerMapping>();
  }

  /**
   * Checks if the singleton object is created or not, if not it creates the
   * object and then the object is returned.
   * 
   * @return the singleton object
   */
  private static void createSingleton()
  {
    if (UDPControllerHashMapObject == null)
      UDPControllerHashMapObject = new UDPControllerHashMap();
  }
  
}