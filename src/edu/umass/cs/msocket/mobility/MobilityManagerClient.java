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

package edu.umass.cs.msocket.mobility;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.KeepAliveStaticThread;
import edu.umass.cs.msocket.MSocketConstants;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.TemporaryTasksES;
import edu.umass.cs.msocket.UDPControllerHashMap;
import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This is a singelton class, that implements mobility manager at client side.
 * Each client MSocket registers with the mobility manager and then mobility
 * manager handles the migration of each flowpath, depending on the active
 * interfaces on the device.
 * 
 * @version 1.0
 */
public class MobilityManagerClient implements Runnable
{

  // policies
  /**
   * Random policy to choose an interface
   */
  public static final int                                 POLICY_RANDOM             = 1;

  // the static singleton object
  private static MobilityManagerClient                    mobilityManagerObj        = null;
  private static HashMap<String, Vector<ConnectionState>> managerConnectionStateMap = null;
  private Vector<String>                                  activeInterfaceAddress    = null;

  private static boolean                                  running                   = true;

  /**
   * registers client side MSocket with the mobility manager
   * 
   * @param ControllerIPAddress
   * @param cInfo
   * @throws SocketException
   * @throws UnknownHostException
   */
  public synchronized static void registerWithManager(ConnectionInfo cInfo)
  {
    createSingleton();
    Vector<SocketInfo> currentSocketInfo = new Vector<SocketInfo>();
    currentSocketInfo.addAll(cInfo.getAllSocketInfo());

    for (int i = 0; i < currentSocketInfo.size(); i++)
    {
      SocketInfo Obj = currentSocketInfo.get(i);
      insertIntoConnectionStateMap(cInfo, Obj);
    }
  }

  /**
   * registers client side MSocket with the mobility manager
   * 
   * @param ControllerIPAddress
   * @param cInfo
   * @throws SocketException
   * @throws UnknownHostException
   */
  public synchronized static void unregisterWithManager(ConnectionInfo cInfo)
  {
    try
    {
      createSingleton();
      removeConnID(cInfo.getConnID());
      MSocketLogger.getLogger().fine("number of socket reg " + getConnectionStateSize());
    }
    catch (Exception ex)
    {
      MSocketLogger.getLogger().fine("unregisterWithManager excp " + ex.getMessage());
    }
  }

  /**
   * shuts down mobility manager as well as udp controllers TODO:
   * shutdownMobilityManager definition.
   */
  public static void shutdownMobilityManager()
  {
    running = false;
    UDPControllerHashMap.stopAllUDPControllers();
    TemporaryTasksES.shutdownES();
    KeepAliveStaticThread.stopKeepAlive();
    DefaultGNSClient.getGnsClient().close();
  }

  @Override
  public void run()
  {
    try
    {
      while (running)
      {
        Vector<String> currentInterfaceIPs = CommonMethods.getActiveInterfaceStringAddresses();
        Vector<String> notWorkingIPs = new Vector<String>();
        notWorkingIPs.clear();
        notWorkingIPs.add(""); // adding this, so that any Sockets which are not
                               // connected at all, also get selected

        for (int i = 0; i < activeInterfaceAddress.size(); i++)
        {
          boolean active = false;
          for (int j = 0; j < currentInterfaceIPs.size(); j++)
          {
            if (activeInterfaceAddress.get(i).equals(currentInterfaceIPs.get(j)))
            {
              active = true;
              break;
            }
          }
          if (!active)
          {
            notWorkingIPs.add(activeInterfaceAddress.get(i));
            MSocketLogger.getLogger().fine("not working IPs " + activeInterfaceAddress.get(i));
          }
        }

        // updating active IPs
        activeInterfaceAddress.clear();
        for (int i = 0; i < currentInterfaceIPs.size(); i++)
        {
          activeInterfaceAddress.add(currentInterfaceIPs.get(i));
        }

        for (int i = 0; i < notWorkingIPs.size(); i++)
        {
          Vector<ConnectionState> cvect = removeConnectionState(notWorkingIPs.get(i));
          if (cvect != null)
            performMigration(cvect);
        }

        // perform migration for the closed flow paths(closed bue to exception)
        Thread.sleep(1000); // runs every 1 sec
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    MSocketLogger.getLogger().fine("Mobility manager client thread exit");
  }

  private synchronized static Vector<ConnectionState> getConnectionState(String key)
  {
    return managerConnectionStateMap.get(key);
  }

  private synchronized static void addConnectionState(Vector<ConnectionState> states, String key)
  {
    managerConnectionStateMap.put(key, states);
  }

  private synchronized static Vector<ConnectionState> removeConnectionState(String key)
  {
    return managerConnectionStateMap.remove(key);
  }

  private synchronized static void removeConnID(Long connID)
  {
    for (Entry<String, Vector<ConnectionState>> entry : managerConnectionStateMap.entrySet())
    {
      String key = entry.getKey();
      Vector<ConnectionState> value = entry.getValue();
      Vector<ConnectionState> newValue = new Vector<ConnectionState>();

      for (int i = 0; i < value.size(); i++)
      {
        if (value.get(i).connecInfo.getConnID() == connID)
        {
          // do nothing,

        }
        else
        {
          newValue.add(value.get(i));
          // to eb put again
        }
      }
      managerConnectionStateMap.put(key, newValue);
    }
  }

  private synchronized static int getConnectionStateSize()
  {
    int numSockets = 0;
    for (Entry<String, Vector<ConnectionState>> entry : managerConnectionStateMap.entrySet())
    {
      Vector<ConnectionState> value = entry.getValue();
      numSockets += value.size();
    }
    return numSockets;
  }

  private synchronized static void insertIntoConnectionStateMap(ConnectionInfo connecInfo, SocketInfo socketInfo)
  {
    // FIXME: need to check if this ip address is still valid , before inserting
    String localIpAddress = socketInfo.getSocket().getLocalAddress().getHostAddress();
    MSocketLogger.getLogger().fine("insertIntoConnectionStateMap " + localIpAddress);
    if (managerConnectionStateMap.containsKey(localIpAddress))
    {
      ConnectionState cstate = new ConnectionState(connecInfo, socketInfo);
      Vector<ConnectionState> cvect = getConnectionState(localIpAddress);
      cvect.add(cstate);
      addConnectionState(cvect, localIpAddress);
    }
    else
    {
      Vector<ConnectionState> cvect = new Vector<ConnectionState>();
      ConnectionState cstate = new ConnectionState(connecInfo, socketInfo);
      cvect.add(cstate);
      addConnectionState(cvect, localIpAddress);
    }
  }

  private String getNewInterface(int policy)
  {
    switch (policy)
    {
      case POLICY_RANDOM :
      {
        Random rand = new Random();
        if (activeInterfaceAddress.size() > 0)
          return activeInterfaceAddress.get(rand.nextInt(activeInterfaceAddress.size()));
        else
        {
          return "";
        }
      }
    }
    return "";
  }

  /**
   * returns if given address is currently active
   * 
   * @return
   */
  private boolean ifAddressActive(String Address)
  {
    boolean ret = false;
    for (int i = 0; i < activeInterfaceAddress.size(); i++)
    {
      if (Address.equals(activeInterfaceAddress.get(i)))
      {
        ret = true;
        break;
      }
    }
    return ret;
  }

  private void performMigration(Vector<ConnectionState> csvector)
  {
    for (int i = 0; i < csvector.size(); i++)
    {
      try
      {
        String newInterface = getNewInterface(POLICY_RANDOM);
        MSocketLogger.getLogger().fine("performMigration newInterface " + newInterface);

        if (newInterface == "") // no active interface to migrate to
        {
          Vector<ConnectionState> vect = getConnectionState(newInterface);
          if (vect == null)
          {
            vect = new Vector<ConnectionState>();
          }
          vect.add(csvector.get(i));
          addConnectionState(vect, newInterface);

          continue;
        }

        ConnectionState cstate = csvector.get(i);
        if (ifAddressActive(cstate.connecInfo.getControllerIP().getHostAddress()))
        { // active nothing to do

        }
        else
        {
          UDPControllerHashMap.updateWithController(InetAddress.getByName(newInterface), 
        		  cstate.connecInfo.getConnID());
          
          cstate.connecInfo.setControllerIP(InetAddress.getByName(newInterface));
        }

        boolean success = cstate.connecInfo.migrateSocketwithId(InetAddress.getByName(newInterface), 0,
            cstate.socketObj.getSocketIdentifer(), MSocketConstants.CLIENT_MIG);

        if (!success)
        {
          throw new Exception("migrateSocketwithId falied");
        }
        MSocketLogger.getLogger().fine("Completed client migration of socket Id " + cstate.socketObj.getSocketIdentifer() + "to interface "
            + newInterface);

        Vector<ConnectionState> vect = getConnectionState(newInterface);
        if (vect == null)
        {
          vect = new Vector<ConnectionState>();
        }
        vect.add(csvector.get(i));
        addConnectionState(vect, newInterface);
      }
      catch (Exception ex)
      {
        // migration failed for some reason, put it in "" IP vector of manager.
        MSocketLogger.getLogger().fine("migratio of socketId " + csvector.get(i).socketObj.getSocketIdentifer() + " failed");
        String failedIP = "";
        Vector<ConnectionState> vect = getConnectionState(failedIP);
        if (vect == null)
        {
          vect = new Vector<ConnectionState>();
        }
        vect.add(csvector.get(i));
        addConnectionState(vect, failedIP);
      }
    }
  }

  /**
   * private constructor
   */
  private MobilityManagerClient()
  {
    managerConnectionStateMap = new HashMap<String, Vector<ConnectionState>>();
    activeInterfaceAddress = new Vector<String>();
  }

  /**
   * Checks if the singleton object is created or not, if not it creates the
   * object and then the object is returned.
   * 
   * @return the singleton object
   */
  private static void createSingleton()
  {
    if (mobilityManagerObj == null)
    {
      mobilityManagerObj = new MobilityManagerClient();
      new Thread(mobilityManagerObj).start();
    }
  }
  
}