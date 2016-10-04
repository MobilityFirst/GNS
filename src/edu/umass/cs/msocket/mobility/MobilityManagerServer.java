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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.msocket.KeepAliveStaticThread;
import edu.umass.cs.msocket.MServerSocketController;
import edu.umass.cs.msocket.TemporaryTasksES;
import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the server side mobility manager. MServerSocket
 * registers with the mobility manager and then mobility manager migrates the
 * listening address of the server, if the device active addresses change.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MobilityManagerServer implements Runnable
{
  // policies
  /**
   * Random policy to choose an interface
   */
  public static final int                                       POLICY_RANDOM             = 1;

  private static boolean                                        running                   = true;

  // the static singleton object
  private static MobilityManagerServer                          mobilityManagerObj        = null;
  private static HashMap<String, Vector<ConnectionStateServer>> managerConnectionStateMap = null;
  private Vector<String>                                        activeInterfaceAddress    = null;

  /**
   * TODO: registerWithManager definition.
   * 
   * @param mServerSocketController
   */
  public synchronized static void registerWithManager(MServerSocketController mServerSocketController)
  {
    createSingleton();
    insertIntoConnectionStateMap(mServerSocketController);
  }

  /**
   * TODO: shutdownMobilityManager definition.
   */
  public static void shutdownMobilityManager()
  {
    running = false;
    TemporaryTasksES.shutdownES();
    KeepAliveStaticThread.stopKeepAlive();
    DefaultGNSClient.getGnsClient().close(); 
  }

  /**
   * TODO: unregisterWithManager definition.
   * 
   * @param mServerSocketController
   */
  public synchronized static void unregisterWithManager(MServerSocketController mServerSocketController)
  {
    createSingleton();
    removeServer();
    MSocketLogger.getLogger().fine("num registered with " + getConnectionStateSize());
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
          Vector<ConnectionStateServer> cvect = removeConnectionState(notWorkingIPs.get(i));
          if (cvect != null)
            performMigration(cvect);
        }
        Thread.sleep(1000); // runs every 1 sec
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    MSocketLogger.getLogger().fine("MobilityManagerServer thread exit");
  }

  private synchronized static Vector<ConnectionStateServer> getConnectionState(String key)
  {
    return managerConnectionStateMap.get(key);
  }

  private synchronized static void addConnectionState(Vector<ConnectionStateServer> states, String key)
  {
    managerConnectionStateMap.put(key, states);
  }

  private synchronized static Vector<ConnectionStateServer> removeConnectionState(String key)
  {
    return managerConnectionStateMap.remove(key);
  }

  private synchronized static int getConnectionStateSize()
  {
    int numServers = 0;
    for (Entry<String, Vector<ConnectionStateServer>> entry : managerConnectionStateMap.entrySet())
    {
      Vector<ConnectionStateServer> value = entry.getValue();
      numServers += value.size();
    }
    return numServers;
  }

  private synchronized static void removeServer()
  {
    for (Entry<String, Vector<ConnectionStateServer>> entry : managerConnectionStateMap.entrySet())
    {
      String key = entry.getKey();
      Vector<ConnectionStateServer> value = entry.getValue();
      Vector<ConnectionStateServer> newValue = new Vector<ConnectionStateServer>();

      for (int i = 0; i < value.size(); i++)
      {
        if (value.get(i).mServerSocketController.isClosed())
        {
          // do nothing
        }
        else
        {
          newValue.add(value.get(i));
          // to be put again
        }
      }
      managerConnectionStateMap.put(key, newValue);
    }
  }

  private synchronized static void insertIntoConnectionStateMap(MServerSocketController mServerSocketController)
  {
    // FIXME: need to check if this ip address is still valid , before inserting
    String localIpAddress = mServerSocketController.getMServerSocket().getInetAddress().getHostAddress();
    MSocketLogger.getLogger().fine("insertIntoConnectionStateMap " + localIpAddress);
    if (managerConnectionStateMap.containsKey(localIpAddress))
    {
      ConnectionStateServer cstate = new ConnectionStateServer(mServerSocketController);
      Vector<ConnectionStateServer> cvect = getConnectionState(localIpAddress);

      cvect.add(cstate);
      addConnectionState(cvect, localIpAddress);
    }
    else
    {
      Vector<ConnectionStateServer> cvect = new Vector<ConnectionStateServer>();
      ConnectionStateServer cstate = new ConnectionStateServer(mServerSocketController);
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

  private void performMigration(Vector<ConnectionStateServer> csvector)
  {
    for (int i = 0; i < csvector.size(); i++)
    {
      try
      {
        String newInterface = getNewInterface(POLICY_RANDOM);
        MSocketLogger.getLogger().fine("performMigration newInterface " + newInterface);

        if (newInterface == "") // no active interface to migrate to
        {
          Vector<ConnectionStateServer> Vect = getConnectionState(newInterface);
          if (Vect == null)
          {
            Vect = new Vector<ConnectionStateServer>();
          }
          Vect.add(csvector.get(i));
          addConnectionState(Vect, newInterface);
          continue;
        }

        ConnectionStateServer cstate = csvector.get(i);

        int newPort = 0;
        cstate.mServerSocketController.getMServerSocket().migrate(InetAddress.getByName(newInterface), newPort);

        MSocketLogger.getLogger().fine("Completed server migration to interface " + newInterface + "port " + newPort);

        Vector<ConnectionStateServer> vect = getConnectionState(newInterface);
        if (vect == null)
        {
          vect = new Vector<ConnectionStateServer>();
        }
        vect.add(csvector.get(i));
        addConnectionState(vect, newInterface);
      }
      catch (Exception ex)
      {

        // migration failed for some reason, put it in "" IP vector of manager.
        MSocketLogger.getLogger().fine("migration failed");
        String failedIP = "";
        Vector<ConnectionStateServer> vect = getConnectionState(failedIP);
        if (vect == null)
        {
          vect = new Vector<ConnectionStateServer>();
        }

        vect.add(csvector.get(i));
        addConnectionState(vect, failedIP);
      }
    }
  }

  /**
   * private constructor
   */
  private MobilityManagerServer()
  {
    managerConnectionStateMap = new HashMap<String, Vector<ConnectionStateServer>>();
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
      mobilityManagerObj = new MobilityManagerServer();
      new Thread(mobilityManagerObj).start();
    }
  }
}