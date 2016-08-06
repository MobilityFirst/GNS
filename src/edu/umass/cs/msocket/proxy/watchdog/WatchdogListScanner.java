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

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;



/**
 * This class defines a WatchdogListScanner
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class WatchdogListScanner extends Thread
{
  private String                         listName;
  private List<String>                   guidList  = new LinkedList<String>();
  private List<MembershipChangeCallback> callbacks = new LinkedList<MembershipChangeCallback>();
  private long                           refreshFrequencyInMs;
  private boolean                        isKilled  = false;
  private GNSClientCommands             gnsClient;
  private GuidEntry                      watchdogGuid;
  private String                         targetGuid;
  private static final Logger            logger    = Logger.getLogger("Watchdog");

  /**
   * Creates a new <code>WatchdogListScanner</code> object
   * 
   * @param gnsClient GNS connection
   * @param watchdogGuid GUID entry for this watchdog service
   * @param targetGuid GUID of the listname we have to read
   * @param listName name of the field where the list is stored
   * @param refreshFrequencyInMs how often we should read the list
   */
  public WatchdogListScanner(GNSClientCommands gnsClient, GuidEntry watchdogGuid, String targetGuid, String listName,
      long refreshFrequencyInMs)
  {
    this.gnsClient = gnsClient;
    this.watchdogGuid = watchdogGuid;
    this.targetGuid = targetGuid;
    this.listName = listName;
    this.refreshFrequencyInMs = refreshFrequencyInMs;
    logger.setLevel(Level.FINE);
  }

  /**
   * Register a callback to be notified of membership changes (additions or
   * deletions)
   * 
   * @param callback
   */
  public void registerMembershipChangeCallback(MembershipChangeCallback callback)
  {
    callbacks.add(callback);
  }

  /**
   * Unregister a previously registered callback
   * 
   * @param callback the callback to unregister
   * @return true if the callback was removed, false if it has not been found
   */
  public boolean unregisterMembershipChangeCallback(MembershipChangeCallback callback)
  {
    return callbacks.remove(callback);
  }

  /**
   * Call this method to terminate the thread
   */
  public void killIt()
  {
    isKilled = true;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    long last = System.currentTimeMillis();
    while (!isKilled)
    {
      long sleepFor = last + refreshFrequencyInMs - System.currentTimeMillis();
      if (sleepFor > 0)
        try
        {
          Thread.sleep(sleepFor);
        }
        catch (InterruptedException ignore)
        {
        }

      last = System.currentTimeMillis();
      try
      {
        logger.fine("Fetching " + listName);
        JSONArray guids = gnsClient.fieldReadArray(targetGuid, listName, watchdogGuid);
        // Duplicate the list in a friendly format
        List<String> newList = new LinkedList<String>();
        for (int i = 0; i < guids.length(); i++)
        {
          String guid = guids.getString(i);
          newList.add(guid);
        }

        // Look for additions
        for (String guid : newList)
        {
          if (!guidList.contains(guid))
          {
            logger.fine("Found new guid " + guid + " in list " + listName);
            guidList.add(guid);
            // Notify callbacks
            for (MembershipChangeCallback callback : callbacks)
            {
              callback.memberAddedCallback(guid, listName);
            }
          }
        }
        // Look for removal
        for (String guid : guidList)
        {
          if (!newList.contains(guid))
          {
            logger.fine(guid + " is no more in list " + listName);
            // Notify callbacks
            for (MembershipChangeCallback callback : callbacks)
            {
              callback.memberRemovedCallback(guid, listName);
            }
          }
        }

        // Switch to the new list
        guidList = newList;
      }
      catch (Exception e)
      {
        logger.log(Level.WARNING, "Failed to read list " + listName + " in GUID " + targetGuid, e);
      }
    }
  }

  /**
   * Returns the listName value.
   * 
   * @return Returns the listName.
   */
  public String getListName()
  {
    return listName;
  }

  /**
   * Returns the guidList value.
   * 
   * @return Returns the guidList.
   */
  public List<String> getGuidList()
  {
    return guidList;
  }

  /**
   * Returns the refreshFrequencyInMs value.
   * 
   * @return Returns the refreshFrequencyInMs.
   */
  public long getRefreshFrequencyInMs()
  {
    return refreshFrequencyInMs;
  }

}
