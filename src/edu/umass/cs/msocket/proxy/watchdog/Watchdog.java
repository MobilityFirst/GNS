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

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;



/**
 * This class defines a Watchdog
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class Watchdog extends Thread implements MembershipChangeCallback, StatusChangeCallback
{
  private String                                 activeListName;
  private String                                 suspiciousListName;
  private String                                 inactiveListName;
  private long                                   suspiciousTimeout;
  private long                                   failureTimeout;
  private GNSClientCommands                     gnsClient;
  private GuidEntry                              watchdogGuid;
  private String                                 targetGuid;
  private long                                   refreshFrequencyInMs;
  private HashMap<String, WatchdogWatcherThread> watchers = new HashMap<String, WatchdogWatcherThread>();
  private WatchdogListScanner                    activeWatch;
  private WatchdogListScanner                    suspiciousWatch;
  private WatchdogListScanner                    inactiveWatch;
  private static final Logger                    logger   = Logger.getLogger("Watchdog");

  /**
   * Creates a new <code>Watchdog</code> object
   * 
   * @param gnsClient GNS connection
   * @param watchdogGuid GUID entry for this watchdog service
   * @param targetGuid GUID of the lists we have to read
   * @param refreshFrequencyInMs how often we should read the list
   * @param activeListName
   * @param suspiciousListName
   * @param inactiveListName
   * @param suspiciousTimeout
   * @param failureTimeout
   */
  public Watchdog(GNSClientCommands gnsClient, GuidEntry watchdogGuid, String targetGuid, long refreshFrequencyInMs,
      String activeListName, String suspiciousListName, String inactiveListName, long suspiciousTimeout,
      long failureTimeout)
  {
    this.gnsClient = gnsClient;
    this.watchdogGuid = watchdogGuid;
    this.targetGuid = targetGuid;
    this.refreshFrequencyInMs = refreshFrequencyInMs;
    this.activeListName = activeListName;
    this.suspiciousListName = suspiciousListName;
    this.inactiveListName = inactiveListName;
    this.suspiciousTimeout = suspiciousTimeout;
    this.failureTimeout = failureTimeout;
  }

  /**
   * Terminate this Watchdog
   */
  public void killIt()
  {
    if (activeWatch != null)
      activeWatch.killIt();
    if (suspiciousWatch != null)
      suspiciousWatch.killIt();
    if (inactiveWatch != null)
      inactiveWatch.killIt();
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    activeWatch = new WatchdogListScanner(gnsClient, watchdogGuid, targetGuid, activeListName, refreshFrequencyInMs);
    activeWatch.registerMembershipChangeCallback(this);
    suspiciousWatch = new WatchdogListScanner(gnsClient, watchdogGuid, targetGuid, suspiciousListName,
        refreshFrequencyInMs);
    suspiciousWatch.start();
    inactiveWatch = new WatchdogListScanner(gnsClient, watchdogGuid, targetGuid, inactiveListName, refreshFrequencyInMs);
    inactiveWatch.start();
  }

  @Override
  public void memberAddedCallback(String guid, String listName)
  {
    if (activeListName.equals(listName))
    { // We need to create a new watcher thread
      if (!watchers.containsKey(guid))
      { // Make sure that we don't already have a watcher in case the guid went
        // inactive and then active again
        WatchdogWatcherThread thread = new WatchdogWatcherThread(gnsClient, watchdogGuid, guid, suspiciousTimeout,
            failureTimeout);
        thread.registerStatusChangeCallback(this);
        watchers.put(guid, thread);
        thread.start();
      }
    }
  }

  @Override
  public void memberRemovedCallback(String guid, String listName)
  {

  }

  @Override
  public void statusChanged(String guid, ServiceStatus oldStatus, ServiceStatus newStatus)
  {
    logger.info("GUID " + guid + ": Status changed from " + oldStatus + " to " + newStatus);
    try
    {
      // Remove from the old list
      switch (oldStatus)
      {
        case STATUS_ACTIVE :
          gnsClient.fieldClear(targetGuid, activeListName, new JSONArray().put(guid), watchdogGuid);
          break;
        case STATUS_SUSPICIOUS :
          gnsClient.fieldClear(targetGuid, suspiciousListName, new JSONArray().put(guid), watchdogGuid);
          break;
        case STATUS_INACTIVE :
          gnsClient.fieldClear(targetGuid, inactiveListName, new JSONArray().put(guid), watchdogGuid);
          break;
        default : // Unknown, just ignore
          break;
      }
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to remove guid " + guid + " from the " + oldStatus + " list", e);
    }

    try
    {
      // Add to the new list
      switch (newStatus)
      {
        case STATUS_ACTIVE :
          gnsClient.fieldAppend(targetGuid, activeListName, new JSONArray().put(guid), watchdogGuid);
          break;
        case STATUS_SUSPICIOUS :
          gnsClient.fieldAppend(targetGuid, suspiciousListName, new JSONArray().put(guid), watchdogGuid);
          break;
        case STATUS_INACTIVE :
          gnsClient.fieldAppend(targetGuid, inactiveListName, new JSONArray().put(guid), watchdogGuid);
          break;
        default : // Unknown, just ignore
          break;
      }
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to add guid " + guid + " to the " + oldStatus + " list", e);
    }
  }
}
