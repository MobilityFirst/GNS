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





import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.msocket.common.Constants;

/**
 * This class defines a WatchdogWatcherThread that regularly polls the
 * CURRENT_TIME field of the GUID to monitor its progress.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class WatchdogWatcherThread extends Thread
{
  private boolean                    isKilled  = false;
  private GNSClientCommands         gnsClient;
  private GuidEntry                  watchdogGuid;
  private String                     targetGuid;
  private long                       suspiciousTimeout;
  private long                       failureTimeout;
  private List<StatusChangeCallback> callbacks = new LinkedList<StatusChangeCallback>();
  private ServiceStatus              currentStatus;
  private static final Logger        logger    = Logger.getLogger("Watchdog");

  /**
   * Creates a new <code>WatchdogListScanner</code> object
   * 
   * @param gnsClient GNS connection
   * @param watchdogGuid GUID entry for this watchdog service
   * @param targetGuid GUID of the listname we have to read
   * @param listName name of the field where the list is stored
   * @param suspiciousTimeout timeout in ms before the target is declared
   *          suspicious
   * @param failureTimeout timeout in ms before the target is declared
   *          inactive/failde
   */
  public WatchdogWatcherThread(GNSClientCommands gnsClient, GuidEntry watchdogGuid, String targetGuid,
      long suspiciousTimeout, long failureTimeout)
  {
    this.gnsClient = gnsClient;
    this.watchdogGuid = watchdogGuid;
    this.targetGuid = targetGuid;
    this.suspiciousTimeout = suspiciousTimeout;
    this.failureTimeout = failureTimeout;
  }

  /**
   * Register a callback to be notified of status changes
   * 
   * @param callback
   */
  public void registerStatusChangeCallback(StatusChangeCallback callback)
  {
    callbacks.add(callback);
  }

  /**
   * Unregister a previously registered callback
   * 
   * @param callback the callback to unregister
   * @return true if the callback was removed, false if it has not been found
   */
  public boolean unregisterMembershipChangeCallback(StatusChangeCallback callback)
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
    currentStatus = ServiceStatus.STATUS_UNKNOWN;
    long remoteClock = readRemoteTime();
    long refreshFrequencyInMs = 0;
    long lastFailureRemoteTime = 0;
    try
    {
      refreshFrequencyInMs = gnsClient.fieldReadArray(targetGuid, Constants.TIME_REFRESH_INTERVAL, watchdogGuid).getLong(
          0);
      logger.info("GUID " + targetGuid + " refreshes every " + refreshFrequencyInMs + " ms");
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to read TIME_REFRESH_INTERVAL on " + targetGuid, e);
    }

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
      long newRemoteTime = readRemoteTime();
      long now = System.currentTimeMillis();
      if (newRemoteTime == remoteClock)
      { // Time has not changed, check if a timeout has expired
        if (now - last > failureTimeout)
        {
          lastFailureRemoteTime = newRemoteTime;
          notifyStatusChange(ServiceStatus.STATUS_INACTIVE);
        }
        else if (now - last > suspiciousTimeout)
          notifyStatusChange(ServiceStatus.STATUS_SUSPICIOUS);
      }
      else if (currentStatus != ServiceStatus.STATUS_ACTIVE)
      { // Clock made progress and we are not active, let's check this out
        if (currentStatus == ServiceStatus.STATUS_UNKNOWN || (currentStatus == ServiceStatus.STATUS_SUSPICIOUS))
          notifyStatusChange(ServiceStatus.STATUS_ACTIVE);
        else
        { // Status was inactive
          if (newRemoteTime - lastFailureRemoteTime > failureTimeout)
          { // Reset the failure time to see if we are getting a heartbeat
            lastFailureRemoteTime = newRemoteTime;
          }
          else
          { // Clock is ticking we are active
            notifyStatusChange(ServiceStatus.STATUS_ACTIVE);
          }
        }
      }
      last = now;

    }
  }

  /**
   * If the status has changed, all callbacks are notified and currentStatus is
   * updated to the new status.
   * 
   * @param newStatus the new status of the service
   */
  private void notifyStatusChange(ServiceStatus newStatus)
  {
    if (currentStatus == newStatus)
      return; // no status change

    for (StatusChangeCallback callback : callbacks)
    {
      callback.statusChanged(targetGuid, currentStatus, newStatus);
    }
    currentStatus = newStatus;
  }

  private long readRemoteTime()
  {
    try
    {
      return gnsClient.fieldReadArray(targetGuid, Constants.CURRENT_TIME, watchdogGuid).getLong(0);
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to read CURRENT_TIME on " + targetGuid, e);
      return 0;
    }
  }

}
