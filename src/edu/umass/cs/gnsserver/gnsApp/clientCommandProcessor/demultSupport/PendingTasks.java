/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.packet.RequestActivesPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Initiates tasks to obtain the set of active replicas for a name from replica controller, and stores requests for
 * that name in a queue while their set of active replicas is being obtained. If local name server successfully obtains
 * the set of active replicas, all queued up requests for that name are executed. If set of active replicas of a name
 * is not received until a given timeout, error messages are sent to clients for all queued up requests for that name.
 *
 *
 *
 * @see edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.RequestActivesTask
 *
 * @author abhigyan
 */
public class PendingTasks {

  /**
   * **********BEGIN: Accesses to these fields is synchronized*****************
   */
  /**
   * This key of this map is name/GUID and values are a list of pending requests for that name that will be executed
   * after we receive the set of active replicas for name.
   */
  private static HashMap<String, ArrayList<PendingTask>> allTasks = new HashMap<String, ArrayList<PendingTask>>();

  /**
   * Set of request IDs of <code>RequestActivesTask</code> that are currently running.
   */
  private static HashSet<Integer> requestActivesOngoing = new HashSet<Integer>();

  /**
   * The request ID for next <code>RequestActivesTask</code> is uniquely determined by incrementing this value by 1.
   * This assignment of request IDs ensures uniqueness.
   */
  private static int requestIDCounter = 0;

  /**
   * **********END: Accesses to these fields is synchronized*****************
   */
  /**
   * Request current set of actives for name, and queue this request to be executed once we receive the current actives.
   *
   * @param requestInfo Information for this request stored at LNS.
   * @param task TimerTask to be executed once actives are received. Request represented in form of TimerTask
   * @param period Frequency at which TimerTask is repeated
   * @param handler
   */
  public static void addToPendingRequests(RequestInfo requestInfo, TimerTask task, int period, ClientRequestHandlerInterface handler) {
    if (requestInfo != null && requestInfo.setLookupActives()) {
      // if lookupActives is true, means this request is not already in pending request queue. so, we add this to queue
      String name = requestInfo.getName();
      PendingTask pendingTask = new PendingTask(name, task, period, requestInfo);
      int requestID = addRequestToQueue(name, pendingTask);

      // The first time we received invalid error for a request, we will request actives without delay.
      // if we get invalid active error a second time or later, it means the set of active replicas is being changed
      // and the new active replica has not received this information. Therefore, we will wait for a timeout value
      // before sending requests again.
      long initialDelay = (requestInfo.getNumLookupActives() == 1) ? 0 : AppReconfigurableNodeOptions.queryTimeout / 10;
      //requestInfo.addEventCode(LNSEventCode.CONTACT_RC);
      if (requestID > 0) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Active request queued: " + requestID);
        }
        RequestActivesTask requestActivesTask = new RequestActivesTask(name, requestID, handler);
        handler.getExecutorService().scheduleAtFixedRate(requestActivesTask, initialDelay,
                AppReconfigurableNodeOptions.queryTimeout, TimeUnit.MILLISECONDS);
      }
    } else {
      GNS.getLogger().warning("This request already in queue so not added to pending requests again. " + requestInfo);
    }

  }

  /**
   * After the set of active name servers is successfully received, execute all its pending requests.
   *
   * @param name
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   * @param handler
   */
  public static void runPendingRequestsForName(String name, int requestID, ClientRequestHandlerInterface handler) {
    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Running pending tasks:\tname\t" + name + "\tCount " + runTasks.size());
      }
      for (PendingTask task : runTasks) {
        // update request info to reset lookup actives
        task.requestInfo.unsetLookupActives();
        //
        if (task.period > 0) {
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().fine(" Running Pending tasks. REPEAT!!");
          }
          handler.getExecutorService().scheduleAtFixedRate(task.timerTask, 0, task.period, TimeUnit.MILLISECONDS);
        } else {
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().fine(" Pending tasks. No repeat.");
          }
          handler.getExecutorService().schedule(task.timerTask, 0, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  /**
   * If the set of active name servers is null, send error messages for all requests.
   *
   * @param name
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   * @param handler
   */
  public static void sendErrorMsgForName(String name, int requestID, 
          //LNSEventCode eventCode, 
          ClientRequestHandlerInterface handler) {

    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      GNS.getLogger().fine("Running pending tasks. Sending error messages: Count " + runTasks.size());
      for (PendingTask task : runTasks) {
        // remove request from queue
        if (handler.removeRequestInfo(task.requestInfo.getCCPReqID()) != null) {
          task.requestInfo.setFinishTime(); // set finish time for request
//          if (eventCode != null) {
//            task.requestInfo.addEventCode(eventCode);
//          }
          //GNS.getStatLogger().fine(task.requestInfo.getLogString());
          handler.getIntercessor().handleIncomingPacket(task.requestInfo.getErrorMessage());
        }
      }
    }
  }

  /**
   * ***************Start of synchronized methods******************************************************
   */
  /**
   *
   * @param name name of the request
   * @param task task to be added to queue
   * @return requestID of the <code>RequestActivesTask</code> to be created,
   * and -1 if a <code>RequestActivesTask</code> already exists for this name and a new task should not be created.
   */
  private static synchronized int addRequestToQueue(String name, PendingTask task) {
    int requestID = -1;
    if (!allTasks.containsKey(name)) {
      allTasks.put(name, new ArrayList<PendingTask>());
      if (requestIDCounter == (Integer.MAX_VALUE / 2)) {
        requestIDCounter = 0;// reset counter
      }
      requestID = ++requestIDCounter;
      requestActivesOngoing.add(requestIDCounter);
    }
    allTasks.get(name).add(task);
    return requestID;
  }

  /**
   * Checks whether reply for this request ID is received
   * @param requestID
   * @return 
   */
  public static synchronized boolean isReplyReceived(int requestID) {
    return !requestActivesOngoing.contains(requestID);
  }

  private static synchronized ArrayList<PendingTask> removeAllRequestsFromQueue(String name, int requestID) {
    ArrayList<PendingTask> runTasks;
    runTasks = allTasks.remove(name);
    requestActivesOngoing.remove(requestID);
    return runTasks;
  }

  /**
   * ***************End of synchronized methods******************************************************
   */
}

/**
 * Represents a pending request of a name for which the set of active replicas is being obtained.
 *
 * @author abhigyan
 */
class PendingTask {

  public String name;

  /**
   * Log entry at local name server in case we don't get actives for this name.
   */
  public RequestInfo requestInfo;
  /**
   * Period > 0 for recurring tasks, = 0 for one time tasks.
   */
  public int period;

  public TimerTask timerTask;

  public PendingTask(String name, TimerTask timerTask, int period, RequestInfo requestInfo) {
    this.name = name;
    this.timerTask = timerTask;
    this.period = period;
    this.requestInfo = requestInfo;
  }
}
