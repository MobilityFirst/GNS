package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
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
 * @see edu.umass.cs.gns.localnameserver.RequestActivesTask
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
   * @param requestInfo Information for this request stored at LNS.
   * @param task TimerTask to be executed once actives are received. Request represented in form of TimerTask
   * @param period Frequency at which TimerTask is repeated
   */
  public static void addToPendingRequests(RequestInfo requestInfo, TimerTask task, int period) {
    if (requestInfo != null && requestInfo.setLookupActives()) {
      // if lookupActives is true, means this request is not already in pending request queue. so, we add this to queue
      String name = requestInfo.getName();
      PendingTask pendingTask = new PendingTask(name, task, period, requestInfo);
      int requestID = addRequestToQueue(name, pendingTask);

      // The first time we received invalid error for a request, we will request actives without delay.
      // if we get invalid active error a second time or later, it means the set of active replicas is being changed
      // and the new active replica has not received this information. Therefore, we will wait for a timeout value before
      // sending requests again.
      long initialDelay = (requestInfo.getNumLookupActives() == 1) ? 0 : StartLocalNameServer.queryTimeout;
      requestInfo.addEventCode(LNSEventCode.CONTACT_RC);
      if (requestID > 0) {
        GNS.getLogger().fine("Active request queued: " + requestID);
        RequestActivesTask requestActivesTask = new RequestActivesTask(name, requestID);
        LocalNameServer.getExecutorService().scheduleAtFixedRate(requestActivesTask, initialDelay,
                StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
      }
    } else {
      GNS.getLogger().info("This request already in queue so not added to pending requests again. " + requestInfo);
    }

  }

  /**
   * Received reply from primary with current actives, update the cache. If non-null set of actives received,
   * execute all pending request. Otherwise, send error messages for all requests.
   *
   * @throws org.json.JSONException
   */
  public static void handleActivesRequestReply(JSONObject json) throws JSONException {
    RequestActivesPacket requestActivesPacket = new RequestActivesPacket(json);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Recvd request actives packet: " + requestActivesPacket
              + " name\t" + requestActivesPacket.getName());
    }
    if (requestActivesPacket.getActiveNameServers() == null
            || requestActivesPacket.getActiveNameServers().size() == 0) {
      GNS.getLogger().fine("Null set of actives received for name " + requestActivesPacket.getName()
              + " sending error");
      sendErrorMsgForName(requestActivesPacket.getName(), requestActivesPacket.getLnsRequestID(), LNSEventCode.RC_NO_RECORD_ERROR);
      return;
    }

    if (LocalNameServer.containsCacheEntry(requestActivesPacket.getName())) {
      LocalNameServer.updateCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Updating cache Name:"
                + requestActivesPacket.getName() + " Actives: " + requestActivesPacket.getActiveNameServers());
      }
    } else {
      LocalNameServer.addCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Adding to cache Name:"
                + requestActivesPacket.getName() + " Actives: " + requestActivesPacket.getActiveNameServers());
      }
    }

    runPendingRequestsForName(requestActivesPacket.getName(), requestActivesPacket.getLnsRequestID());

  }

  /**
   * After the set of active name servers is successfully received, execute all its pending requests.
   *
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   */
  public static void runPendingRequestsForName(String name, int requestID) {
    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Running pending tasks:\tname\t" + name + "\tCount " + runTasks.size());
      }
      for (PendingTask task : runTasks) {
        // update request info to reset lookup actives
        task.requestInfo.unsetLookupActives();
        //
        if (task.period > 0) {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine(" Running Pending tasks. REPEAT!!");
          }
          LocalNameServer.getExecutorService().scheduleAtFixedRate(task.timerTask, 0, task.period, TimeUnit.MILLISECONDS);
        } else {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine(" Pending tasks. No repeat.");
          }
          LocalNameServer.getExecutorService().schedule(task.timerTask, 0, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  /**
   * If the set of active name servers is null, send error messages for all requests.
   *
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   */
  public static void sendErrorMsgForName(String name, int requestID, LNSEventCode eventCode) {

    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      GNS.getLogger().fine("Running pending tasks. Sending error messages: Count " + runTasks.size());
      for (PendingTask task : runTasks) {
        // remove request from queue
        if (LocalNameServer.removeRequestInfo(task.requestInfo.getLnsReqID()) != null) {
          task.requestInfo.setFinishTime(); // set finish time for request
          if (eventCode != null) task.requestInfo.addEventCode(eventCode);
          GNS.getStatLogger().fine(task.requestInfo.getLogString());
          Intercessor.handleIncomingPackets(task.requestInfo.getErrorMessage());
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
