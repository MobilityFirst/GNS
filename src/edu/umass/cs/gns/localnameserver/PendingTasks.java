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
   * @param name request actives for name
   * @param task TimerTask to be executed once actives are received. Request represented in form of TimerTask
   * @param period Frequency at which TimerTask is repeated
   * @param errorMsg In case no actives received, error msg to be send to user.
   * @param errorLog In case no actives received, error log entry to be written for this request.
   * @param firstAttempt Is this the first time request is being queued.
   */
  public static void addToPendingRequests(String name, TimerTask task, int period,
          JSONObject errorMsg, String errorLog, boolean firstAttempt) {
    PendingTask pendingTask = new PendingTask(name, task, period, errorMsg, errorLog);
    int requestID = addRequestToQueue(name, pendingTask);

    // the first time we received invalid error for a request, we will request actives without delay.
    // if we get invalid active error a second time or later, it means the set of active replicas is being changed
    // therefore, we will wait for a timeout value before sending requests again.
    long initialDelay = (firstAttempt) ? 0 : StartLocalNameServer.queryTimeout;
    if (requestID > 0) {
      GNS.getLogger().fine("Active request queued: " + requestID);
      RequestActivesTask requestActivesTask = new RequestActivesTask(name, requestID);
      LocalNameServer.getExecutorService().scheduleAtFixedRate(requestActivesTask, initialDelay,
              StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    }

  }

  /**
   * Received reply from primary with current actives, update the cache. If non-null set of actives received,
   * execute all pending request. Otherwise, send error messages for all requests.
   *
   * @param json
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
      sendErrorMsgForName(requestActivesPacket.getName(), requestActivesPacket.getLnsRequestID());
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
   * @param name
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   */
  public static void runPendingRequestsForName(String name, int requestID) {

    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Running pending tasks:\tname\t" + name + "\tCount " + runTasks.size());
      }
      for (PendingTask task : runTasks) {
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
   * @param name
   * @param requestID request ID of the <code>RequestActivesPacket</code>
   */
  public static void sendErrorMsgForName(String name, int requestID) {

    ArrayList<PendingTask> runTasks = removeAllRequestsFromQueue(name, requestID);

    if (runTasks != null && runTasks.size() > 0) {
      GNS.getLogger().fine("Running pending tasks. Sending error messages: Count " + runTasks.size());
      for (PendingTask task : runTasks) {
        GNS.getStatLogger().fine(task.errorLog);
        Intercessor.handleIncomingPackets(task.errorMsg);
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
      if (requestIDCounter == Integer.MAX_VALUE / 2) {
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
 */
class PendingTask {

  public String name;
  /**
   * Error message that will be sent to client in case we don't get actives for this name.
   */
  public JSONObject errorMsg;

  /**
   * Log entry at local name server in case we don't get actives for this name.
   */
  public String errorLog;
  /**
   * Period > 0 for recurring tasks, = 0 for one time tasks.
   */
  public int period;

  public TimerTask timerTask;

  public PendingTask(String name, TimerTask timerTask, int period, JSONObject errorMsg, String errorLog) {
    this.name = name;
    this.timerTask = timerTask;
    this.period = period;
    this.errorMsg = errorMsg;
    this.errorLog = errorLog;
  }
}
