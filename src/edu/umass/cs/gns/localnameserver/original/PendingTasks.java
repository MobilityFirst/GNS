package edu.umass.cs.gns.localnameserver.original;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PendingTasks
{

  static ConcurrentHashMap<String, ArrayList<PendingTask>> allTasks = new ConcurrentHashMap<String, ArrayList<PendingTask>>();

  static HashSet<String> requestActivesOngoing = new HashSet<String>();


  /**
   * Request current set of actives for name, and queue this request to be executed once we receives the curent actives.
   * @param name request actives for name
   * @param task TimerTask to be executed once actives are received. Request represented in form of TimerTask
   * @param period Frequency at which TimerTask is repeated
   * @param address Address of end user who sent the request
   * @param port Port of user
   * @param errorMsg In case no actives received, error msg to be send to user.
   * @param errorLog In case no actives received, error log entry to be written for this request.
   * @param initialDelay Delay for sending request to name servers to get actives.
   */
  public static void addToPendingRequests(String name, TimerTask task, int period, InetAddress address, int port,
                                          JSONObject errorMsg, String errorLog, long initialDelay) {

		synchronized (allTasks)
		{
//      RequestActivesPacket packet = new RequestActivesPacket(name,LocalNameServer.nodeID);
//      packet.setActiveNameServers(HashFunction.getPrimaryReplicasNoCache(name));
//      LocalNameServer.addCacheEntry(packet);
//      LocalNameServer.executorService.scheduleAtFixedRate(task,0,period, TimeUnit.MILLISECONDS);

      // if allTasks already contains a request for this name,
      // it means we have already sent a packet to get current actives for this name.
      if (!allTasks.containsKey(name)) {
        allTasks.put(name, new ArrayList<PendingTask>());
      }
      allTasks.get(name).add(new PendingTask(name, task, period, address, port,errorMsg, errorLog));

      if (requestActivesOngoing.contains(name) == false) {
        RequestActivesTask requestActivesTask = new RequestActivesTask(name);
        LocalNameServer.executorService.scheduleAtFixedRate(requestActivesTask, initialDelay,
                StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
        requestActivesOngoing.add(name);
      }

		}


	}

  public static void addToPendingRequests(String name) {
    LocalNameServer.executorService.scheduleAtFixedRate(new RequestActivesTask(name), 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
  }

  /**
   *
   * @param name
   */
  public static void runPendingRequestsForName(String name) {

    ArrayList<PendingTask> runTasks;

    synchronized (allTasks) {
      runTasks = allTasks.remove(name);
      if (runTasks == null) return;

//            if (allTasks.containsKey(name)) {
//                ArrayList<PendingTask> y = allTasks.get(name);
//                for (int i = y.size() - 1; i >= 0; i-- ) {
//                    PendingTask task = y.get(i);
//                    if (task.name.equals(name) //&& task.recordKey.equals(key)
//                            ) {
//                        y.remove(i);
//                        runTasks.add(task);
//                    }
//                }
//                if (y.size() == 0) allTasks.remove(name);
//
//            }
    }
		
		if (runTasks.size() > 0) {

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Running pending tasks:\tname\t" + name + "\tCount " +
              runTasks.size());
			for (PendingTask task: runTasks) {
				//
				if (task.period > 0) {
//					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. REPEAT!!" );
          LocalNameServer.executorService.scheduleAtFixedRate(task.timerTask,0,task.period, TimeUnit.MILLISECONDS);
//					LocalNameServer.timer.schedule(task.timerTask, 0, task.period);
				}
				else {
//					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. No repeat." );
          LocalNameServer.executorService.schedule(task.timerTask,0, TimeUnit.MILLISECONDS);
//					LocalNameServer.timer.schedule(task.timerTask, 0);
				}
			}
		}
		
	}

  public static boolean containsRequest(String name) throws  JSONException  {
    synchronized (allTasks) {
      return allTasks.containsKey(name);
    }
  }
    public  static  void sendErrorMsgForName(String name) throws JSONException {

      ArrayList<PendingTask> runTasks;

      synchronized (allTasks) {
        runTasks = allTasks.remove(name);
        if (runTasks == null) return;

//            if (allTasks.containsKey(name)) {
//                ArrayList<PendingTask> y = allTasks.get(name);
//                for (int i = y.size() - 1; i >= 0; i-- ) {
//                    PendingTask task = y.get(i);
//                    if (task.name.equals(name) //&& task.recordKey.equals(key)
//                            ) {
//                        y.remove(i);
//                        runTasks.add(task);
//                    }
//                }
//                if (y.size() == 0) allTasks.remove(name);
//
//            }
      }

      if (runTasks.size() > 0) {
        GNS.getLogger().info("Running pending tasks. Sending error messages: Count " + runTasks.size());
        for (PendingTask task: runTasks) {
          GNS.getStatLogger().fine(task.errorLog);
          if (task.address != null && task.port > 0) {
            LNSListener.udpTransport.sendPacket(task.errorMsg,task.address, task.port);
          } else if (StartLocalNameServer.runHttpServer) {
            Intercessor.checkForResult(task.errorMsg);
          }
        }
      }
    }
}

class PendingTask {
	public String name;
	//public NameRecordKey recordKey;
  public InetAddress address;
  public int port;
  public JSONObject errorMsg;
  public String errorLog;
	/**
	 * Period > 0 for recurring tasks, = 0 for one time tasks.
	 */
	public int period; 
	public TimerTask timerTask;
	
	public PendingTask(String name, TimerTask timerTask, int period, InetAddress address, int port, JSONObject errorMsg, String errorLog) {
		this.name = name;
		//this.recordKey = recordKey;
		this.timerTask = timerTask;
		this.period = period;
    this.address = address;
    this.port = port;
    this.errorMsg = errorMsg;
    this.errorLog = errorLog;
	}
	
	
}