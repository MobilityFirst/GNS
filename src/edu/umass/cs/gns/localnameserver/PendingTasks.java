package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PendingTasks
{
	

  static ConcurrentHashMap<String, ArrayList<PendingTask>> allTasks = new ConcurrentHashMap<String, ArrayList<PendingTask>>();

	public static void addToPendingRequests(String name, TimerTask task, int period, InetAddress address, int port,
                                          JSONObject errorMsg, String errorLog, long initialDelay) {

		synchronized (allTasks)
		{
        if (!allTasks.containsKey(name)) {
          SendActivesRequestTask requestActivesTask = new SendActivesRequestTask(name);
          LocalNameServer.executorService.scheduleAtFixedRate(requestActivesTask, initialDelay,
                  StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
            allTasks.put(name, new ArrayList<PendingTask>());
        }
        allTasks.get(name).add(new PendingTask(name, task, period, address, port,errorMsg, errorLog));
		}


	}

  public static void addToPendingRequests(String name) {
    LocalNameServer.executorService.scheduleAtFixedRate(new SendActivesRequestTask(name), 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);

  }
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

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Running pending tasks:\tname\t" + name + "\tCount " + runTasks.size());
			for (PendingTask task: runTasks) {
				//
				if (task.period > 0) {
//					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. REPEAT!!" );
          LocalNameServer.executorService.scheduleAtFixedRate(task.timerTask,0,task.period, TimeUnit.MILLISECONDS);
//					LocalNameServer.timer.schedule(task.timerTask, 0, task.period);
				}
				else {
					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. No repeat." );
          LocalNameServer.executorService.schedule(task.timerTask,0, TimeUnit.MILLISECONDS);
//					LocalNameServer.timer.schedule(task.timerTask, 0);
				}
			}
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
        GNS.getLogger().severe("Running pending tasks. Sending error messages: Count " + runTasks.size());
        for (PendingTask task: runTasks) {
          GNS.getStatLogger().fine(task.errorLog);
          if (task.address != null && task.port > 0) {
            LNSListener.udpTransport.sendPacket(task.errorMsg,task.address, task.port);
          } else if (StartLocalNameServer.runHttpServer) {
            Intercessor.getInstance().checkForResult(task.errorMsg);
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