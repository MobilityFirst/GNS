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

public class PendingTasks
{
	

    static ConcurrentHashMap<String, ArrayList<PendingTask>> allTasks =
            new ConcurrentHashMap<String, ArrayList<PendingTask>>();

	public static void addToPendingRequests(String name, //NameRecordKey key, 
                TimerTask t, int period,
                                            InetAddress address, int port, JSONObject errorMsg) {
		synchronized (allTasks)
		{
            if (!allTasks.containsKey(name)) {
                allTasks.put(name, new ArrayList<PendingTask>());
            }
            allTasks.get(name).add(new PendingTask(name, //key,
                    t, period, address, port,errorMsg));



		}
		
	}
	
	public static void runPendingRequestsForName(String name//, NameRecordKey key
                ) {
		
		ArrayList<PendingTask> runTasks = new ArrayList<PendingTask>();
		
		synchronized (allTasks) {
            if (allTasks.containsKey(name)) {
                ArrayList<PendingTask> y = allTasks.get(name);
                for (int i = y.size() - 1; i >= 0; i-- ) {
                    PendingTask task = y.get(i);
                    if (task.name.equals(name) //&& task.recordKey.equals(key)
                            ) {
                        y.remove(i);
                        runTasks.add(task);
                    }
                }
                if (y.size() == 0) allTasks.remove(name);
            }
		}
		
		if (runTasks.size() > 0) {

            if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Running pending tasks: Count " + runTasks.size());
			for (PendingTask task: runTasks) {
				// 
				if (task.period > 0) {
					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. REPEAT!!" );
					LocalNameServer.timer.schedule(task.timerTask, 0, task.period);
				}
				else {
					if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Pending tasks. No repeat." );
					LocalNameServer.timer.schedule(task.timerTask, 0);
				}
			}
		}
		
	}

    public  static  void sendErrorMsgForName(String name//, NameRecordKey key
            ) throws JSONException {

        ArrayList<PendingTask> runTasks = new ArrayList<PendingTask>();

        synchronized (allTasks) {
            if (allTasks.containsKey(name)) {
                ArrayList<PendingTask> y = allTasks.get(name);
                for (int i = y.size() - 1; i >= 0; i-- ) {
                    PendingTask task = y.get(i);
                    if (task.name.equals(name) //&& task.recordKey.equals(key)
                            ) {
                        y.remove(i);
                        runTasks.add(task);
                    }
                }
                if (y.size() == 0) allTasks.remove(name);
            }
        }

        if (runTasks.size() > 0) {

            if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Running pending tasks. Sending error messages: Count " + runTasks.size());
            for (PendingTask task: runTasks) {
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
	/**
	 * Period > 0 for recurring tasks, = 0 for one time tasks.
	 */
	public int period; 
	public TimerTask timerTask;
	
	public PendingTask(String name, //NameRecordKey recordKey, 
                TimerTask timerTask, int period,
                       InetAddress address, int port, JSONObject errorMsg) {
		this.name = name;
		//this.recordKey = recordKey;
		this.timerTask = timerTask;
		this.period = period;
        this.address = address;
        this.port = port;
        this.errorMsg = errorMsg;
	}
	
	
}