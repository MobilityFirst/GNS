package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian implements Runnable {
	private ClientPool clientPool;
	private static int cnt = 0;
	private ConcurrentHashMap<ActiveCodeTask, Thread> threadMap = new ConcurrentHashMap<ActiveCodeTask, Thread>();
	private static ConcurrentHashMap<ActiveCodeTask, Long> tasks = new ConcurrentHashMap<ActiveCodeTask, Long>();
	private ConcurrentHashMap<ActiveCodeTask, FutureTask<ValuesMap>> runnableMap = new ConcurrentHashMap<ActiveCodeTask, FutureTask<ValuesMap>>();
	private ConcurrentHashMap<ActiveCodeTask, ActiveCodeTask> relationMap = new ConcurrentHashMap<ActiveCodeTask, ActiveCodeTask>();
	
	public ActiveCodeGuardian(ClientPool clientPool){
		 this.clientPool = clientPool;
	}
	
	public void run(){
		while(true){
			synchronized(tasks){
				long now = System.currentTimeMillis();
				
				for(ActiveCodeTask task:tasks.keySet()){
					if (now - tasks.get(task) > 1000){
						cancelTask(task);
					}
				}		
			}
			try{
				Thread.sleep(100);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}
	
	protected void cancelTask(ActiveCodeTask task){
		
		long start = System.currentTimeMillis();
		synchronized(clientPool){
			// shutdown the previous worker process 
			assert(getThread(task) != null);
			ActiveCodeClient client = clientPool.getClient(getThread(task).getId());
			client.forceShutdownServer();
			
			// generate a spare worker in another thread
			clientPool.generateNewWorker();
														
			// get the spare worker and set the client port to the new worker
			int port = clientPool.getSpareWorkerPort();
			Process proc = clientPool.getSpareWorker(port);
			client.setNewWorker(port, proc);
		}
		// deregister the task and cancel it
		removeThread(task);
		//System.out.println("The task to be cancelled is "+runnableMap.get(task));
		runnableMap.get(task).cancel(true);
		
		deregisterFutureTask(task);
		DelayProfiler.updateDelay("ActiveCodeRestart", start);
	}
	
	protected void registerFutureTask(ActiveCodeTask act, FutureTask<ValuesMap> task){
		runnableMap.put(act, task);
	}
	
	protected void deregisterFutureTask(ActiveCodeTask act){
		runnableMap.remove(act);
	}
	
	protected void register(ActiveCodeTask task){
		synchronized(tasks){
			tasks.put(task, System.currentTimeMillis());
		}
	}
	
	protected void remove(ActiveCodeTask task){
		synchronized(tasks){
			tasks.remove(task);
		}
	}
	
	protected void registerThread(ActiveCodeTask r, Thread t){
		threadMap.put(r, t);
		register(r);
	}
	
	protected void removeThread(ActiveCodeTask r){
		remove(r);
		threadMap.remove(r);
	}
	
	protected Thread getThread(ActiveCodeTask r){
		return threadMap.get(r);
	}
	
	protected void registerParent(ActiveCodeTask parent, ActiveCodeTask child){
		relationMap.put(parent, child);
	}
	
	protected void deregisterParent(ActiveCodeTask parent){
		relationMap.remove(parent);
	}
}
