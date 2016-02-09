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
	
	public ActiveCodeGuardian(ClientPool clientPool){
		 this.clientPool = clientPool;
	}
	
	public void run(){
		while(true){
			synchronized(tasks){
				long now = System.currentTimeMillis();
				
				for(ActiveCodeTask task:tasks.keySet()){
					if (now - tasks.get(task) > 1000){
						long start = System.currentTimeMillis();
						synchronized(clientPool){
							// shutdown the previous worker process 
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
						//runnableMap.get(task).cancel(true);
						deregisterFutureTask(task);
						cnt++;
						System.out.println("There are "+(cnt)+" tasks being cancelled.");
						DelayProfiler.updateDelay("ActiveCodeRestart", start);
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
	
}
