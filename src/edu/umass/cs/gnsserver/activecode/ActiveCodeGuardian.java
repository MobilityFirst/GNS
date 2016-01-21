package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian implements Runnable {
	private static ConcurrentHashMap<FutureTask<ValuesMap>, Long> tasks = new ConcurrentHashMap<FutureTask<ValuesMap>, Long>();
	private ConcurrentHashMap<FutureTask<ValuesMap>, Thread> threadMap = new ConcurrentHashMap<FutureTask<ValuesMap>, Thread>();
	private ClientPool clientPool;
	
	public ActiveCodeGuardian(ClientPool clientPool){
		 this.clientPool = clientPool;
	}
	
	public void run(){
		while(true){
			synchronized(tasks){
				long now = System.currentTimeMillis();
				
				for(FutureTask<ValuesMap> task:tasks.keySet()){
					
					if (now - tasks.get(task) > 10000){
						ActiveCodeClient client = this.clientPool.getClient(getThread(task).getId());
						client.restartServer();
						removeThread(task);
						task.cancel(true);
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
	
	protected void register(FutureTask<ValuesMap> task){
		//System.out.println("Submitted task is "+task);
		synchronized(tasks){
			tasks.put(task, System.currentTimeMillis());
		}
	}
	
	protected void remove(Future<ValuesMap> task){
		//System.out.println("Removed task is "+task);
		synchronized(tasks){
			tasks.remove(task);
		}
	}
	
	protected void registerThread(FutureTask<ValuesMap> r, Thread t){
		threadMap.put(r, t);
		register(r);
	}
	
	protected void removeThread(FutureTask<ValuesMap> r){
		remove(r);
		threadMap.remove(r);
	}
	
	protected Thread getThread(FutureTask<ValuesMap> r){
		return threadMap.get(r);
	}
}
