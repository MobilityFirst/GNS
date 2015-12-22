package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class periodically checks the status of each running task,
 * and terminates the task whose execution time exceeds the limitation.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeGuardian implements Runnable {
	private static ConcurrentHashMap<Future<ValuesMap>, Long> tasks = new ConcurrentHashMap<Future<ValuesMap>, Long>();
	
	public void run(){
		while(true){
			synchronized(tasks){
				long now = System.currentTimeMillis();
				
				for(Future<ValuesMap> task:tasks.keySet()){
					if (now - tasks.get(task) > 500){
						task.cancel(true);
						tasks.remove(task);
					}
				}		
			}
			try{
				Thread.sleep(5);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}
	
	protected void register(Future<ValuesMap> task){
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
}
