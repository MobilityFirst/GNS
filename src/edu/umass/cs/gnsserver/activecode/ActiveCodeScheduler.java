package edu.umass.cs.gnsserver.activecode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used to do a fair queue across all the GUIDs
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeScheduler implements Runnable{
	private ThreadPoolExecutor executorPool;
	private HashMap<String, ArrayList<FutureTask<ValuesMap>>> queue = new HashMap<String, ArrayList<FutureTask<ValuesMap>>>();
	private ArrayList<String> guidList = new ArrayList<String>();
	private int ptr = 0;
	private Lock lock = new ReentrantLock();
	
	protected ActiveCodeScheduler(ThreadPoolExecutor executorPool){
		this.executorPool = executorPool;
	}
	
	public void run(){
		//System.out.println("################### Start running ####################");
		while(true){
			//System.out.println("################### guidlist is ####################"+guidList);
			if(executorPool.getQueue().remainingCapacity()>0 && !guidList.isEmpty()){
				FutureTask<ValuesMap> futureTask = getNextTask();
				executorPool.execute(futureTask);
				
				//System.out.println("Task is submitted to the pool:"+futureTask);				
			}else{
				synchronized (lock){
					try{
						lock.wait();
					}catch(InterruptedException e){
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	protected void release(){
		synchronized(lock){
			lock.notify();
		}
	}
	
	protected synchronized FutureTask<ValuesMap> getNextTask(){
		FutureTask<ValuesMap> futureTask = null;
		String guid;
		if (guidList.size() >= ptr){
			guid = guidList.get(ptr);
		}else{
			ptr = 0;
			guid = guidList.get(ptr);
		}
		
		futureTask = queue.get(guid).remove(0);
		if(queue.get(guid).isEmpty()){
			queue.remove(guid);
			guidList.remove(guid);
			return futureTask;
		}
		
		ptr++;
		return futureTask;
	}
	
	protected synchronized void submit(FutureTask<ValuesMap> futureTask, String guid){
		if(queue.containsKey(guid)){
			queue.get(guid).add(futureTask);
			release();
		}else{
			ArrayList<FutureTask<ValuesMap>> taskList = new ArrayList<FutureTask<ValuesMap>>();
			taskList.add(futureTask);
			queue.put(guid, taskList);
			guidList.add(guid);
			
			release();
						
			//System.out.println("Task submitted:"+futureTask+"for"+guid);
		}		
	}
}
