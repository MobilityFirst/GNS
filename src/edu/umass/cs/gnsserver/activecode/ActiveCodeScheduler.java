package edu.umass.cs.gnsserver.activecode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
	//private HashMap<String, ArrayList<FutureTask<ValuesMap>>> queue = new HashMap<String, ArrayList<FutureTask<ValuesMap>>>();
	private ArrayList<String> guidList = new ArrayList<String>();
	private HashMap<String, LinkedList<FutureTask<ValuesMap>>> fairQueue = new HashMap<String, LinkedList<FutureTask<ValuesMap>>>();
	private int ptr = 0;
	
	private Lock lock = new ReentrantLock();
	private Lock queueLock = new ReentrantLock();
	
	protected ActiveCodeScheduler(ThreadPoolExecutor executorPool){
		this.executorPool = executorPool;
	}
	
	public void run(){		
		while(true){
			while(executorPool.getQueue().remainingCapacity()<=0 || guidList.isEmpty()){
				synchronized (lock){
					try{
						lock.wait();
					}catch(InterruptedException e){
						e.printStackTrace();
					}
				}
			}
			FutureTask<ValuesMap> futureTask = getNextTask();
			if (futureTask != null){
				executorPool.execute(futureTask);
			}
		}
	}
	
	protected void release(){
		synchronized(lock){
			lock.notify();
		}
	}
	
	protected FutureTask<ValuesMap> getNextTask(){
		FutureTask<ValuesMap> futureTask = null;
		String guid = null;
		synchronized(queueLock){
			if (guidList.size() > ptr){
				if (ptr > 0){
					guid = guidList.get(ptr);
				}else{ 
					if(guidList.isEmpty()){
						guid = null;
						return null;
					}else{
						assert(ptr == 0);
						guid = guidList.get(ptr);
					}
				}
			}else{
				ptr = 0;
				if(guidList.isEmpty()){
					guid = null;
					return null;
				}else{
					assert(ptr == 0);
					guid = guidList.get(ptr);
				}
			}
			
			ptr++;
			
			futureTask = fairQueue.get(guid).pop();
			if(fairQueue.get(guid).isEmpty()){
				fairQueue.remove(guid);
				guidList.remove(guid);
			}
		}		
		
		return futureTask;
	}
	
	protected void submit(FutureTask<ValuesMap> futureTask, String guid){
		synchronized(queueLock){
			if(fairQueue.containsKey(guid)){
				fairQueue.get(guid).add(futureTask);
				
			}else{
				LinkedList<FutureTask<ValuesMap>> taskList = new LinkedList<FutureTask<ValuesMap>>();
				taskList.add(futureTask);
				fairQueue.put(guid, taskList);
				guidList.add(guid);
			}	
			release();
		}
	}
}
