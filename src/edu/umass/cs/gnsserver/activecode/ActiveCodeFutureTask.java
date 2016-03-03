package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class holds the reference to the ActiveCodeTask
 * @author gaozy
 */
public class ActiveCodeFutureTask extends FutureTask<ValuesMap>{
	
	private ActiveCodeTask callable;
	private boolean running = false;
	
	/**
	 * @param callable
	 */
	public ActiveCodeFutureTask(Callable<ValuesMap> callable) {
		super(callable);
		this.callable = (ActiveCodeTask) callable;
	}
	
	public String toString() {
		return this.callable.toString();
	}
	/**
	 * @return the callable wrapped by the future task, i.e., the ActiveCodeTask
	 */
	protected ActiveCodeTask getWrappedTask(){
		return callable;
	}
	
    protected boolean isRunning(){
    	return running;
    }
    
    protected boolean setRunning(boolean running){
    	boolean prev = this.running;
    	this.running = running;
    	return prev;
    }
}