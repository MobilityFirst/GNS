package edu.umass.cs.gns.activecode.protocol;

import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeResult {
	ValuesMap result;
	
	public void setResult(ValuesMap result) {
		this.result = result;
	}
	
	public ValuesMap getResult() {
		synchronized(this) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public void finished() {
        synchronized(this) {
        	this.notify();
        }
	}
}
