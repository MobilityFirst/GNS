package edu.umass.cs.gns.reconfiguration.deprecated.scratch;

import edu.umass.cs.gns.reconfiguration.reconfigurationutils.StringLocker;

public class Scratchpad {
	int counter = 0;
	
	void incr(String lockString) {
		synchronized(lockString) {
			System.out.println(counter++);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			counter--;
		}
	}

	class Incrementer implements Runnable {

		final String str;
		Incrementer(String str) {
			this.str = str;
		}
		@Override
		public void run() {
			incr(str);
		}
	}
	
	public static void main(String[] args) {
		Scratchpad pad = new Scratchpad();
		
		StringLocker locker = new StringLocker();
		for(int i=0; i<100; i++) {
			(new Thread(pad.new Incrementer(locker.get(new String("random".getBytes()))))).start();
		}
	}
}
