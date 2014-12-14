package edu.umass.cs.gns.gigapaxos.scratch;



/**
@author V. Arun
 */
public class Scratchpad {
	public static int multiply(int x, int y) {
		return x * y;
	}
	
	public void testMultiply() {
		assert(Scratchpad.multiply(3, 4)==3*4);
	}
	public static boolean doSomething() {
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	public static void main(String[] args) {
		assert(doSomething());
	}
}
