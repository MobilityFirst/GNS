package edu.umass.cs.gns.gigapaxos.scratch;

import java.net.InetAddress;
import java.net.UnknownHostException;


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

	public static void main(String[] args) {
		System.out.println(Integer.valueOf("3"));
		try {
			System.out.println(InetAddress.getByName("128.1.1.1"));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
