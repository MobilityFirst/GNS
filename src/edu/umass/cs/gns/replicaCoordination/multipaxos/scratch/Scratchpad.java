package edu.umass.cs.gns.replicaCoordination.multipaxos.scratch;

import java.io.File;
import java.security.MessageDigest;


import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class Scratchpad {

	public static int digest(MessageDigest md, String s) {
		md.update(s.getBytes());
		byte[] digest = md.digest();
		int dig=0;
		for(int i=0; i<digest.length; i++) {
			dig = (int)digest[i];
		}
		return dig;
	}

	public static void main(String[] args) {
		int[] array = {1, 3, 5};
		String str = Util.arrayToSet(array).toString();
		int[] converted = Util.stringToArray(str);
		assert(Util.arrayToSet(converted).toString().equals(str));
		System.out.println(Util.arrayToString(array));
		try {
			MessageDigest md = MessageDigest.getInstance("SHA");
			int million = 1000000;
			int size = 1*million;
			long t1=System.currentTimeMillis();
			String s = "Random string to be digested";
			for(int i=0; i<size; i++) {
				digest(md, s);
			}
			System.out.println("MD5 throughput = " + size*1.0/(System.currentTimeMillis()-t1));
		} catch(Exception e) {e.printStackTrace();}
	}
}
