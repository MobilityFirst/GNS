/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Base64;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;

public class ActiveCodeUtils {
	public static byte[] serializeObject(Object o) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(o);
			oos.flush();
			oos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return baos.toByteArray();
	}
	
	public static Object deserializeObject(byte[] data) {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
	    Object o = null;
		try {
			ObjectInputStream oin = new ObjectInputStream(bais);
			o = oin.readObject();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return o;
	}
	
	public static void sendMessage(PrintWriter out, ActiveCodeMessage acm) {
		byte[] data = serializeObject(acm);
		String data64 = Base64.getEncoder().encodeToString(data);
		out.println(data64);
	}
	
	public static ActiveCodeMessage getMessage(BufferedReader in) {
		String res64 = null;
		
		try {
			res64 = in.readLine();
		} catch (IOException e) {
			// We timed out, but that's OK
		}
		
		if(res64 == null) {
			// We crashed, but still mark the request as finished
			ActiveCodeMessage acm = new ActiveCodeMessage();
			acm.setFinished(true);
			acm.setCrashed(true);
			return acm;
		}
		
		byte[] res = Base64.getDecoder().decode(res64);
	    return (ActiveCodeMessage) deserializeObject(res);
	}
}
