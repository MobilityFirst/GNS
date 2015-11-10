/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
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
