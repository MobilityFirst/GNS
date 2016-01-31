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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Base64;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used to serialize and deserialize, send 
 * and receive message between active worker and active client.
 * 
 * @author mbadov
 */
public class ActiveCodeUtils {
	/**
	 * Serialize an object, ready to send
	 * @param o
	 * @return a byte array ready to send
	 */
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
	
	/**
	 * Deserialize a byte array, ready to cast
	 * @param data
	 * @return a object, ready to cast
	 */
	public static Object deserializeObject(byte[] data) {
		long t = System.nanoTime();
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
	    Object o = null;
		try {
			ObjectInputStream oin = new ObjectInputStream(bais);
			o = oin.readObject();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DelayProfiler.updateDelayNano("activeDeserialize", t);
	    return o;
	}
	
	/**
	 * Send the serialized message
	 * @param out
	 * @param acm
	 */
	/*
	public static void sendMessage(PrintWriter out, ActiveCodeMessage acm) {
		long t1 = System.nanoTime();
		byte[] data = serializeObject(acm);
		DelayProfiler.updateDelayNano("activeSerialize", t1);
		
		long t2 = System.nanoTime();
		String data64 = Base64.getEncoder().encodeToString(data);
		DelayProfiler.updateDelayNano("activeEncode", t2);
		
		long t3 = System.nanoTime();
		out.println(data64);
		DelayProfiler.updateDelayNano("activeSendOutMsg", t3);
	}
	*/
	
	/**
	 * Receive the serialize the message
	 * @param in
	 * @return an {@link ActiveCodeMessage} ready to cast
	 */
	/*
	public static ActiveCodeMessage getMessage(BufferedReader in) {
		
		long t1 = System.nanoTime();
		String res64 = null;
		
		try {
			res64 = in.readLine();
		} catch (IOException e) {
			// We timed out, but that's OK
			e.printStackTrace();
		}
		
		if(res64 == null) {
			// We crashed, but still mark the request as finished
			ActiveCodeMessage acm = new ActiveCodeMessage();
			acm.setFinished(true);
			acm.setCrashed(true);
			return acm;
		}
		DelayProfiler.updateDelayNano("activeReadInMessage", t1);
		
		long t2 = System.nanoTime();
		byte[] res = Base64.getDecoder().decode(res64);
		DelayProfiler.updateDelayNano("activeDecode", t2);
		
		return (ActiveCodeMessage) deserializeObject(res);
	}
	*/
	
	public static void sendMessage(DatagramSocket socket, ActiveCodeMessage acm, int port){
		long t = System.nanoTime();
		try{
			InetAddress addr = InetAddress.getByName("localhost");
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(outputStream);
			os.writeObject(acm);
			byte[] data = outputStream.toByteArray();
			DatagramPacket pkt = new DatagramPacket(data, data.length, addr, port);
			socket.send(pkt);
			
		}catch(IOException e){
			e.printStackTrace();
		}
		DelayProfiler.updateDelayNano("activeSend", t);
	}
	
	/**
	 * receive a datagram through a datgram socket
	 * @param socket
	 * @return an ActiveCodeMessage
	 */
	public static ActiveCodeMessage receiveMessage(DatagramSocket socket, byte[] buffer){
		long t = System.nanoTime();
		ActiveCodeMessage acm = null;
		//byte[] buffer = new byte[8096*10];
		Arrays.fill(buffer, (byte) 0);
		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
		try{
			socket.receive(pkt);
			ByteArrayInputStream in = new ByteArrayInputStream(pkt.getData());
			ObjectInputStream is = new ObjectInputStream(in);			 
			acm = (ActiveCodeMessage) is.readObject();			
		}catch(IOException e){
			//e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		DelayProfiler.updateDelayNano("activeReceive", t);
		return acm;
	}
}
