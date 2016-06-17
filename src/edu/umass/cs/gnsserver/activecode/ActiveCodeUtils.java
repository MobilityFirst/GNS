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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used to serialize and deserialize, send and receive message
 * between active worker and active client.
 * 
 * @author mbadov
 */
public class ActiveCodeUtils {
	
	/**
	 * Timeout error
	 */
	public static final String TIMEOUT_ERROR = "TimeOut";
	
	/**
	 * Serialize an object, ready to send
	 * 
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
	 * 
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
	 * 
	 * @param socket
	 * @param acm
	 * @param port
	 */
	public static void sendMessage(DatagramSocket socket, ActiveCodeMessage acm, int port) {
		long t = System.nanoTime();
		try {
			InetAddress addr = InetAddress.getByName("localhost");
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(outputStream);
			os.writeObject(acm);
			byte[] data = outputStream.toByteArray();
			DatagramPacket pkt = new DatagramPacket(data, data.length, addr, port);
			if (!socket.isClosed()) {
				socket.send(pkt);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
		DelayProfiler.updateDelayNano("activeSend", t);
	}

	/**
	 * receive a datagram through a datgram socket
	 * 
	 * @param socket
	 * @param buffer
	 * @return an ActiveCodeMessage
	 */
	public static ActiveCodeMessage receiveMessage(DatagramSocket socket, byte[] buffer) {
		long t = System.nanoTime();
		ActiveCodeMessage acm = null;
		Arrays.fill(buffer, (byte) 0);
		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
		try {
			socket.receive(pkt);
			acm = (ActiveCodeMessage) (new ObjectInputStream(new ByteArrayInputStream(pkt.getData()))).readObject();
		} catch (IOException e) {
			// Swallow this exception
			// e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			
		}
		DelayProfiler.updateDelayNano("activeReceive", t);
		return acm;
	}
	
	/**
	 * @param socket
	 * @param buffer
	 * @return datagram packet received from the socket
	 */
	public static DatagramPacket receivePacket(DatagramSocket socket, byte[] buffer) {
		Arrays.fill(buffer, (byte) 0);
		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
		
		try {
			socket.receive(pkt);
		} catch (IOException e) {
			// Swallow this exception
			// e.printStackTrace();
			return null;
		} finally{
			
		}
		
		return pkt;
	}
	
	
	
}
