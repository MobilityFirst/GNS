package edu.umass.cs.gnsserver.activecode.prototype.channels;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Message;


public class ActiveDatagramChannel implements Channel {
	
	private DatagramSocket socket;
	private int serverPort;
	private static InetAddress localhost;

	public final static int maxPacketSize = 1024;
	

	public ActiveDatagramChannel(int clientPort, int serverPort){
		this.serverPort = serverPort;
		try {
			socket = new DatagramSocket(clientPort);
		} catch (SocketException e) {
			e.printStackTrace();
		}
		try {
			localhost = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void sendMessage(Message msg) throws IOException {
		byte[] buf = msg.toBytes();

		if(buf.length > maxPacketSize)
			throw new IOException("Packet size "+buf.length+" is too large for this channel.");
		
		socket.send(new DatagramPacket(buf, buf.length, localhost, serverPort));
	}

	@Override
	public Message receiveMessage() throws IOException {
		
		byte[] buf = new byte[maxPacketSize];
		DatagramPacket packet = new DatagramPacket(buf, maxPacketSize);
		socket.receive(packet);
		ActiveMessage am = null;
		try {
			am = new ActiveMessage(packet.getData());
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return am;
	}

	@Override
	public void close() {
		socket.close();		
	}

}
