package edu.umass.cs.gns.mSocket;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ControlMessage  {
	public static final short KEEP_ALIVE=0;
	public static final short ACK_ONLY=1;
	public static final short REBIND_ADDRESS_PORT=2;
	//aditya
	public static final short CLOSING=3;
	
	public static final String[] msgStr = {"KEEP_ALIVE", "ACK_ONLY", "REBIND_ADDRESS_PORT","CLOSING"};

	public static final int INET_ADDR_SIZE = 4; 
	public static final int SIZE = (Integer.SIZE*5 + Long.SIZE)/8 + INET_ADDR_SIZE;
	
	final int sendseq;
	private final int ackseq;
	final int type;
	private final long flowID;
	
	private final int port;
	private final InetAddress iaddr;
	private final int UDPPort;
	
	ControlMessage(int s, int a, int t, long f) {
		sendseq=s;
		ackseq=a;
		type=t;
		flowID=f;
		port=0;
		iaddr=null;
		UDPPort=0;
	}
	ControlMessage(int s, int a, int t, long f, int p, int udpport,InetAddress ia) {
		sendseq=s;
		ackseq=a;
		type=t;
		flowID=f;
		port=p;
		iaddr=ia;
		UDPPort=udpport;
	}

	public byte[] getBytes() throws UnknownHostException {
		ByteBuffer buf = ByteBuffer.allocate(ControlMessage.SIZE);
		buf.putInt(sendseq);
		buf.putInt(ackseq);
		buf.putInt(type);
		buf.putLong(flowID);
		buf.putInt((short)port);
		//aditya
		buf.putInt(UDPPort);
		buf.put(iaddr!=null?iaddr.getAddress():(new byte[ControlMessage.INET_ADDR_SIZE]));
		buf.flip();
		return buf.array();
	}
	public static ControlMessage getControlMessage(byte[] b) throws UnknownHostException {
		if(b==null) return null;
		ByteBuffer buf = ByteBuffer.wrap(b);
		ControlMessage cm = new ControlMessage(buf.getInt(), buf.getInt(), buf.getInt(), 
				buf.getLong(), buf.getInt(), buf.getInt(),
				InetAddress.getByAddress(Arrays.copyOfRange(b, 
						ControlMessage.SIZE - ControlMessage.INET_ADDR_SIZE, 
						ControlMessage.SIZE)));
		return cm;
	}
	
	public String toString() {
		String s="[";
		s+="sendseq="+sendseq + ", ";
		s+="ackseq="+ackseq + ", ";
		s+="type="+msgStr[type] + ", ";
		s+="flowID="+flowID;
		s+=port;
		s+=(iaddr!=null?", iaddr="+iaddr:"");
		s+="]";
		return s;
	}
	
	
	public int getSendseq() {
		return sendseq;
	}
	public int getAckseq() {
		return ackseq;
	}
	public long getFlowID() {
		return flowID;
	}
	public InetAddress getInetAddress() {
		return iaddr;
	}
	public int getRemoteUDPControlPort()
	{
		return UDPPort;
	}
	public int getPort() {
		return port;
	}
	public int getType() {
		return type;
	}
	public static void main(String[] args) {
		try {
			ControlMessage cm = new ControlMessage(2, 3, (short)1, 5, (short)6, 0,InetAddress.getLocalHost());
			InetAddress iaddr = InetAddress.getLocalHost();
			System.out.println(ControlMessage.getControlMessage(cm.getBytes()));
			ByteBuffer buf = ByteBuffer.allocate(64);
			buf.putInt(25);
			buf.putShort((short)4);
			buf.put("Test".getBytes());
			buf.flip();
			buf.getInt();
			buf.getShort();
			byte[] b = new byte[buf.remaining()];
			buf.get(b);
			System.out.println((b.length));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
