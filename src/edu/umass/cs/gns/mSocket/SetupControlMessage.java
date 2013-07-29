package edu.umass.cs.gns.mSocket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SetupControlMessage {
	public static final int CONTROL_MESG=1;
	public static final int CLOSE_MESG=2;
	
	public static final int SIZE = ControlMessage.INET_ADDR_SIZE + (Integer.SIZE*3 + Long.SIZE)/8;

	final InetAddress iaddr;
	final int port;
	final long flowID;
	final int ackSeq;
	final int MesgType;
	
	SetupControlMessage(InetAddress ia, int p, long fid, int as,int mstype) {
		iaddr = ia;
		port = p;
		flowID = fid;
		ackSeq = as;
		MesgType=mstype;
	}
	public byte[] getBytes() throws UnknownHostException {
		ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);
		buf.put(iaddr.getAddress());
		buf.putInt(port);
		buf.putLong(flowID);
		buf.putInt(ackSeq);
		buf.putInt(MesgType);
		buf.flip();
		return buf.array();
	}
	public static SetupControlMessage getSetupControlMessage(byte[] b) throws UnknownHostException {
		if(b==null) return null;
		ByteBuffer buf = ByteBuffer.wrap(b);
		byte[] ia = new byte[ControlMessage.INET_ADDR_SIZE];
		buf.get(ia);
		SetupControlMessage cm = new SetupControlMessage(InetAddress.getByAddress(ia),buf.getInt(), buf.getLong(), buf.getInt(),buf.getInt());
		return cm;
	}
	public String toString() {
		String s="[" + iaddr + ", " + port + ", " + flowID + "]";
		return s;
	}
	public static void main(String[] args) {
		try {
			SetupControlMessage scm = new SetupControlMessage(InetAddress.getLocalHost(), 2345, 473873211L, 24567,1);
			byte[] enc = scm.getBytes();
			SetupControlMessage dec = SetupControlMessage.getSetupControlMessage(enc);
			System.out.println(dec);
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}