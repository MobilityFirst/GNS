package mSocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DataMessage {
	public static final int DATA_MESG=0;
	public static final int CLOSE_MESG=1;
	public static final int DATA_ACK_REQ=2;
	
	public static final String[] Mesg_Type_Str={"DATA_MESG","CLOSE_MESG"};
	
	//aditya
	public static final int HEADER_SIZE = (Integer.SIZE*4)/8;
	final int sendSeq;
	final int ackSeq;
	final int length;
	final int Type;
	final byte[] msg;
	
	/* If the byte[] argument b is null or longer than the specified
	 * length argument l, then length is set to l; else length is 
	 * shortened to b.length. We need to allow length>0 and msg==null
	 * in the case of a header-only DataMessage.
	 */
	DataMessage(int Type,int s, int a, int l, byte[] b) {
		this.Type=Type;
		sendSeq = s;
		ackSeq = a;
		if(b==null || l<=b.length) length = l;
		else length = b.length; 
		msg = b;
	}
	public static int sizeofHeader() {
		return HEADER_SIZE;
	}
	public int size() {
		return sizeofHeader() + length;
	}
	public byte[] getBytes() {
		ByteBuffer buf = ByteBuffer.allocate(DataMessage.HEADER_SIZE + (msg!=null?msg.length:0));
		buf.putInt(Type);
		buf.putInt(sendSeq);
		buf.putInt(ackSeq);
		buf.putInt(length);
		if(msg!=null) buf.put(msg, 0, length);
		buf.flip();
		return buf.array();
	}
	/* This method assumes that the byte[] argument b exactly contains
	 * a DataMessage object, i.e., there is no excess bytes beyond the 
	 * header and the message body. If that is not the case, it will
	 * return null. 
	 */
	public static DataMessage getDataMessage(byte[] b) {
		if(b==null || b.length<DataMessage.HEADER_SIZE) return null;
		ByteBuffer buf = ByteBuffer.wrap(b);
		DataMessage dm =  new DataMessage(buf.getInt(),buf.getInt(), buf.getInt(), buf.getInt(), 
				Arrays.copyOfRange(b, DataMessage.HEADER_SIZE, b.length)); 
		return dm;
	}
	public static DataMessage getDataMessageHeader(byte[] b) {
		if(b==null || b.length<DataMessage.HEADER_SIZE) return null;
		ByteBuffer buf = ByteBuffer.wrap(b, 0, DataMessage.HEADER_SIZE);
		return new DataMessage(buf.getInt(),buf.getInt(), buf.getInt(), buf.getInt(), null);
	}
	
	public String toString() {
		String s="";
		s+=Type + ", " +sendSeq + ", " + ackSeq + ", " + length + ", " + (msg!=null?new String(msg):"");
		return s;
	}
	
	public static void main(String[] args) {
		byte[] b = "Testing the waters to get a feel".getBytes();
		DataMessage dm = new DataMessage(0,23, 19, b.length, b);
		byte[] enc = dm.getBytes();
	
		DataMessage dec = DataMessage.getDataMessage(enc);
		enc[11] = 98;
		System.out.println(dec);
	}
}