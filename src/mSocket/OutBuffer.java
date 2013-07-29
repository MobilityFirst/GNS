package mSocket;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class OutBuffer {
	public static final int MAX_OUTBUFFER_SIZE=30000000;  //30MB
	ArrayList<byte[]> sbuf=null;
	
	/* Same as ConnectionInfo.dataSendSeq, this is the sequence number 
	 * of the next byte to be sent.
	 */
	long dataSendSeq=0; 
	
	/* dataBaseSeq is the sequence number of the lowest byte that has not been 
	 * cumulatively acknowledged by the receiver.
	 */
	long dataBaseSeq=0;  
	
	/* dataStartSeq is the sequence number of first byte in the buffer. It may be 
	 * less than dataBaseSeq as dataStartSeq is advanced only when dataBaseSeq
	 * moves beyond the first whole buffer in the buffer list sbuf. 
	 */
	long dataStartSeq;  

	private static Logger log = Logger.getLogger(OutBuffer.class.getName());
	
	OutBuffer() {
		sbuf = new ArrayList<byte[]>();
	}
	
	public synchronized boolean add(byte[] src, int offset, int length) {
		if(src.length < offset + length) return false;
		//aditya 
		//FIXME: may need to improve here
		 if( (getOutbufferSize()+length)> (java.lang.Runtime.getRuntime().maxMemory()/2) )
		 {
			 log.trace("Local write fail JVM Heap memeory threshold exceeded");
			 return false;
		 }
		 byte[] dst=null;
		
		dst = new byte[length];
		
		System.arraycopy(src, offset, dst, 0, length);
		sbuf.add(dst);
		dataSendSeq += length;
		return true;
	}
	
	public synchronized int getOutbufferSize()
	{
		int i=0;
		int sizeinbytes=0;
		for(i=0;i<sbuf.size();i++)
		{
			sizeinbytes+=sbuf.get(i).length;
		}
		return sizeinbytes;
	}
	
	public boolean add(byte[] b) {
		return add(b,0,b.length);
	}
	
	public synchronized boolean ack(long ack) {
		if(ack-dataBaseSeq<=0 || ack-dataSendSeq>0) return false;
		dataBaseSeq = ack;
		while((ack-dataStartSeq>0) && sbuf.size()>0) {
			byte[] b = sbuf.get(0);
			if(ack-(dataStartSeq+b.length)>=0) {
				sbuf.remove(0);
				dataStartSeq += b.length;
			}
			else break;
		}
		return true;
	}
	
	//FIXME: have some lock on outbuffer, or call them through a single synchronized funcion from cinfo
	public synchronized void FreeOutBuffer(int RecvDataSeqNum){
		
		
		
	}
	
	public synchronized void setDataBaseSeq(long bs) {
		if(bs-dataStartSeq>=0 && bs-dataSendSeq<=0) {
			dataBaseSeq = bs;
		}
	}
	
	public synchronized byte[] getUnacked() {
		if(dataSendSeq-dataBaseSeq<=0) return null;
		ByteBuffer buf=ByteBuffer.wrap(new byte[(int)(dataSendSeq-dataBaseSeq)]);
		long curStart=dataStartSeq;
		long curPos=0;
		for(int i=0; i<sbuf.size(); i++) {
			byte[] b = sbuf.get(i);
			if(curStart+b.length-dataBaseSeq>0) {
				int srcPos = (int)Math.max(0,dataBaseSeq-curStart);
				buf.put(b, srcPos, b.length-srcPos);
			}
			curStart += b.length;
		}
		if(buf.array().length==0) log.debug("base=" + this.dataBaseSeq + "send="+this.dataSendSeq);
		return buf.array();
	}

	public String toString() {
		String s="[";
		s += "dataSendSeq=" + dataSendSeq + ", ";
		s += "dataBaseSeq=" + dataBaseSeq + ", ";
		s += "dataStartSeq=" + dataStartSeq + ", ";
		s += "numbufs=" + sbuf.size();
		s += "]";

		return s;
	}
	
	public static void main(String[] args) {
		
		OutBuffer ob = new OutBuffer();
		byte[] b1 = "Test1".getBytes();
		byte[] b2 = "Test2".getBytes();
		ob.add(b1);
		log.debug(ob);
		ob.add(b2);
		log.debug(ob);
		ob.ack(3);
		log.debug(ob);
		ob.ack(4);
		log.debug(ob);
		log.debug(new String(ob.getUnacked()));
	}
}