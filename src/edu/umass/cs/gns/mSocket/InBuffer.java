package edu.umass.cs.gns.mSocket;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class InBuffer {
	ArrayList<byte[]> rbuf=null;
	
	
	/* Same as ConnectionInfo.dataSendSeq, this is the sequence number 
	 * of the next byte to be sent.
	 */
	/*datRecvSeq denotes how much of data has been read from input stream
	 * End seq num of data
	 * */
	 
	long dataEndSeq=-1;
	
	/* dataBaseSeq is the sequence number of the lowest byte that has not been 
	 * cumulatively acknowledged by the receiver.
	 */
	/* denotes how much data app has read from buffer
	 * points to the 
	 * 
	 */
	long dataReadSeq=-1;  
	
	/* 
	 * data Start Seq number denotes the starting seq num of data in InBuffer 
	 */
	long dataStartSeq=-1;

	private static Logger log = Logger.getLogger(InBuffer.class.getName());
	
	InBuffer() {
		rbuf = new ArrayList<byte[]>();	
	}
	
	public synchronized boolean putInBuffer(byte[] src, int offset, int length) {
		if(src.length < offset + length) return false;
		//aditya 
		//FIXME: may need to improve here
		 /*if( (getOutbufferSize()+length)> (java.lang.Runtime.getRuntime().maxMemory()/2) )
		 {
			 log.trace("Local write fail JVM Heap memeory threshold exceeded");
			 return false;
		 }*/
		 byte[] dst=null;
		
		dst = new byte[length];
		
		System.arraycopy(src, offset, dst, 0, length);
		rbuf.add(dst);
		dataEndSeq += length;
		return true;
	}
	
	public synchronized boolean UpdateSeqNumIfNeeded(int ReaddataAckSeq)
	{
		if(ReaddataAckSeq > dataEndSeq )
		{
			log.trace("reset the seq num in Inbuffer");
			//FIXME: free the Inbuffer too here
			rbuf.clear(); // if inbuffer contains stale data then clear it, ap has already read it.
			
			dataEndSeq=ReaddataAckSeq;
			dataStartSeq=ReaddataAckSeq;
			dataStartSeq=ReaddataAckSeq;
			return true;
		}
		return false;
	}
	
	public synchronized long getdataEndSeq(){
		return dataEndSeq;
	}
	
	
	 /*public synchronized boolean CheckIfCloseMesgRecv()
	 {
		 boolean ret=false;
		 return ret;
	 }*/
	
	/*public synchronized */
	
	/*private int getOutbufferSize()
	{
		int i=0;
		int sizeinbytes=0;
		for(i=0;i<sbuf.size();i++)
		{
			sizeinbytes+=sbuf.get(i).length;
		}
		return sizeinbytes;
	}*/
	
	public boolean putInBuffer(byte[] b) {
		return putInBuffer(b,0,b.length);
	}
	
	
	public int getInBuffer(byte[] b)
	{
		return getInBuffer(b,0,b.length);
	}
	
	public synchronized int getInBuffer(byte[] b, int offset, int length) {
		
		//if(dataSendSeq-dataBaseSeq<=0) return null;
		//ByteBuffer buf=ByteBuffer.wrap(new byte[(int)(dataEndSeq-dataReadSeq)]);
		//ByteBuffer buf=ByteBuffer.wrap(new byte[length]);
		log.trace("Start="+this.dataStartSeq+" Read=" + this.dataReadSeq + " End="+this.dataEndSeq);
		
		long curStart=dataStartSeq;
		long curPos=0;
		int numread=0;
		for(int i=0; i<rbuf.size(); i++) {
			byte[] IB = rbuf.get(i);
			if(curStart+IB.length-dataReadSeq>0) {
				int srcPos = (int)Math.max(0,dataReadSeq-curStart);
				//FIXME: check for long to int conversion
				int cpylen=IB.length-srcPos;
				int actlen=0;
				if((numread+cpylen)>length)
				{
					actlen=length-numread;
				}else{
					actlen=cpylen;
				}
				System.arraycopy(IB,srcPos,b,numread ,actlen );
				numread+=actlen;
				dataReadSeq+=actlen;
				if(numread>=length)
					break;
				//buf.put(b, srcPos, b.length-srcPos);
			}
			curStart += IB.length;
		}
		//if(buf.array().length==0) 
		log.trace("Start="+this.dataStartSeq+" Read=" + this.dataReadSeq + " End="+this.dataEndSeq);
		return numread;
	}
	
	public synchronized void setdataReadSeqNum(int SubtractValue){
		this.dataReadSeq-=SubtractValue;
	}
	
	public synchronized int getRemaingBytesinInbuffer()
	{
		//FIXME: casted to int
		return (int) (dataEndSeq-dataReadSeq);
	}
	
	public synchronized int ProcessHeadersFromInBuffer(int dataBoundarySeqNum)
	{
		int MaxDataACKSeqFound=-1;
		int localdataBoundarySeq=dataBoundarySeqNum;
		
		log.trace("Start="+this.dataStartSeq+" Read=" + this.dataReadSeq + " End="+this.dataEndSeq +" dataBoundarySeqNum="+dataBoundarySeqNum);
		
		long curStart=dataStartSeq;
		long curPos=0;
		int numread=0;
		byte[] DataMessageHeaderBuf=new byte[DataMessage.HEADER_SIZE];
		for(int i=0; i<rbuf.size(); i++) {
			byte[] IB = rbuf.get(i);
			log.trace("IB.length "+IB.length);
			
			//FIXME: check for boundary case here ==0
			while( (curStart+IB.length-localdataBoundarySeq) >0 ) {
				int srcPos = (int)Math.max(0,localdataBoundarySeq-curStart);
				//FIXME: check for long to int conversion
				int cpylen=IB.length-srcPos;
				int actlen=0;
				if( cpylen >= DataMessage.HEADER_SIZE)
				{
					System.arraycopy(IB, srcPos,DataMessageHeaderBuf ,0 , DataMessage.HEADER_SIZE);
					DataMessage DM=DataMessage.getDataMessage(DataMessageHeaderBuf);
					MaxDataACKSeqFound=DM.ackSeq;
					localdataBoundarySeq=localdataBoundarySeq+DM.length+ DataMessage.HEADER_SIZE;
					
				}else{   //FIXME: header partitioned into two buffers
					System.arraycopy(IB, srcPos, DataMessageHeaderBuf, 0, cpylen);
					localdataBoundarySeq+=cpylen;
					int lencopied=cpylen;
					i++;
					if( i< rbuf.size() )  //FIXME: assuming header can only b split in two rows not more
					{
						curStart += IB.length;
						IB = rbuf.get(i);
						
						srcPos = (int)Math.max(0,localdataBoundarySeq-curStart);
						cpylen=IB.length-srcPos;
						
						if( cpylen >= DataMessage.HEADER_SIZE)
						{
							System.arraycopy(IB, srcPos,DataMessageHeaderBuf , lencopied, DataMessage.HEADER_SIZE-lencopied);
							DataMessage DM=DataMessage.getDataMessage(DataMessageHeaderBuf);
							MaxDataACKSeqFound=DM.ackSeq;
							localdataBoundarySeq+=(DM.length+DataMessage.HEADER_SIZE-lencopied);
							
						}else{
							log.trace("ERROR!!!: header split into more than twwo rows");
						}
					}else{
						log.trace("case of half header ignore it for updating MaxSeq Num recv");
						break;
					}
					//actlen=cpylen;
				}
						//buf.put(b, srcPos, b.length-srcPos);
			}
			curStart += IB.length;
		}
		//if(buf.array().length==0) 
		log.trace(" MaxDataACKSeqFound="+MaxDataACKSeqFound);
		
		return MaxDataACKSeqFound;
	}
	
	
	/*public synchronized boolean ack(long ack) {
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
		
		InBuffer ob = new InBuffer();
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
	}*/
}