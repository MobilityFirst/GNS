package edu.umass.cs.gns.mSocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class ResendIfNeededThread implements Runnable{
	private long flowID;
	private SetupControlMessage scm;
	private ConnectionInfo cinfo;
	private MSocket CallingMSocket;
	private static final int ResendChunk=10000000;   //resending data in 10MB chunks
	private static Logger log = Logger.getLogger(MSocket.class.getName());
	
	ResendIfNeededThread(long flowID,SetupControlMessage scm,ConnectionInfo cinfo,MSocket CallingMSocket) {
		this.flowID=flowID;
		this.scm=scm;
		this.cinfo=cinfo;
		this.CallingMSocket=CallingMSocket;
	}
	
	public void run() {
		try{
			resendIfNeeded(flowID, scm);
			//FIXME: may be better method
			cinfo.setblockingFlag(false);
			
			cinfo.setState(ConnectionInfo.ALL_READY);
			log.trace("Set server state to ALL_READY");
		}catch(IOException ex)
		{
			log.trace("Succesive migration: exception during migration");
			//FIXME: close the socket here
			//aditya for handling the succesive migration case
			//CallingMSocket.SocketState=MSocket.CLOSING;
			cinfo.setState(ConnectionInfo.ALL_READY);
		}
	}
	
	private void resendIfNeeded (long fid, SetupControlMessage scm) throws IOException {
		//ConnectionInfo cinfo = controller.getConnectionInfo(fid);
		if(scm.ackSeq < cinfo.getDataSendSeq()) {
			// need to resend
			log.trace("fetching resend data from  out buffer");
			//TODO: modify this code to do write in chunks instead of single large write which may crash heap size, is scm.ackSeq change after each separate resend
			byte[]  b = cinfo.getUnacked(scm.ackSeq);
			if(b==null) return;
			
			int current=0;
			int remaining=b.length;
			int totallength=b.length;
			int ackseq=scm.ackSeq;
			log.trace("total length if resend data in bytes"+totallength);
			while(current < totallength)
			{
				int sendnow=remaining%ResendChunk;
				if(sendnow==0)
				{
					sendnow=ResendChunk;
				}
				byte[] bf=new byte[sendnow];
				System.arraycopy(b, current, bf, 0, sendnow);
				
				int ACK=ackseq+current;
				DataMessage dm = new DataMessage(DataMessage.DATA_MESG,ACK, cinfo.getDataAckSeq(), sendnow, bf);
				log.trace("resend data length "+bf.length+"remote socket  port"+CallingMSocket.getDataChannel().socket().getPort());
				ByteBuffer buf = ByteBuffer.wrap(dm.getBytes());
				int numwritten=0;
				
				while(buf.remaining()>0) {
					
					numwritten=CallingMSocket.getDataChannel().write(buf);
					log.trace("num written "+numwritten);
					}	
				
				log.trace("Resent unacked bytes [" + ACK + ", " + bf.length + "] from before migration");
				current+=sendnow;
				remaining-=sendnow;
			}
		}
	}
}