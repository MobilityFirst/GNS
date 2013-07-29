package mSocket;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;

import org.apache.log4j.Logger;

public class MWrappedInputStream extends FilterInputStream {
	MSocketController controller=null;
	final long flowID;
	InputStream curIn=null;
	
	private static Logger log = Logger.getLogger(MWrappedInputStream.class.getName());

	
	MWrappedInputStream(InputStream in, MSocketController msc, long fid) {
		super(in);
		curIn = in;
		controller = msc;
		flowID = fid;
	}
	
	public boolean markSupported() {
		return false;
	}
	
	public void reset() throws IOException {
		throw new IOException("InputStream does not support mark and reset operations");
	}
	
	protected InputStream getRawInputStream() {
		return curIn;
	}
	
	//FIXME: if whole data header is not in input buffer
	private  DataMessage readDataMessageHeader(boolean ReadFromInBuffer) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		int nreadHeader=0;
		byte[] buf = new byte[DataMessage.sizeofHeader()];
		boolean EOF=false;
		DataMessage dm=null;
		//log.debug("Inside readDataMessageHeader, nreadHeader=" + nreadHeader + ", buf.length=" + buf.length);
		int BytesInBuffer=cinfo.getRemaingBytesinInbuffer();
		int bufferread=0;
		while (nreadHeader!=buf.length) {
			int cur=0;
			if((BytesInBuffer < DataMessage.sizeofHeader()) && ReadFromInBuffer)  //only checcked with begining bytes, this part of code may never get tested
			{
				if(nreadHeader < (buf.length-BytesInBuffer))
				{
					try{
						cur= curIn.read(buf, BytesInBuffer+nreadHeader, buf.length-BytesInBuffer-nreadHeader);
					}catch(IOException e){
						//exception while reading the stream whole header is not in inbuffer, not in stream as well
						return null;
					}
				}else{  //time to read begining part of header form inbuffer, reading of inbuffer is done after reading stream so that it always successful
					cur= cinfo.ReadInBuffer(buf, bufferread, BytesInBuffer-bufferread);
					bufferread+=cur;
				}
			}else
			{
				if(ReadFromInBuffer)
				{
					cur= cinfo.ReadInBuffer(buf, nreadHeader, buf.length-nreadHeader);
				}else{
					cur= curIn.read(buf, nreadHeader, buf.length-nreadHeader);
				}
			}
			if(cur!=-1) nreadHeader += cur;
			else {
				EOF=true;
				break;
			}
		}
		if(nreadHeader==DataMessage.sizeofHeader())
			dm = DataMessage.getDataMessageHeader(buf);
		// else EOF must be true and null will be returned
		return dm;
	}
	
	/* Reads either the entire length requested if possible, else reads up to dataBoundarySeq,
	 * which could be 0 if the next byte is dataBoundarySeq. Returns -1 if EOF encountered. 
	 */
	private  int singleRead(byte[] b, int offset, int length) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		int nread=0;
		boolean EOF=false;
		int ndirect=cinfo.canReadDirect();
		
		//log.debug("Insider singleRead, ndirect=" + ndirect);
		
		boolean ReadFromInBuffer=false;
		if( cinfo.getDataAckSeq() < cinfo.getInbufferDataEndSeqNum() )
		{
			ReadFromInBuffer=true;
		}
		
		if(ndirect >= length) {
			int cur = 0;
			if(ReadFromInBuffer)
			{
				cur=cinfo.ReadInBuffer(b, offset+nread, length-nread);
			}else{
				cur=curIn.read(b, offset+nread, length-nread);
			}
			//log.debug("Read direct " + cur + " bytes");
			if(cur!=-1) nread += cur;
			else EOF=true;
		}
		else if(ndirect>0) {
			int cur=0;
			if(ReadFromInBuffer){
				cur=cinfo.ReadInBuffer(b, offset+nread, ndirect);  //Read Buffer will never return -1 so it will never signla EOF
			}
			else
			{
				cur = curIn.read(b, offset+nread, ndirect);
			}
			
			//log.debug("Read less-than-direct " + cur + " bytes");
			if(cur!=-1) nread += cur;
			else EOF=true;
		}
		else { // ndirect==0
			DataMessage dmheader = readDataMessageHeader(ReadFromInBuffer);
			if(dmheader!=null) {
				cinfo.updateDataBoundarySeq(dmheader.length);
				log.trace("Data message header encountered header length "+dmheader.length);
				if(dmheader.Type==DataMessage.CLOSE_MESG)
				{
					cinfo.getMSocket().setSocketState(MSocket.CLOSING);
					log.trace("Close Message Encountered");
				}
				if( dmheader.Type==DataMessage.DATA_ACK_REQ ){
					log.trace("sending ACK Message for DATA_ACK_REQ");
					
					//FIXME: sync problem
					//cinfo.setState(ConnectionInfo.ALL_READY);
					controller.sendDataAckOnly(flowID);
				}
			}
			else {
				if(!ReadFromInBuffer)  //FIXME: only signal EOF when read form inbuffer wasn't true, inbuffer will never signal EOF it will return 0
				{
					EOF=true; // readDataMessageHeader() returns null iff EOF is encountered
				}
			}
			//log.debug("Read header " + (dmheader!=null?dmheader.size():0));
		}
		cinfo.updateDataAckSeq(nread);
		if(EOF){
			//if( cinfo.getMSocket().getSocketState() == MSocket.CLOSING )
			//{
				nread=-1;
			//}
			//else{  //FIXME: if close message is not encountered or issued, then abrupt disconnection return 0, so that it keeps on reading, check if returning 0 doesn't cause any issue
			//	nread=0;
			//}
		}
		return nread;
	}
	
	private boolean refreshInputStream() throws IOException {
		boolean ret=false;
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo!=null) {
			//while(cinfo.getState()==ConnectionInfo.MIGRATING);
			//aditya-- shouldn't here migration check be there, as user may be reading while migration is going on
			InputStream newIn = ((MWrappedInputStream)cinfo.getMSocket().getInputStream()).getRawInputStream();
			if(newIn!=curIn) {
				curIn = newIn;
				ret = true;
			}
		}
		return ret;
	}
	
	public synchronized int read(byte[] b, int offset, int length) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		while(!cinfo.setState(ConnectionInfo.READ_WRITE));
		//FIXME: improve here, protocol, read once if any exception close the socket and return 0 to the client, this way during exception we don't know how much was read, or even weahter dataseqack should be updated or not
		//best to close the socket and allow the client to read
		//aditya for handling synchronization
		//MSocket socketbeforeexception=cinfo.getMSocket();
		
		int nread=0;
		try {
			refreshInputStream();
			while(nread==0) {
					nread = readInternal(b, offset, length);
			}
		} catch(IOException e) {
			
			cinfo.setState(ConnectionInfo.ALL_READY);
			System.out.println("refreshInputStream Exception");  //aditya check to see if new input stream avaialable, if not then return 0 read, close socket before exception
			//FIXME: with other blocking method, also make read blocking if exception comes, current read also blocks if stream is not ready
			if(cinfo.getMSocket().getSocketState()==MSocket.CLOSING) 
			{
				log.trace("Otherside closed the socket, exception on reading");
				throw e;
			}
			
			//socketberfore exception ensures that it closes old socket not the new one to avoid sync problems
			/*if(socketbeforeexception.isConnected())
			{
				log.trace("Closing existing connection");
				socketbeforeexception.closeAll();
			}*/
			
			return 0;  // assuming nothing was read, corrected by resendIfneeded
		}
		
		cinfo.setState(ConnectionInfo.ALL_READY);
		if( (cinfo.getMSocket().getSocketState() != MSocket.CLOSING) && nread==-1) //FIXME: if close message is not encountered or issued, then abrupt disconnection return 0, so that it keeps on reading, check if returning 0 doesn't cause any issue
		{
			nread=0;
		}
		return nread;
	}
	
	private int readInternal(byte[] b, int offset, int length) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		int nread=0;
		//modified
		//while(!cinfo.setState(ConnectionInfo.READ_WRITE));
		{
			while(nread==0) {
				nread += this.singleRead(b, offset+nread, length-nread);
			}

			/*if(cinfo.notAckedInAWhile()) {  //aditya
				//before any write state should  change back to ALL_READY
				cinfo.setState(ConnectionInfo.ALL_READY);
				controller.sendDataAckOnly(flowID);
			}*/
		}
		//cinfo.setState(ConnectionInfo.ALL_READY);
		return nread;
	}
}