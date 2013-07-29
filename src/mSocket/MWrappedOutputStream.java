package mSocket;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

public class MWrappedOutputStream extends FilterOutputStream {
	MSocketController controller=null;
	final long flowID;
	OutputStream curOut=null;
	
	private static Logger log = Logger.getLogger(MWrappedOutputStream.class.getName());
	
	MWrappedOutputStream(OutputStream out, MSocketController msc, long fid) {
		super(out);
		curOut=out;
		controller=msc;
		flowID = fid;
	}
	
	protected OutputStream getRawOutputStream() {
		return curOut;
	}
	
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length,DataMessage.DATA_MESG);
	}
	
	//aditya non blocking local write call, returns number written , may be equal to buffer length or eqaul to amount of space available in outbuffer
	public void localwrite(byte[] b) throws IOException {
		localwrite(b, 0, b.length);
	}
	
	public synchronized int localwrite(byte[] b,int offset, int length) throws IOException
	{
		int numwritten=0;
		
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("Connection must be established before a write");
		boolean ret=cinfo.addOutBuffer(b, offset, length); // first write to outbuffer
		if(!ret)
		{
			return numwritten;
		}else{
			numwritten=length;
		}
		cinfo.updateDataSendSeq(length);
		
		
		
		//aditya if some earlier wrte failed and til now socket is not connected, then don't atttmept to write to socket, just add data in local buffer and return
		if(cinfo.getblockingFlag() )
		{
			log.trace("only outbuffer write");
			return numwritten;
		}
		
		//aditya for handling synchronization
		//MSocket socketbeforeexception=cinfo.getMSocket();
		
		//aditya check to refresh output stream proactively, 
		//TODO: more better solution needed, currently first write after migration  vanishes  if this is not done, can't call write again in exception as don't know how much was written, typical socket problem
		//temporary hack, may need to block here if refreshstream also throws exceptions
		try {
		refreshOutputStream();
		
		log.debug("message here length"+length);
		writeInternal(b, offset, length,DataMessage.DATA_MESG);
		
		}catch(IOException e){
			log.trace("IOException during localwrite setting blocking falg to true");
			cinfo.setblockingFlag(true);
		
			//socketberfore exception ensures that it closes old socket not the new one to avoid sync problems 
			//log.trace("Closing existing connection");
			//socketbeforeexception.closeAll();
			//	log.trace("IOException blocking starts");
			
			//log.trace("IOException blocking ends");
			//assuming rensendIfNeeded sends the data missed here
		}
		return numwritten;
	}
	
	private boolean refreshOutputStream() throws IOException {
		boolean ret=false;
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo!=null) {
			//while(cinfo.getState()==ConnectionInfo.MIGRATING);
			OutputStream newOut = ((MWrappedOutputStream)cinfo.getMSocket().getOutputStream()).getRawOutputStream();
			if(newOut!=curOut) {
				log.debug("OutputStream changed");
				curOut = newOut;
				ret = true;
			}
		}
		return ret;
	}
	
	public void write(byte[] b, int MesgType) throws IOException {  //for sending the close message
		write(b, 0, b.length,MesgType);
	}
	
	public  void write(byte[] b, int offset, int length) throws IOException {  //application calls this function to send data message
		write(b, offset, length,DataMessage.DATA_MESG);
	}
	
	public synchronized void write(byte[] b, int offset, int length,int MesgType) throws IOException {
		
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("Connection must be established before a write");
		
		
		//while(!cinfo.setState(ConnectionInfo.READ_WRITE));
		//in case of exception, even migration should block until code reads inbuffer, lets not allow migration function to close input stream
		while(!cinfo.setState(ConnectionInfo.INBUFFER_READ));
		
		//aditya check to see if outbuffer is above threshold, if i is then request DataAckSeqNum from other side
		/*if( cinfo.getOutBufferSize() > OutBuffer.MAX_OUTBUFFER_SIZE)
		{
			
			log.debug("outbuffer size overflow");
			cinfo.UpdateSeqNumIfNeeded();
			controller.sendDataAckRequest(flowID);
			// wait to read the stream and put it into input buffer		
			int MaxDataSeqNum=InputStreamReadToInbuffer(cinfo);
			
			//int MaxDataSeqNum= cinfo.ProcessHeadersFromInBuffer(cinfo.getDataBoundarySeq());
			log.debug("MaxDataSeqNum "+MaxDataSeqNum);
			
			FreeOutBuffer(MaxDataSeqNum);
		}*/
		
		if(MesgType!=DataMessage.CLOSE_MESG || length!=0)
		{
			cinfo.addOutBuffer(b, offset, length); // first write to outbuffer
			cinfo.updateDataSendSeq(length);
		}
		
		//aditya for handling synchronization
		//MSocket socketbeforeexception=cinfo.getMSocket();
		cinfo.setblockingFlag(true);
		
		//aditya check to refresh output stream proactively, 
		//TODO: more better solution needed, currently first write after migration  vanishes  if this is not done, can't call write again in exception as don't know how much was written, typical socket problem
		//temporary hack, may need to block here if refreshstream also throws exceptions
		try {
				refreshOutputStream();
				
				log.debug("message here length "+length+" dataAckSeq "+cinfo.getDataAckSeq());
				//aditya
				// if same data is sent twice, then it will double update the seq number.
				//msocket was not working with mutiple out.write during experiments 
				//aditya
				writeInternal(b, offset, length,MesgType);
		}catch(IOException e) {
			//cinfo.setState(ConnectionInfo.ALL_READY);
			/*if(socketbeforeexception.getSocketState()==MSocket.CLOSING)
			{
				log.trace("Otherside closed the socket, exception on writing");
				throw e;
			}*/
			
			//change state here to ALL_READY so that state after this write reverts back, and synchronize among READ and Inbuffer read
			//cinfo.setState(ConnectionInfo.ALL_READY);
			//time to read the input stream
			//while(cinfo.setState(ConnectionInfo.INBUFFER_READ));
			
			boolean CloseRecv=false;
			
			cinfo.UpdateSeqNumIfNeeded();
			
			InputStream MyIn=cinfo.getMSocket().getUnderlyingSocket().getInputStream();
			
			SocketChannel DataChannel=cinfo.getMSocket().getDataChannel();
			DataChannel.configureBlocking(false);
			//read the input stream in the chunk of 10MBs and put them into inbuffer until EOF reached
			//byte[] readbuffer=new byte[10000000];
			ByteBuffer bytebuf = ByteBuffer.allocate(10000000);
			int cur=0;
			//FIXME: need to make it non blocking by checking if there is some data in stream
			boolean run=true;
			boolean try_migrate=false;
			while(run)
			{	
				bytebuf.clear();
				cur=0;
				while( (cur!=-1) && (cur < 10000000) )
				{
					try{
						//log.trace("bytes read from  inputstream "+cur);
							//aditya: reading from channel shouldn't cause sync problem as chaning state to inbuffer will prevent any other thread from reading or writing
						//cur+= MyIn.read(readbuffer, cur, 10000000-cur);
						
						cur+=DataChannel.read(bytebuf);
					}catch(IOException eme){
						log.trace("possibly input stream reading finished bytes read from inputstream "+cur);
						run=false;
						break;
					}
					//aditya FIXME: some thread has tried migrating, it means don't need to read input stream for close call, as there may be none, write whatever read into inbuffer and exit, don't need to read input stream anymore
					if( cinfo.getState() == ConnectionInfo.TRY_MIGRATE ) 
					{
						try_migrate=true;
						break;
					}
				}
				log.trace("bytes read from  inputstream "+cur);
				if(cur==-1)
				{
					log.trace("Input stream read complete and stored into inbuffer");
					break;
				}
				bytebuf.flip();
				//cinfo.addInBuffer(readbuffer, 0, cur);
				cinfo.addInBuffer(bytebuf.array(), 0, cur);
				
				bytebuf.flip();
				//FIXME: currently a hack o get  the closing message, improve this
				if(cur >= DataMessage.HEADER_SIZE)
				{
					byte[] closebuf=new byte[DataMessage.HEADER_SIZE];
					//System.arraycopy(readbuffer, cur-DataMessage.HEADER_SIZE, closebuf, 0, DataMessage.HEADER_SIZE);
					System.arraycopy(bytebuf.array(), cur-DataMessage.HEADER_SIZE, closebuf, 0, DataMessage.HEADER_SIZE);
					DataMessage dcm = DataMessage.getDataMessage(closebuf);
					if(dcm.Type==DataMessage.CLOSE_MESG)
					{
						log.trace("Close Recv");
						CloseRecv=true;
					}
				}
				if(try_migrate)
				{
					log.trace("honoring try migrate call, exiting the input stream read");
					break;
				}
			}
			
			//log.trace("possibly input stream reading finished bytes read from inputstream "+cur);
			cinfo.setState(ConnectionInfo.ALL_READY);
			
			if(CloseRecv)
			{
				cinfo.getMSocket().setSocketState(MSocket.CLOSING);
				log.trace("Otherside closed the socket, exception on writing");
				throw e;
			}
			
			//aditya block until socket becomes connected again,
			//socketberfore exception ensures that it closes old socket not the new one to avoid sync problems
			//log.trace("Closing existing connection");
			//socketbeforeexception.closeAll();
			log.trace("IOException blocking starts");
			//TODO: replace it with wait notufy, to prevent busy wait
			//while(!cinfo.getMSocket().isConnected() )
			while(cinfo.getblockingFlag() && (cinfo.getMSocket().getSocketState()==MSocket.ACTIVE) ) //cannot block on socket which other side has explicitly closed  
			{
				
			}
			log.trace("IOException blocking ends");
			//assuming rensendIfNeeded sends the data missed here
		}
		cinfo.setblockingFlag(false);
		//aditya change state t ALL_READY
		cinfo.setState(ConnectionInfo.ALL_READY);
			//break;
		//}
	}
	
	public void FreeOutBuffer(int DataRecvSeqNum) throws IOException{
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("Connection must be established before a write");
		log.trace("Freeing Out Buffer upto "+DataRecvSeqNum);
		cinfo.FreeOutBuffer(DataRecvSeqNum);
	}
	
	private int InputStreamReadToInbuffer(ConnectionInfo cinfo)
	{
		int MaxDataSeqNum=-1;
		boolean DataACKRecv=false;
		boolean run=true;
		ByteBuffer bytebuf = ByteBuffer.allocate(10000000);
		int cur=0;
		SocketChannel DataChannel=cinfo.getMSocket().getDataChannel();
		
		while(run)
		{
			bytebuf.clear();
			cur=0;
			while( (cur!=-1) && (cur < 10000000) )
			{
				try{
					//log.trace("bytes read from  inputstream "+cur);
						//aditya: reading from channel shouldn't cause sync problem as chaning state to inbuffer will prevent any other thread from reading or writing
					//cur+= MyIn.read(readbuffer, cur, 10000000-cur);
					bytebuf.clear();
					int readnow=DataChannel.read(bytebuf);
					cur+=readnow;
					bytebuf.flip();
					//cinfo.addInBuffer(readbuffer, 0, cur);
					log.trace("read now value "+readnow);
					cinfo.addInBuffer(bytebuf.array(), 0, readnow);
					MaxDataSeqNum= cinfo.ProcessHeadersFromInBuffer(cinfo.getDataBoundarySeq());
					if(MaxDataSeqNum !=-1)
						return MaxDataSeqNum;
				//	cinfo.getMSocket().getUnderlyingSocket().getInputStream().read(arg0, arg1, arg2);
					
				}catch(IOException eme){
					log.trace("possibly input stream reading finished bytes read from inputstream "+cur);
					run=false;
					break;
				}
			}
			log.trace("bytes read from  inputstream "+cur);
			if(cur==-1)
			{
				log.trace("Input stream read complete and stored into inbuffer");
				break;
			}
			//bytebuf.flip();
			//cinfo.addInBuffer(readbuffer, 0, cur);
			//cinfo.addInBuffer(bytebuf.array(), 0, cur);
			
			//FIXME: check later on if needed there
			/*bytebuf.flip();
			//FIXME: currently a hack o get  the closing message, improve this
			if(cur >= DataMessage.HEADER_SIZE)
			{
				byte[] closebuf=new byte[DataMessage.HEADER_SIZE];
				//System.arraycopy(readbuffer, cur-DataMessage.HEADER_SIZE, closebuf, 0, DataMessage.HEADER_SIZE);
				System.arraycopy(bytebuf.array(), cur-DataMessage.HEADER_SIZE, closebuf, 0, DataMessage.HEADER_SIZE);
				DataMessage dcm = DataMessage.getDataMessage(closebuf);
				if(dcm.Type==DataMessage.CLOSE_MESG)
				{
					log.trace("Close Recv");
					CloseRecv=true;
				}
			}*/
		}
		
		//process inbuffer to check data Headers for ACKs seq
		//int MaxSeqNumRecv;
		//should not reach here
		return MaxDataSeqNum;
	}
	
	/*public void flush() throws IOException {
		//Try to write everything in OutBuffer that hasn't been acked
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("Connection must be established before a write");
		
		while(!(cinfo.getUnacked()==null || refreshOutputStream()));
	}*/
	
	public boolean NBFlush() throws IOException {
		return refreshOutputStream();
	}
	
	private void writeInternal(byte[] b, int offset, int length,int MesgType) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		
		//while(!cinfo.setState(ConnectionInfo.READ_WRITE));
		{
			if(MesgType==DataMessage.CLOSE_MESG || length==0)   //close message and ACK
			{
				DataMessage dm = new DataMessage(MesgType,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), 0, null);
				byte[] writebuf = dm.getBytes();
				curOut.write(writebuf, 0, writebuf.length);
				
			}else{
			byte[] buf = new byte[length];
			System.arraycopy(b, offset, buf, 0, length);
			DataMessage dm = new DataMessage(MesgType,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), length, buf);
			byte[] writebuf = dm.getBytes();
			curOut.write(writebuf, 0, writebuf.length);
			}
		}
		//cinfo.setState(ConnectionInfo.ALL_READY);
	}
	
	public void write(int b) throws IOException {
		byte[] buf = new byte[1];
		buf[0] = (byte)b;
		write(buf);
	}
}