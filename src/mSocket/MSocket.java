package mSocket;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.apache.log4j.Logger;

public class MSocket extends Socket {
	
	public static final int ACTIVE=0;
	public static final int CLOSING=1;
	public static final int SETUP=2;
	public static String[] sstate={"ACTIVE","CLOSING","SETUP"};
	
	
	private SocketChannel dataChannel=null;
	private Socket socket=null;
	private long flowID=-1;
	private String dstName=null;
	private int SocketState;
	
	/* The two IOStreams below are stored locally 
	 * as an optimization so as to prevent the creation
	 * of a new stream object each time get*Stream() is called.
	 */
	private InputStream min=null;
	private OutputStream mout=null;
	private static final int ResendChunk=10000000;   //resending data in 10MB chunks
	
	public boolean isNew=true;
	
	private MSocketController controller=null;
	
	private static Logger log = Logger.getLogger(MSocket.class.getName());
	
	// Just returns an unbound socket
	public MSocket() {
		Socket socket = new Socket();
	}
	
	// MServerSocket invokes this
	public MSocket(SocketChannel sc, MServerSocketController mssc) throws IOException {
		socket = sc.socket();
		dataChannel = sc;
		
		controller = mssc;
		
		SocketState=MSocket.ACTIVE;
		//SocketState=MSocket.SETUP;
		
		setupControlServer();
		
		//aditya socket closing, no need to change the flow information here, as app may be reading form the socket streams even after other end has closed the socket so let the apps read froom the socket and let them close their soclet on their own
		// code will check in the exception for the socketstate if its closing then code won't block, but throw the exception to the app
		/*if(SocketState==MSocket.ACTIVE)
		{
			controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
			log.trace("Set server state to ALL_READY");
		}*/
	}

	// Client opening MSocket invokes this
	public MSocket(String name, int port) throws IOException {
		connect(name, port);
	}
	
	// Client calls this
	public void connect(String name, int port) throws IOException {
		dataChannel = SocketChannel.open();
		dataChannel.connect(new InetSocketAddress(name, port));
		while(!dataChannel.finishConnect());
		socket = dataChannel.socket();
		
		//aditya
		SocketState=MSocket.ACTIVE;
		
		log.info("Connected to server at " + socket.getInetAddress() + ":" + socket.getPort());
		setupControlClient();	
		controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
	}
	
	public int getSocketState()
	{
		return this.SocketState;
	}
	public void setSocketState(int state)
	{
		this.SocketState=state;
	}
	
	// Client calls to reconnect socket by binding it to a different local IP/port
	public synchronized void migrateLocal(InetAddress localAddress, int localPort) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("MSocket can not be migrated before a connection is established");
		while(!cinfo.setState(ConnectionInfo.MIGRATING));
		
		try {
			this.reconnectLocal(localAddress, localPort);
		} finally {
			controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
		}
	}
	
	/* Should be called only by SocketContoller, otherwise it will throw an IOException. */
	public synchronized void migrateRemote(InetAddress remoteAddress, int remotePort) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new IOException("MSocket can not be migrated before a connection is established");
		else if(!cinfo.getMigrateRemote()) throw new IOException("Unauthorized invocation of migrateRemote");
		while(!cinfo.setState(ConnectionInfo.MIGRATING)) {}

		try {
			this.reconnectRemote(remoteAddress, remotePort);
		} finally {
			controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
		}
	}	

	public synchronized InputStream getInputStream() throws IOException {
		if(flowID==-1 || controller==null) throw new SocketException("Socket is not connected");
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new SocketException("Socket is not connected");
		while(cinfo.getState()==ConnectionInfo.MIGRATING);
		
		if(this.min!=null) return this.min;
		InputStream in = socket.getInputStream();
		min = new MWrappedInputStream(in, controller, flowID);
		return this.min;
	}
	
	public synchronized OutputStream getOutputStream() throws IOException {
		if(flowID==-1 || controller==null) throw new SocketException("Socket is not connected");
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		if(cinfo==null) throw new SocketException("Socket is not connected");
		while(cinfo.getState()==ConnectionInfo.MIGRATING);
		
		if(this.mout!=null) return this.mout;
		OutputStream out = socket.getOutputStream();
		mout = new MWrappedOutputStream(out, controller, flowID);
		return this.mout;
	}	
	
	//aditya need to implement TCP state machine for closing,
	//close the socket 
	public void close() throws IOException {
		
			/*InetSocketAddress remoteaddr = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
			InetSocketAddress localaddr = new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()+(int)(Math.random()*1000)); //aditya binding on random port 
			log.trace("remote address "+remoteaddr);
			log.trace("local address "+localaddr+"SocketState "+this.SocketState);
			log.trace("SocketState "+sstate[this.SocketState]);
			if(this.SocketState==MSocket.ACTIVE)
			{
				//FIXME: may be better to wait for ack before closing own socket
				//FIXME: need to improve caaling here
				int CloseSeqNum;
				if( !(controller instanceof MServerSocketController) )
				{
					CloseSeqNum=controller.SendControllerMesg(flowID,ControlMessage.CLOSING );	
				}else{
					CloseSeqNum=((MServerSocketController)controller).SendControllerMesg(flowID,ControlMessage.CLOSING,0,0,null );
					
				}
				
				ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
				
				//aditya FIXME: close blocks until recv acknowledges the close message, may be removed later on;
				while(cinfo.getCtrlBaseSeq() <= CloseSeqNum);  
				log.trace("close block ends");
			}
			closeAll();*/
			
			ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
			
			
			if(this.SocketState==MSocket.ACTIVE)
			{	
				/*dataChannel = SocketChannel.open();
				dataChannel.socket().bind(null);
				dataChannel.connect(remoteaddr);
				while(!dataChannel.finishConnect());
				socket = dataChannel.socket();
		
				log.info("Re-connected to server at " + socket.getInetAddress() + ":" + socket.getPort()+" to send close mesg");*/
				
				sendCloseControlmesg();
				
				/*try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				//FIXME: may be removed later on
				closeAll();
				SocketState=MSocket.CLOSING;
				
				while(!cinfo.setState(ConnectionInfo.CLOSING))
				{
					
				}
			}
		//socket.close();
		//controller.close();
		//closeAll();
	}
	
	public void sendCloseControlmesg() throws IOException
	{	
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		
		//aditya 
		// write and read of control message over socket should be in same order as its read by either server or client, earlier bug, control mesaage was sent after sending data from client side and read in different order.
		// may not be good idea to send control messages in main data stream, serval paper has different data and control stream, it helps them proving formal correctness of their architecture
		
		// else connection exists, do nothing
		// is it the part of handshake?, are these sent over TCP socket
		// Write local port, address, and flowID
		if(controller instanceof MServerSocketController)
		{
			((MServerSocketController)controller).sendCloseMesgOnly(flowID);
		}else{
				((MSocketController)controller).sendCloseMesgOnly(flowID);
		}
		
		//setupControlWrite(flowID,SetupControlMessage.CLOSE_MESG);       
		
		// Read remote port, address, and flowID
		///SetupControlMessage scm = setupControlRead();	
	}
	
	public boolean isNew() {
		return isNew;
	}
	
	public int getPort() {
		return socket.getPort();
	}
	public InetAddress getInetAddress() {
		return socket.getInetAddress();
	}
	public int getLocalPort() {
		return socket.getLocalPort();
	}
	public InetAddress getLocalAddress() {
		return socket.getLocalAddress();
	}
	public boolean isConnected()
	{
		return socket.isConnected();
	}
	
	public long getFlowID() {
		return flowID;
	}
	
	/* Synchronized because min and mout are read and written
	 * by the client/server thread as well as the controller thread.
	 */
	private synchronized void nullifyIOStreams() {
		min = null;
		mout = null;
	}
	
	private void reconnectLocal(InetAddress localAddress, int localPort) throws IOException {
		
		InetSocketAddress isaddr = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
		log.trace(isaddr);
		closeAll();
		
		/*try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		dataChannel = SocketChannel.open();
		dataChannel.socket().bind(new InetSocketAddress(localAddress, localPort));
		dataChannel.connect(isaddr);
		while(!dataChannel.finishConnect());
		socket = dataChannel.socket();

		log.info("Re-connected to server at " + socket.getInetAddress() + ":" + socket.getPort());
		
		setupControlClient();
	}
	
	private void reconnectRemote(InetAddress remoteAddress, int remotePort) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		//while(!cinfo.setState(ConnectionInfo.MIGRATING));

		InetSocketAddress isaddr = new InetSocketAddress(remoteAddress, remotePort);
		closeAll();
		
		dataChannel = SocketChannel.open();
		
		dataChannel.connect(isaddr);
		while(!dataChannel.finishConnect());
		
		socket = dataChannel.socket();
		
		//aditya
		SocketState=MSocket.ACTIVE;

		log.info("Re-connected to server at " + socket.getInetAddress() + ":" + socket.getPort());
		
		setupControlClient();
		controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
	}
	

	// Write local port, address, and flowID
	public synchronized void setupControlWrite(long lfid,int mstype) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		int DataAckSeq=0;
		if(cinfo!=null)
		{
			if(cinfo.getInbufferDataEndSeqNum() > cinfo.getDataAckSeq() )
			{
				//FIXME: long to int conversion
				DataAckSeq=(int)cinfo.getInbufferDataEndSeqNum();
			}else{
				DataAckSeq=cinfo.getDataAckSeq();
			}
		}
		SetupControlMessage scm = new SetupControlMessage(controller.getLocalAddress(),
				controller.getLocalPort(), lfid, (cinfo!=null?DataAckSeq:0),mstype);
		ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());
		
		//FIXME: confirm if data sent over datachannel and data sent over stream doesn't interface
		while(buf.remaining()>0) {dataChannel.write(buf);} // SocketChannel writes may write less than requested*/
		//getOutputStream().write(buf.array());
		log.trace("Sent IP:port " + controller.getLocalPort() + ":" + controller.getLocalAddress() + "; ackSeq = " + (cinfo!=null?DataAckSeq:0));		
	}
	
	public synchronized SetupControlMessage setupControlRead() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);
		while(buf.position()<SetupControlMessage.SIZE) {dataChannel.read(buf);}
		/*byte[] b=new byte[SetupControlMessage.SIZE];
		int curr=0;
		while(curr<SetupControlMessage.SIZE) { curr+=getInputStream().read(b,curr,SetupControlMessage.SIZE-curr);}
		SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(b);*/
		SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(buf.array());
		return scm;
	}
	
	private void startController(SetupControlMessage scm) {
		log.trace("Received IP:port " + scm.port + ":" + scm.iaddr + " for flowID " + flowID + "; ackSeq = " + scm.ackSeq);
		controller.getConnectionInfo(flowID).setRemoteControlAddress(scm.iaddr);
		controller.getConnectionInfo(flowID).setRemoteControlPort(scm.port);
		//aditya moved it to controller intialization
		//(new Thread(controller)).start();
	}

	/*private void resendIfNeeded (long fid, SetupControlMessage scm) throws IOException {
		ConnectionInfo cinfo = controller.getConnectionInfo(fid);
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
				log.trace("resend data length "+bf.length+"remote socket  port"+dataChannel.socket().getPort());
				ByteBuffer buf = ByteBuffer.wrap(dm.getBytes());
				int numwritten=0;
				
				while(buf.remaining()>0) {
					
					numwritten=dataChannel.write(buf);
					log.trace("num written "+numwritten);
					}	
				
				log.trace("Resent unacked bytes [" + ACK + ", " + bf.length + "] from before migration");
				current+=sendnow;
				remaining-=sendnow;
			}
		}
	}*/
	
	// Client writes first, then reads
	private void setupControlClient() throws IOException {
		long localFlowID = (long)(Math.random()*Long.MAX_VALUE);
		boolean isNewConnection=false;
		
		if(flowID==-1) {
			flowID = localFlowID;
			controller = new MSocketController(this);
			//aditya moved here
			(new Thread(controller)).start();
			isNewConnection=true;
		}
		
		ConnectionInfo cinfo = controller.getConnectionInfo(flowID);
		//aditya client resets its databoundary seq number,  so that in resendIfneeeded next message will be a message with data header, sp earlier incomplete data message header based databoundaryseq should be reset
		// imp bug, 
		if(cinfo!=null)
		cinfo.setDataBoundarySeq(cinfo.getDataAckSeq());
		
		
		//aditya 
		// write and read of control message over socket should be in same order as its read by either server or client, earlier bug, control mesaage was sent after sending data from client side and read in different order.
		// may not be good idea to send control messages in main data stream, serval paper has different data and control stream, it hellps them proving formal correctness of their architecture
		
		// else connection exists, do nothing
		// is it the part of handshake?, are these sent over TCP socket
		// Write local port, address, and flowID
		setupControlWrite(flowID,SetupControlMessage.CONTROL_MESG);
		// Read remote port, address, and flowID
		SetupControlMessage scm = setupControlRead();
		
		if(isNewConnection) {
			// flowID is computed as average of both proposals for new connections
			flowID = (localFlowID+scm.flowID)/2;
			log.trace("Created new flow ID " + flowID);
		}
		else {
			//resendIfNeeded(flowID, scm);
			//cinfo.setblockingFlag(false);
			ResendIfNeededThread RensendObj=new ResendIfNeededThread(flowID,scm,cinfo,this);
			(new Thread(RensendObj)).start();
		}
		startController(scm);//aditya starting controller each time migration takes place
	}
	
	/*//aditya: function copied from mwrappedoutputstream, output stream shoudl also be refreshed as soon as migration is done before calling resendIfneeded, so that any failed writes can be addressed by resendFineed
	//TODO: may be need for input stream also, currently rensending lost data is migration event triggered, may need to change some time
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
	}*/
	
	// Server reads first, then writes
	private void setupControlServer() throws IOException {
		long localFlowID = (long)(Math.random()*Long.MAX_VALUE);

		// Read remote port, address, and flowID
		//FIXME: successive migation won't be able to handle this case, migration happening between setup control messages
		SetupControlMessage scm = setupControlRead();
		
		//aditya if other side has sent close message, the close the connection even before other side does it,
		//FIXME: remove the flow information form the connection info, but it should work for now
		System.out.println("Control mesg type "+scm.MesgType);
		ConnectionInfo cinfo = controller.getConnectionInfo(scm.flowID);
		if(scm.MesgType==SetupControlMessage.CLOSE_MESG)
		{
			this.SocketState=MSocket.CLOSING;
			cinfo.getMSocket().setSocketState(MSocket.CLOSING);
			//this.closeAll();
			return;
		}
		
		// Write local port, address, and flowID
		setupControlWrite(localFlowID,SetupControlMessage.CONTROL_MESG);

		
		if(cinfo!=null) {
			/* connection exists, so just change MSocket. Note that setupControlServer
			 * (unlike setupControlClient) needs to change the MSocket itself in addition
			 * to the underlying Socket as a new MSocket is returned by the accept().
			 */
			
			isNew=false;
			//aditya closing output stream so that write generates exception immidiately
			//FIXME: check if Socketchannel write also gets shutdown
			cinfo.getMSocket().socket.shutdownOutput();
			while(!cinfo.setState(ConnectionInfo.MIGRATING));
				
				log.trace("Set server state to MIGRATING");
				
				cinfo.getMSocket().closeAll();
				flowID = scm.flowID;
				cinfo.getMSocket().setMSocket(this);
				ResendIfNeededThread RensendObj=new ResendIfNeededThread(flowID,scm,cinfo,this);
				(new Thread(RensendObj)).start();
				/*	try{
					resendIfNeeded(flowID, scm);
					//FIXME: may be better method
					cinfo.setblockingFlag(false);
				}catch(IOException ex)
				{
					log.trace("Succesive migration: exception during  migration");
					//aditya for handling the succesive migration case
					this.SocketState=MSocket.CLOSING;
					cinfo.setState(ConnectionInfo.ALL_READY);
				}*/
		}
		else {
			// new flowID is computed as average of both proposals for new connections
			flowID = (localFlowID+scm.flowID)/2;
			((MServerSocketController)controller).setConnectionInfo(this);
			log.trace("Created new flow ID " + flowID);
			controller.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY);
			log.trace("Set server state to ALL_READY");
		}
		// Write local port, address, and flowID
		//setupControlWrite(localFlowID);
		startController(scm);
		//FIXME: moving cinfo here causes accept fail check later on
	}
	
	//aditya made it public
	public synchronized void closeAll() throws IOException {
		dataChannel.close();
		socket.close();
		nullifyIOStreams();
	}
	
	public MSocketController getMSocketController() {
		return controller;
	}
	private synchronized void setMSocket(MSocket ms) {
		dataChannel = ms.dataChannel;
		socket = ms.socket;
		flowID = ms.flowID;
		dstName = ms.dstName;
		min = ms.min;
		mout = ms.mout;
		controller = ms.controller;
	}
	
	public synchronized Socket getUnderlyingSocket()
	{
		return this.socket;
	}
	public synchronized SocketChannel getDataChannel()
	{
		return this.dataChannel;
	}

	
	public static void main(String[] args) {
		try {
			MSocket ms = new MSocket("localhost", 6789);
			InputStream in = ms.getInputStream();
			OutputStream out = ms.getOutputStream();
			while(true) {
				String s = "Test\n";
				//out.write(s.getBytes());
				ByteBuffer buf = ByteBuffer.allocate(1024);
				int nread = in.read(buf.array());
				log.debug("Read " + nread + " : " + new String(buf.array()));
				buf.clear();
				Thread.sleep(1000);
			}
		} catch(IOException e) {
			log.error(e); e.printStackTrace();
		} catch(Exception e) {
			log.error(e); e.printStackTrace();
		}
	}
}