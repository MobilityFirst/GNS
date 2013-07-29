package edu.umass.cs.gns.mSocket;

import java.io.IOException;

import org.apache.log4j.Logger;

public class setupControlServerThread implements Runnable{
	
	private long flowID;
	private SetupControlMessage scm;
	private ConnectionInfo cinfo;
	private MSocket CallingMSocket;
	private MSocketController controller;
	private static final int ResendChunk=10000000;   //resending data in 10MB chunks
	private static Logger log = Logger.getLogger(MSocket.class.getName());
	
	public setupControlServerThread(MSocket CallingMSocket, MSocketController controller) {
		this.CallingMSocket=CallingMSocket;
		this.controller=controller;
	}
	
	public void run() {
		//	resendIfNeeded(flowID, scm);
			//FIXME: may be better method
			cinfo.setblockingFlag(false);
			
			cinfo.setState(ConnectionInfo.ALL_READY);
			log.trace("Set server state to ALL_READY");
	}
	
	/*private void setupControlServer() throws IOException {
		long localFlowID = (long)(Math.random()*Long.MAX_VALUE);

		// Read remote port, address, and flowID
		//FIXME: successive migation won't be able to handle this case, migration happening between setup control messages
		SetupControlMessage scm = CallingMSocket.setupControlRead();
		
		//aditya if other side has sent close message, the close the connection even before other side does it,
		//FIXME: remove the flow information form the connection info, but it should work for now
		System.out.println("Control mesg type "+scm.MesgType);
		ConnectionInfo cinfo = controller.getConnectionInfo(scm.flowID);
		if(scm.MesgType==SetupControlMessage.CLOSE_MESG)
		{
			CallingMSocket.setSocketState(MSocket.CLOSING);
			cinfo.getMSocket().setSocketState(MSocket.CLOSING);
			//this.closeAll();
			return;
		}
		
		// Write local port, address, and flowID
		CallingMSocket.setupControlWrite(localFlowID,SetupControlMessage.CONTROL_MESG);

		
		if(cinfo!=null) {
			/* connection exists, so just change MSocket. Note that setupControlServer
			 * (unlike setupControlClient) needs to change the MSocket itself in addition
			 * to the underlying Socket as a new MSocket is returned by the accept().
			 */
			
		/*	CallingMSocket.isNew=false;
			//aditya closing output stream so that write generates exception immidiately
			//FIXME: check if Socketchannel write also gets shutdown
			cinfo.getMSocket().getUnderlyingSocket().shutdownOutput();
			while(!cinfo.setState(ConnectionInfo.MIGRATING));
				
				log.trace("Set server state to MIGRATING");
				
				cinfo.getMSocket().closeAll();
				flowID = scm.flowID;
				cinfo.getMSocket().setMSocket(CallingMSocket);
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
		/*else {
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
	}*/
