package edu.umass.cs.gns.mSocket;

import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.*;
import java.util.HashMap;
import java.util.Timer;

public class MServerSocketController extends MSocketController implements Runnable {
	//private DatagramSocket ctrlSocket=null;
	private DatagramSocket oldCtrlSocket=null;
	private MServerSocket mserversocket=null;
	HashMap<Long,ConnectionInfo> cinfoMap=null;
			
	private static Logger log = Logger.getLogger(MServerSocketController.class.getName());
	
	MServerSocketController(MServerSocket ms) throws SocketException {
		mserversocket = ms;
		log.trace(mserversocket.getInetAddress());
		ctrlSocket = new DatagramSocket(0, mserversocket.getInetAddress());
		cinfoMap = new HashMap<Long,ConnectionInfo>();
	}

	public void setConnectionInfo(MSocket ms) {
		ConnectionInfo cinfo = cinfoMap.get(ms.getFlowID());
		if(cinfo==null) {
			cinfo = new ConnectionInfo(ms);
			cinfoMap.put(ms.getFlowID(), cinfo);
		}
	}
	
	public synchronized void sendCloseMesgOnly(long flowID) throws IOException {
		ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		byte[] b=null;
		((MWrappedOutputStream)(cinfo.getMSocket().getOutputStream())).write(b, 0, 0,DataMessage.CLOSE_MESG);
		/*DataMessage dm = new DataMessage(DataMessage.CLOSE_MESG,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), (short)0, null);
		byte[] buf = dm.getBytes();
		//OutputStream mout = this.getMSocket(flowID).getOutputStream();
		cinfo.getMSocket().getOutputStream().write(buf);
		cinfo.setDataLastSentAckSeq(cinfo.getDataAckSeq());*/
	}
	
	public ConnectionInfo getConnectionInfo(long flowID) {
		return cinfoMap.get(flowID);
	}
		//aditya FIXME: here previous socket address may not be used, use new IP if server migrated and send this info to cliet as well 
	public int renewControlSocket(InetAddress bindToMe) throws SocketException {
		oldCtrlSocket = ctrlSocket;
		oldCtrlSocket.close();
		//aditya bind UDP to new socket as well
		//FIXME: close previous socket here
		
		ctrlSocket = new DatagramSocket(0, bindToMe);
		
		return ctrlSocket.getLocalPort();
	}
	
	private DatagramPacket receive() throws Exception {
		byte[] buf = new byte[256];
		DatagramPacket p = new DatagramPacket(buf, 0, buf.length);
		try{
		ctrlSocket.receive(p);
		}catch(Exception e)
		{
			//FIXME: improve so that udp socket can also generate exceptions
			//if(mserversocket.getMigratingState())	
			return null;
			//else{
		//		throw e;
			//}
		}
		return p;
	}
	private ControlMessage toControlMessage(byte[] buf) throws IOException {
		if(true) return ControlMessage.getControlMessage(buf);
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bais);
		ControlMessage msg=null;
		try {
			msg = (ControlMessage)ois.readObject();
		} catch(ClassNotFoundException e) {
			log.debug(e); e.printStackTrace();
		}
		return msg;
	}
	private ControlMessage extract(DatagramPacket p) throws Exception{
		ControlMessage msg = null;
		
			
		
		byte[] buf = p.getData();
		
		try {
			msg = toControlMessage(buf);
		} catch(IOException e) {
			log.debug("IOException while processing received message; discarding message");
			e.printStackTrace();
		}
		
		return msg;
	}
	
	//FIXME: controller in server MSocket is of type MSocketCOntroller instead of MServer SocketController, so by default it calls method of MSocketController not MServerSocketController
	private void suspendIO(long flowID) throws IOException {
		//TODO: may be some better way
		ConnectionInfo cinfo = cinfoMap.get(flowID);
		cinfo.getMSocket().closeAll();
	}
	
	/*private void process(ControlMessage msg) throws IOException {
		log.trace("Processing message: " + msg);
		if(msg.type==ControlMessage.ACK_ONLY) {
			ConnectionInfo cinfo = cinfoMap.get(msg.getFlowID());
			if(msg.getAckseq()>cinfo.getCtrlBaseSeq()) {
				cinfo.setCtrlBaseSeq(msg.getAckseq());
			}
		}
	}*/
	//aditya protocol is ignore the out of order message, send the ACK for the rensent message, basically reveing window is 1 here
	private void process(ControlMessage msg) throws IOException {
		//FIXME: name cinfo properly so that they avoid confusion with parent class
		ConnectionInfo cinfo = cinfoMap.get(msg.getFlowID()); //aditya different from parent class cinfo
		//aditya modified here from != to > as if ACK got lost, then sender will resend that message. but here just ACK needs to be sent
		if(msg.sendseq > cinfo.getCtrlAckSeq()) {
			log.trace("Received out-of-order message " + msg + "; expecting ackseq="+cinfo.getCtrlAckSeq());
			return; 
		}
		else {
			log.trace("Received in-order message " + msg);
		}
		
		if(msg.type==ControlMessage.CLOSING)
		{
			log.trace("Recv Closing");
			if(msg.sendseq == cinfo.getCtrlAckSeq() )  //aditya for the case when ACK of rebind gets lost, so the server will send close again, but close should happen only ACK should be sent
			{
				//aditya set state to closing so that when thios app closes the socket then we don't send any UDP messga enad close the socket knwing other side has already closed it
				
				cinfo.getMSocket().setSocketState(MSocket.CLOSING);
			}	
		}
		else if(msg.type==ControlMessage.ACK_ONLY) {
			System.out.println("ACK recv "+msg.getAckseq());
			//adiya chenged added this condition
			if(msg.getAckseq() > cinfo.getCtrlBaseSeq()) {
			cinfo.setCtrlBaseSeq(msg.getAckseq());
			}
		}
		if(msg.getType()!=ControlMessage.ACK_ONLY) {  //aditya FIXME: need to find that if this message had previously arrived and our ACK just got lost so that we don't do REBINDING again
			
			boolean sendAckk=true;
			if(msg.sendseq == cinfo.getCtrlAckSeq())
			{
				sendAckk=false;
			}
			
			System.out.println("sending ACK msg.sendseq"+msg.sendseq);
			cinfo.setCtrlAckSeq(msg.sendseq+1);
			//if(Math.random()*10 %2 ==1)
			//if(sendAckk)
			//sendAck(msg);
			SendControllerMesg(msg.getFlowID(),ControlMessage.ACK_ONLY,0,0,null);
		}
	}
	
	public void send(ControlMessage msg) throws IOException {
		RetransmitTask rtxtask = new RetransmitTask(msg, this);
	}
	
	public void initMigrateChildren(InetAddress iaddr, int port,int UDPPort) throws IOException {
		boolean succ=false;
		for(Object obj: cinfoMap.values()) {
			ConnectionInfo ci = (ConnectionInfo)obj;
			log.trace("Initiating migrate for flow " + ci.getFlowID());
			// Prepare control message
			//while(!ci.setState(ConnectionInfo.MIGRATING));
			initMigrate(iaddr, port, ci.getFlowID(),UDPPort);
		}
	}
	
	public void initMigrate(InetAddress iaddr, int port, long flowID,int UDPPort) throws IOException {
		InetSocketAddress sockaddr = new InetSocketAddress(iaddr, port);
		this.suspendIO(flowID);
		/*ConnectionInfo cinfo = cinfoMap.get(flowID);
		ControlMessage cmsg = new ControlMessage(cinfo.getCtrlSendSeq(), 
				cinfo.getCtrlAckSeq(),ControlMessage.REBIND_ADDRESS_PORT, flowID, (short)port, UDPPort,iaddr);
		log.trace("Sending control message " + cmsg + " to " + cinfo.getRemoteControlAddress() + ":" + cinfo.getRemoteControlPort());
		send(cmsg);
		cinfo.setCtrlSendSeq(cinfo.getCtrlSendSeq()+1);*/
		this.SendControllerMesg(flowID,ControlMessage.REBIND_ADDRESS_PORT,UDPPort,port,iaddr);
	}
	
	
	
	public synchronized int SendControllerMesg(long flowID,int Mesg_Type,int UDPPort,int port, InetAddress iaddr) throws IOException
	{
		ConnectionInfo cinfo=cinfoMap.get(flowID);
		switch(Mesg_Type)
		{
			case ControlMessage.ACK_ONLY:
			{
				ControlMessage ack = new ControlMessage(cinfo.getCtrlSendSeq(), cinfo.getCtrlAckSeq(), ControlMessage.ACK_ONLY, flowID);
				send(ack);
				break;
			}
			case ControlMessage.CLOSING:
			{
				ControlMessage cmsg = new ControlMessage(cinfo.getCtrlSendSeq(), 
														cinfo.getCtrlAckSeq(),ControlMessage.CLOSING, flowID);
				log.trace("Sending closing control message" + cmsg + " to " + cinfo.getRemoteControlAddress() + ":" + cinfo.getRemoteControlPort());
				send(cmsg);
				int ret=cinfo.getCtrlSendSeq();
				cinfo.setCtrlSendSeq(cinfo.getCtrlSendSeq()+1);	
				return ret; //return to the calling function the seq so that it can check when the ACK for this has arrived and block until then  
				//break;
			}
			case ControlMessage.REBIND_ADDRESS_PORT:
			{
				ControlMessage cmsg = new ControlMessage(cinfo.getCtrlSendSeq(), 
						cinfo.getCtrlAckSeq(),ControlMessage.REBIND_ADDRESS_PORT, flowID, (short)port, UDPPort,iaddr);
				log.trace("Sending control message " + cmsg + " to " + cinfo.getRemoteControlAddress() + ":" + cinfo.getRemoteControlPort());
				send(cmsg);
				cinfo.setCtrlSendSeq(cinfo.getCtrlSendSeq()+1);
				
				break;
			}
		}
		return 0;
	}
	
	public Timer getTimer() {
		return timer;
	}
	public DatagramSocket getDatagramSocket() {
		return ctrlSocket;
	}

	public void run() {
		if(timer==null) timer = new Timer();
		try{
			for(long fid: cinfoMap.keySet()) {
				log.trace("Controller: " + "[ " + getLocalPort() + ", " + getLocalAddress() + 
						"; " + cinfoMap.get(fid).getRemoteControlPort() + ", " + 
						cinfoMap.get(fid).getRemoteControlAddress() + "]");				
			}
			while(true) {
				
					DatagramPacket dg=receive();
					if(dg==null)
						continue;
					process(extract(dg));
				
				if(isClosed) break;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}