package edu.umass.cs.gns.mSocket;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Timer;

import org.apache.log4j.Logger;

public class MSocketController implements Runnable {
	protected DatagramSocket ctrlSocket=null;
	private MSocket msocket=null;	
	ConnectionInfo cinfo=null;	
	protected static Timer timer=null;
	boolean isClosed=false;
			
	private static Logger log = Logger.getLogger(MSocketController.class.getName());
	
	MSocketController() {}
	
	MSocketController(MSocket ms) throws SocketException {
		msocket = ms;
		ctrlSocket = new DatagramSocket(0, msocket.getLocalAddress());
		cinfo = new ConnectionInfo(ms);
	}
	public ConnectionInfo getConnectionInfo(long flowID) {
		if(flowID != cinfo.getFlowID()) return null;
		return cinfo;
	}

	public int getLocalPort() {
		return ctrlSocket.getLocalPort();
	}
	public InetAddress getLocalAddress() {
		return ctrlSocket.getLocalAddress();
	}

	public void refreshDatagramSocket() {
		
	}
	
	private DatagramPacket receive() throws IOException {
		byte[] buf = new byte[256];
		DatagramPacket p = new DatagramPacket(buf, 0, buf.length);
		ctrlSocket.receive(p);
		return p;
	}
	private ControlMessage toControlMessage(byte[] buf) throws IOException {
		ControlMessage msg=null;
		msg = ControlMessage.getControlMessage(buf);
		return msg;
	}
	private ControlMessage extract(DatagramPacket p) {
		byte[] buf = p.getData();
		ControlMessage msg = null;
		try {
			msg = toControlMessage(buf);
		} catch(IOException e) {
			log.debug("IOException while processing received message; discarding message");
			e.printStackTrace();
		}
		return msg;
	}
	private ControlMessage receiveControlMessage() throws IOException{
		return extract(receive());
	}
	
	/*@SuppressWarnings("unused")
	private void suspendIO() throws IOException {
		//ConnectionInfo cinfo = cinfoMap.get(flowID);
		cinfo.getMSocket().closeAll();
	}*/
	
	//aditya protocol is ignore the out of order message, send the ACK for the rensent message, basically reveing window is 1 here
	private void process(ControlMessage msg) throws IOException {
		//aditya modified here from != to > as if ACK got lost, then sender will resend that message. but here just ACK needs to be sent
		if(msg.sendseq > cinfo.getCtrlAckSeq()) {
			log.trace("Received out-of-order message " + msg + "; expecting ackseq="+cinfo.getCtrlAckSeq());
			return; 
		}
		else {
			log.trace("Received in-order message " + msg);
		}
		
		if(msg.type==ControlMessage.REBIND_ADDRESS_PORT) {
			log.trace("Got Rebind");
			if(msg.sendseq == cinfo.getCtrlAckSeq() )  //aditya for the case when ACK of rebind gets lost, so the server will send rebind again, but no rebind should happen only ACK should be sent
			{
				//need to set remote control ser ip and port as it may also have changed during serve migration
				cinfo.setRemoteControlAddress(msg.getInetAddress());
				cinfo.setRemoteControlPort(msg.getRemoteUDPControlPort());
				log.trace("REBIND_ADDRESS_PORT UDP port is "+msg.getRemoteUDPControlPort());
				
				cinfo.setMigrateRemote(true);
				msocket.migrateRemote(msg.getInetAddress(), msg.getPort());
				cinfo.setMigrateRemote(false);
			}
		}else if(msg.type==ControlMessage.CLOSING)
		{
			log.trace("Recv Closing");
			if(msg.sendseq == cinfo.getCtrlAckSeq() )  //aditya for the case when ACK of rebind gets lost, so the server will send close again, but close should happen only ACK should be sent
			{
				//aditya set state to closing so that when thios app closes the socket then we don't send any UDP messga enad close the socket knwing other side has already closed it
				msocket.setSocketState(MSocket.CLOSING);
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
			SendControllerMesg(msg.getFlowID(),ControlMessage.ACK_ONLY);
		}
	}
	
	private void send(ControlMessage msg) throws IOException {
		RetransmitTask rtxtask = new RetransmitTask(msg, this);
	}
	
	//aditya writes on data stream
	//FIXME: double encapsulation
	public void sendDataAckOnly(long flowID) throws IOException {
		
		/*ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		byte[] b=null;
		((MWrappedOutputStream)(cinfo.getMSocket().getOutputStream())).write(b, 0, 0,DataMessage.DATA_MESG);*/
		
		ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		DataMessage dm = new DataMessage(DataMessage.DATA_MESG,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), (short)0, null);
		byte[] buf = dm.getBytes();
		//aditya not send by stream, as then state also needs to be changed to ALL_READY before which may cause sync problem
		//OutputStream mout = this.getMSocket(flowID).getOutputStream();
		//mout.write(buf);
		
		ByteBuffer bytebuf = null;
		bytebuf=ByteBuffer.wrap(buf);
		this.getMSocket(flowID).getDataChannel().write(bytebuf);
		
		cinfo.setDataLastSentAckSeq(cinfo.getDataAckSeq());
	}
	
	
	public synchronized void sendDataAckRequest(long flowID) throws IOException {
		/*ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		byte[] b=null;
		((MWrappedOutputStream)(cinfo.getMSocket().getOutputStream())).write(b, 0, 0,DataMessage.DATA_MESG);*/
		
		ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		DataMessage dm = new DataMessage(DataMessage.DATA_ACK_REQ,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), (short)0, null);
		byte[] buf = dm.getBytes();
		//OutputStream mout = this.getMSocket(flowID).getOutputStream();
		ByteBuffer bytebuf = null;
		bytebuf=ByteBuffer.wrap(buf);
		cinfo.getMSocket().getDataChannel().write(bytebuf);
		cinfo.setDataLastSentAckSeq(cinfo.getDataAckSeq());
	}
	
	public synchronized void sendCloseMesgOnly(long flowID) throws IOException {
		ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		byte[] b=null;
		((MWrappedOutputStream)(cinfo.getMSocket().getOutputStream())).write(b, 0, 0,DataMessage.CLOSE_MESG);
		/*ConnectionInfo cinfo = this.getConnectionInfo(flowID);
		DataMessage dm = new DataMessage(DataMessage.CLOSE_MESG,cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), (short)0, null);
		byte[] buf = dm.getBytes();
		//OutputStream mout = this.getMSocket(flowID).getOutputStream();
		cinfo.getMSocket().getOutputStream().write(buf);
		cinfo.setDataLastSentAckSeq(cinfo.getDataAckSeq());*/
	}
	
	private MSocket getMSocket(long flowID) {
		if(flowID!=cinfo.getFlowID()) return null;
		return cinfo.getMSocket();
	}
	
	/*private void sendAck(ControlMessage msg) throws IOException {
		ControlMessage ack = new ControlMessage(cinfo.getCtrlSendSeq(), cinfo.getCtrlAckSeq(), ControlMessage.ACK_ONLY, msg.getFlowID());
		send(ack);
	}*/
	
	public synchronized int SendControllerMesg(long flowID,int Mesg_Type) throws IOException
	{
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
		}
		return 0;
	}
	
	public Timer getTimer() {
		return timer;
	}
	
	public DatagramSocket getDatagramSocket() {
		return ctrlSocket;
	}
	
	public void close() {
		isClosed = true;
	}

	public void run() {
		if(timer==null) timer = new Timer();
		try{
			log.trace("Controller: " + "[ " + getLocalPort() + ", " + 
					getLocalAddress() + "; " + cinfo.getRemoteControlPort() + 
					", " + cinfo.getRemoteControlAddress() + "]");
			while(true) {
				process(receiveControlMessage());
				if(isClosed) break;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}