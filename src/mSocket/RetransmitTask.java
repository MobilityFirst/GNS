package mSocket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.TimerTask;

import org.apache.log4j.Logger;

public class RetransmitTask extends TimerTask {
	private ControlMessage cmsg=null;
	private MSocketController controller=null;
	private int txCount=0;
	
	private static Logger log = Logger.getLogger(MServerSocket.class.getName());

	RetransmitTask(ControlMessage m, MSocketController c) throws IOException {
		cmsg = m;
		controller = c;
		if(m.getType()!=ControlMessage.ACK_ONLY) {
			/* Regular messages are scheduled for periodic 
			 * retransmission until acknowledged.
			 */
			controller.getTimer().schedule(this, 0, 1000);
		}
		else {
			// Acks are transmitted just once.
			controller.getTimer().schedule(this, 0);	
		}
	}
	
	private byte[] toByteArray() throws IOException {
		if(true) return cmsg.getBytes();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
		final ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(cmsg);
		return baos.toByteArray();		
	}
	
	private void retransmit() throws IOException {	
		DatagramPacket p = new DatagramPacket(toByteArray(), 0, toByteArray().length);
		ConnectionInfo cinfo = controller.getConnectionInfo(cmsg.getFlowID());
		InetSocketAddress sockaddr = new InetSocketAddress(
				cinfo.getRemoteControlAddress(), 
				cinfo.getRemoteControlPort());
		p.setSocketAddress(sockaddr);
		//log.debug("Sending message " + cmsg + " to " + sockaddr);
		controller.getDatagramSocket().send(p);
	}
	
	public void run() {
		try {
			ConnectionInfo cinfo = controller.getConnectionInfo(cmsg.getFlowID());
			if(cmsg.getType()==ControlMessage.ACK_ONLY) {
				log.trace("Sending ack " + cmsg);
				retransmit();
			}
			else if(controller.getConnectionInfo(cmsg.getFlowID()).getCtrlBaseSeq()<cmsg.getSendseq()) {
				if(txCount>0) log.debug("Retransmitting message " + cmsg + " coz baseseq="+cinfo.getCtrlBaseSeq());
				retransmit();
				txCount++;
			}
			else {
				log.trace("Completed delivery of message " + cmsg);
				this.cancel();
			}
		} catch(IOException e) {
			log.debug("IOException while retransmitting packet " + cmsg + "; canceling retransmission attempts");
			this.cancel();
			e.printStackTrace();
		}
	}
}