package mSocket;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class MServerSocket extends ServerSocket {
	ServerSocketChannel ssc=null;
	ServerSocket ss=null;
	MServerSocketController controller=null;
	HashMap<Long,MSocket> socketMap=null;
	private boolean Migrating=false;  // used for catching accept exception while migrating the server listening socket
	
	private static Logger log = Logger.getLogger(MServerSocket.class.getName());

	public MServerSocket() throws IOException, SocketException {
		ssc = ServerSocketChannel.open();
		ss = ssc.socket();
		initialize();
	}
	public MServerSocket(InetAddress iaddr, int port) throws IOException, SocketException {
		ssc = ServerSocketChannel.open();
		ss = ssc.socket();
		ss.bind(new InetSocketAddress(iaddr, port));
		initialize();
	}
	public MServerSocket(int port) throws IOException, SocketException {
		ssc = ServerSocketChannel.open();
		ss = ssc.socket();
		ss.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
		initialize();
	}
	public InetAddress getInetAddress() {
		return ss.getInetAddress();
	}
	public boolean getMigratingState()
	{
		return Migrating;
	}
	
	/* TBD: Need to transfer new MSocket info to old MSocket */

	public MSocket accept() throws IOException {	
		MSocket ms=null;
		while(true)
		{
			try{
			SocketChannel connectionSocketChannel = ssc.accept();
			ms = new MSocket(connectionSocketChannel, controller);
			if(ms.getSocketState()==MSocket.ACTIVE)  //aditya no need to do these steps for scoket which sent closing message from client
			{
				socketMap.put(ms.getFlowID(), ms);
				controller.setConnectionInfo(ms);
			
				log.info("Accepted connection from " + ms.getInetAddress() + ":" + ms.getPort());
				if(ms.isNew())
				{
					break;
				}
			}else{
				ms.close();  //aditya close this new socket
			}
			}catch (IOException e)
			{
				if(Migrating)
				{
					log.info("Accept causes exception due to listening socekt migration");
					continue;
				}else{
					throw e;
				}	
			}
			catch(Exception e)
			{
				if(Migrating)
				{
					log.info("Accept causes exception due to listening socekt migration");
					continue;
				}else{
					log.info("exception of Exception type must be because of close of accepting socket");
					continue;
				}
			}
		}
		return ms;
	}
	
	// MServerSocket only supports local migrate
	public void migrate(InetAddress localAddress, int localPort) throws IOException {
		//FIXME: may need t change state here  of all flows to migrating, close the previous socket 
		//aditya changing listening socket,
		Migrating=true;
		ssc.close();
		ss.close();
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ssc = ServerSocketChannel.open();
		ss = ssc.socket();
		ss.bind(new InetSocketAddress(localAddress, localPort));
		int UDPPort=controller.renewControlSocket(localAddress);
		Migrating=false;
		//aditya UDP address will also change
		
		log.debug("MServerSocket new UDP port of server "+UDPPort);
		controller.initMigrateChildren(localAddress, localPort,UDPPort);
			
		//aditya call accept here, so that new socket is listening 
	}
	
	public void close() throws IOException {
		ssc.close();
		controller.close();
	}
	
	private void initialize() throws SocketException {
		if(socketMap==null) socketMap = new HashMap<Long,MSocket>();    //may be related to flowIDs
		if(controller==null) { controller = new MServerSocketController(this);  
								//aditya moving thread here
								(new Thread(controller)).start();
								}// enabling UDP control socket
	}
}