package mSocket;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.Timer;

import org.apache.log4j.Logger;

public class ConnectionInfo {
	public static final int ALL_READY=0;
	public static final int READ_WRITE=1;
	public static final int MIGRATING=3;
	public static final int CLOSING=4;    //CLOSING has highest can be done from eiter ALL_READY state or READ_WRITE state
	public static final int INBUFFER_READ=5;
	public static final int TRY_MIGRATE=6;

	public static final String[] msgStr = {"ALL_READY", "READ_WRITE", "WRITE", "MIGRATING","CLOSING","INBUFFER_READ","TRY_MIGRATE"};
	
	//FIXME: this really needs to be handlede
	public static final int MAX_UNACKED=3000;

	private MSocket msocket=null;
	private OutBuffer obuffer=null;
	private InBuffer ibuffer=null;
	private int remoteControlPort=-1;
	private InetAddress remoteControlAddress=null;
			
	// Note: These sequence numbers are for control messages and are irrelevant for data
	private int ctrlSendSeq=0;
	private int ctrlBaseSeq=-1;
	private int ctrlAckSeq=0;
	
	private int dataSendSeq=0; // sequence number of next byte to be sent
	private int dataAckSeq=0; // sequence number of first byte yet to be received
	private int dataLastSentAckSeq=0; // last dataAckSeq actually sent to other side

	/* dataBoundarySeq is the sequence number up to which data in the socket may be read
	 * without encountering a DataMessage header.
	 */
	private int dataBoundarySeq=0;

	private int state=MIGRATING;
	
	private boolean migrateRemote=false;
	
	private boolean blockingFlag=false;
	

	private static Logger log = Logger.getLogger(ConnectionInfo.class.getName());
	
	ConnectionInfo(MSocket s) {
		msocket = s;
		obuffer = new OutBuffer();
		ibuffer = new InBuffer();
	}
	
	/* flowID is set just once in the beginning by Msocket, so no
	 * synchronization is needed. 
	 */
	public long getFlowID() {
		return msocket.getFlowID();
	}
	
	public synchronized void setblockingFlag(boolean value)
	{
		blockingFlag=value;
	}
	
	public synchronized boolean getblockingFlag()
	{
		return blockingFlag;
	}
	/* The methods below are invoked by just one thread, the MSocket
	 * thread, so no synchronization is needed.
	 */
	public void setRemoteControlPort(int p) {
		remoteControlPort = p;
	}
	public void setRemoteControlAddress(InetAddress iaddr) {
		remoteControlAddress = iaddr;
	}
	public int getRemoteControlPort() {
		return remoteControlPort;
	}
	public InetAddress getRemoteControlAddress() {
		return remoteControlAddress;
	}
	public int getCtrlSendSeq() {
		return ctrlSendSeq;
	}
	public int getCtrlBaseSeq() {
		return ctrlBaseSeq;
	}
	public int getCtrlAckSeq() {
		return ctrlAckSeq;
	}
	public void setCtrlSendSeq(int s) {
		ctrlSendSeq = s;
	}
	public void setCtrlBaseSeq(int s) {
		ctrlBaseSeq = s;
	}
	public void setCtrlAckSeq(int s) {
		ctrlAckSeq = s;
	}
	public int getDataAckSeq() {
		return dataAckSeq;
	}
	public int getDataBoundarySeq() {
		return dataBoundarySeq;
	}
	public int getDataSendSeq() {
		return dataSendSeq;
	}
	public int canReadDirect() {
		return (int)(dataBoundarySeq - dataAckSeq);
	}
	public void setDataBoundarySeq(int s) {
		dataBoundarySeq = s;
	}
	
	public void updateDataBoundarySeq(int s) {
		dataBoundarySeq += s;
	}
	public void updateDataSendSeq(int s) {
		dataSendSeq += s;
	}
	public void updateDataAckSeq(int s) {
		dataAckSeq += s;
	}	
	public boolean notAckedInAWhile() {
		if(this.dataAckSeq - this.dataLastSentAckSeq > this.MAX_UNACKED)
			return true;
		else
			return false;
	}
	
	// OutBuffer internally synchronized, so no synchronization needed
	public byte[] getUnacked(int bs) {
		obuffer.setDataBaseSeq(bs);
		return obuffer.getUnacked();
	}
	public byte[] getUnacked() {
		return obuffer.getUnacked();
	}
	
	// Only called and read by Controller, so no synchronization needed
	public void setMigrateRemote(boolean b) {
		migrateRemote = b;
	}
	public boolean getMigrateRemote() {
		return migrateRemote;
	}
	
	// Only called by Msocket, so no synchronization needed
	public void setDataLastSentAckSeq(int s) {
		this.dataLastSentAckSeq = s;
	}
	
	// OutBuffer is internally synchronized, so no synchronization needed
	public boolean addOutBuffer(byte[] buf, int offset, int length) {
		//TODO: modify this part to restrict outbuffer based on system's heap size
		return obuffer.add(buf, offset, length);
	}
	
	public int getOutBufferSize(){
		return obuffer.getOutbufferSize();
	}
	
	public int ProcessHeadersFromInBuffer(int dataBoundarySeqNum)
	{
		return ibuffer.ProcessHeadersFromInBuffer(dataBoundarySeqNum);
	}
	
	public void FreeOutBuffer(int RecvDataSeqNum){
		//FIXME: long to int conversion
		ackOutBuffer(RecvDataSeqNum);
	}
	
	
	public void ackOutBuffer(long ack) {
		obuffer.ack(ack);
	}
	
	public boolean addInBuffer(byte[] buf, int offset, int length)  {
		return ibuffer.putInBuffer(buf, offset, length);
	}
	
	public boolean UpdateSeqNumIfNeeded()
	{
		return ibuffer.UpdateSeqNumIfNeeded(dataAckSeq);
	}
	
	public long getInbufferDataEndSeqNum()
	{
		return ibuffer.getdataEndSeq();
	}
	
	public int ReadInBuffer(byte[] b, int offset,int length)
	{
		return ibuffer.getInBuffer(b, offset, length);
	}
	
	public void setInbufferDataReadSeqNum(int SubtractValue){
		ibuffer.setdataReadSeqNum(SubtractValue);
	}
	
	public synchronized int getRemaingBytesinInbuffer()
	{
		return ibuffer.getRemaingBytesinInbuffer();
	}
	
	public synchronized int getState() {
		return state;
	}
	
	public synchronized boolean setState(int s) {
		boolean ret=false;
		switch(state)
		{
			case ALL_READY:
			{
				state = s;
				log.trace("Changing connection state to " + msgStr[s]);
				ret = true;
				break;
			}
			case READ_WRITE:
			{
				if(s==ALL_READY)   //sync problems everywhere, can't allow to go from READ_WRITE to MIGRATE as don't want to write to socket  data and control message simulatanoeusly, go from READ_WRITE to INBUFFER_READ, may cause sync problem with INBUFFER_READ and thread reads
				{
					state = s;
					log.trace("Changing connection state to " + msgStr[s]);
					ret = true;
				}
				break;
			}
			case INBUFFER_READ:
			{
				if(s==ALL_READY)   //only go back to ALL_READY from INBUFFER_READ, don't want thread reads of input stream and inbuffer reads to read one after another and inbuffer read non-contigous data
				{					// not even allow to go from INBUFFER_RED to INBUFFER_READ as we can't allow multiple thread to read into in buffer at same time
					state = s;
					log.trace("Changing connection state to " + msgStr[s]);
					ret = true;
				}
				break;
			}
			case MIGRATING:
			{
				if( (s==ALL_READY) /*|| (s==MIGRATING) || (s==CLOSING)*/)
				{
					state = s;
					log.trace("Changing connection state to " + msgStr[s]);
					ret = true;
				}
				break;
			}
			case CLOSING:
			{
				//closing almost closed
				break;
			}
			case TRY_MIGRATE:
			{
				//FIXME: need to check what happens in try migrate state
				if(s==ALL_READY)
				{
					state = s;
					log.trace("Changing connection state to " + msgStr[s]);
					ret = true;
				}
				break;
			}
		}
		
		/*if(state==s) {
			ret = true;
		}
		else if(state!=s && (state==ALL_READY || s==ALL_READY) ) {
			state = s;
			log.trace("Changing connection state to " + msgStr[s]);
			ret = true;
		}
		else if(s==MIGRATING) {
			state = s;
			log.trace("Changing connection state to " + msgStr[s]);
			ret = true;
		}*/
		
		if(!ret) {
			log.trace("Failed to change state to " + msgStr[s] + " from " + msgStr[state]);
			try {
				if( ( state == INBUFFER_READ ) && ( s== MIGRATING ) )
				{
					state=TRY_MIGRATE;					
				}
				wait();
				
				state = s;
				log.trace("Changing connection state to " + msgStr[s]);
				ret = true;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(s==ALL_READY){
			//FIXME: check for notify instead of notifyAll
			notifyAll();
		}
		//if(ret) notifyAll();
		return ret;
	}
	
	/* Synchronized coz msocket can get changed by either Msocket 
	 * or Controller.
	 */
	public synchronized void setMSocket(MSocket ms) {
		this.msocket = ms;
	}
	public synchronized MSocket getMSocket() {
		return msocket;
	}
	
	public static void main(String[] args) {
	}
}