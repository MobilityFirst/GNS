package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Message;
import io.mappedbus.MappedBusReader;
import io.mappedbus.MappedBusWriter;

/**
 * @author gaozy
 *
 */
public class ActiveMappedBus implements Channel{
	private MappedBusReader reader;
	private MappedBusWriter writer;
	
	byte[] buffer = new byte[msgSize];
	
	// The size of the random access file is 1mB
	final static long fullSize = 1000000000L;
	
	// The maximal size of the message is 4KB
	final static int msgSize = 1024;
	/**
	 * @param rfile
	 * @param wfile
	 */
	public ActiveMappedBus(String rfile, String wfile){
		reader = new MappedBusReader(rfile, fullSize, msgSize);
		writer = new MappedBusWriter(wfile, fullSize, msgSize, true);
		
		try {
			reader.open();
			writer.open();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void sendMessage(Message msg) throws IOException {
		byte[] buf = msg.toBytes();
		int length = buf.length;
		try {
			writer.write(buf, 0, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	@Override
	public Message receiveMessage() throws IOException {
		Message am = null;
		
		if(reader.next()){
			try {
				reader.readBuffer(buffer, 0);
				am = new ActiveMessage(buffer);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		return am;
	}
	
	@Override
	public void shutdown() {
		if(reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		if(writer != null)
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	
}
