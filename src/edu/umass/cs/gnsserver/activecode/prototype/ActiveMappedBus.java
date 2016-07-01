package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.EOFException;
import java.io.IOException;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;
import io.mappedbus.MappedBusReader;
import io.mappedbus.MappedBusWriter;

/**
 * @author gaozy
 *
 */
public class ActiveMappedBus implements ActiveChannel{
	private MappedBusReader reader;
	private MappedBusWriter writer;
	
	final static long fullSize = 800000000L;
	final static int msgSize = 16;
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
	
	public int read(byte[] buffer){
		int length = -1;
		try {
			if(reader.next()){
				length = reader.readBuffer(buffer, 0);
			}
		} catch (EOFException e) {
			e.printStackTrace();
		}
		return length;
	}
	
	public boolean write(byte[] buffer, int offset, int length){
		boolean wSuccess = false;
		try {
			writer.write(buffer, offset, length);
			wSuccess = true;
		} catch (EOFException e) {
			e.printStackTrace();
		}
		return wSuccess;
	}
	
	
	public void shutdown(){
		if(reader != null){
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(writer != null){
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
