package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;

/**
 * This class is deprecated, and it is only used for test only.
 * @author gaozy
 *
 */
@Deprecated
public class ActivePipe implements ActiveChannel{
	
	private InputStream reader;
	private OutputStream writer;
	byte[] readerLengthBuffer = new byte[Integer.BYTES];
	byte[] writerLengthBuffer = new byte[Integer.BYTES];
			
	/**
	 * @param ifile 
	 * @param ofile 
	 */
	public ActivePipe(String ifile, String ofile){
		Thread t = new Thread(new Runnable() {
	         public void run()
	         {
	        	try {
					reader = new FileInputStream(new File(ifile));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
	         }
		});
		t.start();
		
		try {			
			writer = new FileOutputStream(new File(ofile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public int read(byte[] buffer){
		int length = 0;
		
		try {
			
			if(reader.read(readerLengthBuffer)>0){
				length = ByteBuffer.wrap(readerLengthBuffer).getInt();
				reader.read(buffer, 0, length);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return length;
	}
	
	/**
	 * This method must be synchronized because it is not guaranteed that lines will remain intact
	 * when writing into a same pipe. They can become intermingled.
	 */
	public boolean write(byte[] buffer, int offset, int length){
		boolean wSuccess = false;
		try {
			// write the length of byte array first then send the content			
			writer.write(ByteBuffer.allocate(Integer.BYTES+length).putInt(length).put(buffer).array());
			//writer.write(buffer, offset, length);
			writer.flush();
			wSuccess = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return wSuccess;
	}
	
	public void shutdown() {
		try{
			if(reader != null)
				reader.close();
			if(writer != null)
				writer.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	
}
