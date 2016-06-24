package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author gaozy
 *
 */
public class ActivePipe implements ActiveChannel{
	
	private RandomAccessFile reader;
	private RandomAccessFile writer;
	
	/**
	 * @param ifile 
	 * @param ofile 
	 */
	public ActivePipe(String ifile, String ofile){
		try {
			reader = new RandomAccessFile(ifile, "r");
			writer = new RandomAccessFile(ofile, "rw");			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public int read(byte[] buffer){
		int length = -1;
		try {
			length = reader.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return length;
	}
	
	public boolean write(byte[] buffer, int offset, int length){
		boolean wSuccess = false;
		try {
			writer.write(buffer, offset, length);
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
