package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author gaozy
 *
 */
public class ActivePipe implements ActiveChannel{
	
	private InputStream reader;
	private OutputStream writer;
	
	/**
	 * @param ifile 
	 * @param ofile 
	 */
	public ActivePipe(String ifile, String ofile){
		Thread t = new Thread(new Runnable() {
	         public void run()
	         {
	              // Insert some method call here.
	        	 try {
					reader = new FileInputStream(new File(ifile));
//					System.err.println("Setup reader on file "+ifile);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
	         }
		});
		t.start();
		
		try {			
			writer = new FileOutputStream(new File(ofile));
//			System.err.println("Setup writer on file "+ofile);
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
