package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * @author gaozy
 */
public class EchoNamedPipe implements Runnable{
	
	private OutputStream writer;
	private InputStream reader;
	
	private final static int bufferSize = 2048;
	protected static int total = 100000;
			
	private String ifile;
	private String ofile;
	private boolean isServer = false;

	protected long received = 0;
	synchronized long incrRcvd(long length){
		received += length;
		return received;
	}
	
	private int totalSent = 0;
	private long sent = 0;
	synchronized void incrSent(long length){
		++totalSent;
		sent += length;
	}
	synchronized long getSent(){
		return sent;
	}
	
	protected boolean isFinished(){
		return (totalSent==total)&&(received == sent);
	}
	
	protected EchoNamedPipe(String ifile, String ofile){
		this(ifile, ofile, false);
	}
	
	protected EchoNamedPipe(String ifile, String ofile, boolean isServer){
		this.ifile = ifile;
		this.ofile = ofile;
		this.isServer = isServer;
		
		File f = new File(ofile);
		
		try {	
			writer = new FileOutputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		System.out.println((isServer?"Server":"Client")+" writer is ready!");
	}
	
	public void run() {
		File f = new File(ifile);
		try {
			reader = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println((isServer?"Server":"Client")+" reader is ready!");
				
		byte[] buffer = new byte[bufferSize];
		int length = -1;
		try {
			while( (length= reader.read(buffer)) != -1){			
				//System.out.println("Server received:"+(new String(buffer))+" "+length+"bytes");				
				// echo
				write(buffer, 0, length);
				//System.out.println("Server sent:"+(new String(buffer)));				
				Arrays.fill(buffer, (byte) 0);					
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	protected boolean write(byte[] buffer, int offset, int length){
		incrSent(length);
		//System.out.println((isServer?"Server":"Client")+" writes "+(new String(buffer))+" "+length+"bytes");
		boolean wSuccess = false;		
		try {
			writer.write(buffer, offset, length);
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
			return wSuccess;
		}
		return wSuccess;
	}
	
	protected void close(){
		try {
			if(reader != null)
				reader.close();
			if(writer != null)
				writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	
	public static void main(String[] args){
		String cfile = args[0];
		String sfile = args[1];
		EchoNamedPipe p = new EchoNamedPipe(cfile, sfile, true);
		new Thread(p).start();
	}

	
}
