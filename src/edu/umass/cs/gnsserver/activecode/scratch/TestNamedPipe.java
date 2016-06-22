package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gaozy
 */
public class TestNamedPipe implements Runnable{
	
	private OutputStream writer;
	private InputStream reader;
	
	private final static int bufferSize = 1024;
	protected final static int total = 100000;
			
	private String ifile;
	private String ofile;
	private boolean isServer = false;
	private long last = 0;

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
	
	protected TestNamedPipe(String ifile, String ofile){
		this(ifile, ofile, false);
	}
	
	protected TestNamedPipe(String ifile, String ofile, boolean isServer){
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
			 //reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			reader = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println((isServer?"Server":"Client")+" reader is ready!");
		
	    
		
		byte[] buffer = new byte[bufferSize];
		
		while(true){
			try {
				int length = reader.read(buffer);
				if(length > 0){
					if(isServer) {
						//System.out.println("Server received:"+(new String(buffer))+" "+length+"bytes");
						
						// echo
						write(buffer, 0, length);
						//System.out.println("Server sent:"+(new String(buffer)));
					} else {
						//System.out.println("Client received:"+(new String(buffer)));
						incrRcvd(length);
						
					}
					Arrays.fill(buffer, (byte) 0);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
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
	
	protected void getStat(){
		System.out.println("done!"+sent+" "+received);
	}
	
	private void startWorker() throws IOException{
		List<String> command = new ArrayList<String>();
		
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms64m");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    
	    
		ProcessBuilder builder = new ProcessBuilder();
		builder.start();
	}
	
	
	
	public static void main(String[] args) throws IOException {
		
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		String msg = new String(new byte[512]);
		Runtime runtime = Runtime.getRuntime();
		
		runtime.exec("mkfifo "+cfile);
		runtime.exec("mkfifo "+sfile);
		System.out.println("mkfifo created");
		
		TestNamedPipe p1 = new TestNamedPipe(cfile, sfile);	
		new Thread(p1).start();
		TestNamedPipe p2 = new TestNamedPipe(sfile, cfile, true);
		new Thread(p2).start();
		byte[] content = (msg+"\n").getBytes();
		
		long t = System.currentTimeMillis();
		for(int i=0; i<total; i++){
			p1.write(content, 0, content.length);			
		}
		
		while(!p1.isFinished()){
		}
		
		long elapsed = System.currentTimeMillis()-t;
		System.out.println("The average latency is "+elapsed*1000.0/total+"us,thruput is "+total*1000.0/elapsed+"/s");
		
		// rm pipe file
		runtime.exec("rm "+cfile);
		runtime.exec("rm "+sfile);
	}

	
}
