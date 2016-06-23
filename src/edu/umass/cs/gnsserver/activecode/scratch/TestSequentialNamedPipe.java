package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSequentialNamedPipe {
	
	private static Process startWorker(String cfile, String sfile) throws IOException{
		
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms1024m");
	    command.add("-Xmx1024m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.scratch.EchoNamedPipe");
	    command.add(cfile);
	    command.add(sfile);
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		//builder.redirectError(Redirect.INHERIT);
		//builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();
		
		return process;
	}
	
	
	public static void main(String[] args) throws IOException {
		int size = 1000;
		int total = 100000;
		if(args.length >= 1){
			size = Integer.parseInt(args[0]);
		}
		if(args.length >= 2){
			total = Integer.parseInt(args[1]);
		}
		
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		String msg = new String(new byte[size]);
		Runtime runtime = Runtime.getRuntime();
		
		runtime.exec("mkfifo "+cfile);
		runtime.exec("mkfifo "+sfile);
		System.out.println("mkfifo created");
		
		Process proc = startWorker(cfile, sfile);
		
		FileInputStream reader = new FileInputStream(new File(sfile));
		FileOutputStream writer = new FileOutputStream(new File(cfile));
		byte[] content = msg.getBytes();
		
		byte[] buffer = new byte[1024];
		
		long t = System.currentTimeMillis();
		for(int i=0; i<total; i++){
			writer.write(content, 0, content.length);
			writer.flush();
			reader.read(buffer);
			Arrays.fill(buffer, (byte) 0);
		}
		
		long elapsed = System.currentTimeMillis()-t;
		System.out.println("The average latency is "+elapsed*1000.0/total+"us,thruput is "+total*1000.0/elapsed+"/s");
		
		writer.close();
		reader.close();
		// rm pipe file
		new File(cfile).delete();
		new File(sfile).delete();
		proc.destroy();
		
		System.exit(0);
	}
}
