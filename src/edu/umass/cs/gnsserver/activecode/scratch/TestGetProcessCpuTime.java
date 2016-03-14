package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;

public class TestGetProcessCpuTime {
	
	private static Process createNewProcess(){
		List<String> command = new ArrayList<>();
		
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.scratch.TestMXBean");
	    
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		builder.redirectInput(Redirect.INHERIT);
		
		Process process = null;
		try{
			process = builder.start();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		return process;
	}
	
	public static void main(String[] args) throws InterruptedException{
		int numProcess = 1; //Integer.parseInt(args[0]);
		Process[] processes = new Process[numProcess];
		
		for(int i=0; i<numProcess; i++){
			processes[i] = createNewProcess();
		}
		
		for(int i=0; i<numProcess; i++){
			while(processes[i].isAlive()){
				//System.out.println("Process "+i+" is not finished.");
				Thread.sleep(100);
			}
		}
		
	}
}
