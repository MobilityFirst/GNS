package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
	
	
	private final ActiveRunner runner;
	private final Channel channel;
	private final int id;
	
	private Querier querier;
		
	/**
	 * Initialize a worker with a UDP channel
	 * @param port
	 * @param id
	 * @param numThread
	 */
	protected ActiveWorker(int port, int serverPort, int id){
		this.id = id;
		
		channel = new ActiveDatagramChannel(port, serverPort);
		querier = new ActiveQuerier(channel);
		runner = new ActiveRunner(querier);
		
		try {
			runWorker();
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			channel.shutdown();
		}
	}
	
	/**
	 * Initialize a worker with a named pipe
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param numThread 
	 * @param isTest
	 */
	protected ActiveWorker(String ifile, String ofile, int id, boolean isTest) {
		this.id = id;
		
		if(!isTest){
			channel = new ActiveNamedPipe(ifile, ofile);
			querier = new ActiveQuerier(channel);
			runner = new ActiveRunner(querier);
			
			try {
				runWorker();
			} catch (Exception e){
				e.printStackTrace();
			} finally {
				channel.shutdown();
			}
		} else {
			channel = null;
			runner = null;
		}
	}
	
	/**
	 * @param ifile
	 * @param ofile
	 */
	public ActiveWorker(String ifile, String ofile){
		this(ifile, ofile, 0, false);
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 * @param id 
	 * @return ValuesMap result 
	 * @throws Exception 
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl, long id) throws Exception {	
		return runner.runCode(guid, field, code, value, ttl, id);
	}

	
	private void runWorker() throws JSONException, IOException {		
		
		ActiveMessage msg = null;
		while((msg = (ActiveMessage) channel.receiveMessage()) != null){
			
			if(msg.type == Type.REQUEST){
				//System.out.println(this+" receives a request "+msg);
				ActiveMessage response;
				try {
					response = new ActiveMessage(msg.getId(), runCode(msg.getGuid(), msg.getField(), msg.getCode(), msg.getValue(), msg.getTtl(), msg.getId()), null);
				} catch (Exception e) {
					response = new ActiveMessage(msg.getId(), null, e.getMessage());
					e.printStackTrace();
				}				
				channel.sendMessage(response);
			} else if (msg.type == Type.RESPONSE ){
				System.out.println("This is a response message, the execution should not be here!");
			}
			
		}
	}
	
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		boolean pipeEnable = Boolean.parseBoolean(args[3]);
		if(pipeEnable){
			String cfile = args[0];
			String sfile = args[1];
			int id = Integer.parseInt(args[2]);
			
			new ActiveWorker(cfile, sfile, id, false);
		}
		else {
			int port = Integer.parseInt(args[0]);
			int serverPort = Integer.parseInt(args[1]);
			int id = Integer.parseInt(args[2]);
			
			new ActiveWorker(port, serverPort, id);
		}
	}
}
