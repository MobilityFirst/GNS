package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 */
public class ActiveCodeWorkerMalTest {
	
	private final static int workerPort = 40000;
	private final static int guardPort = 60001;
	private final static int socketTimeout = 2000;
	
	
	/**
	 * @throws JSONException
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	
	@Test
	public void testMalCode() throws JSONException, IOException, InterruptedException {
		// Start a worker
		(new Thread(new WorkerGeneratorRunanble())).start();	
		DatagramSocket clientPoolSocket = new DatagramSocket(ClientPool.CALLBACK_PORT);
		DatagramSocket guardSocket = new DatagramSocket(guardPort);
		
		DatagramSocket socket = new DatagramSocket();
		socket.setSoTimeout(socketTimeout);
		
		// For timeout test
		byte[] buffer1 = new byte[2048];
		byte[] buffer2 = new byte[2048];
		
		// receive the ready message
		ActiveCodeUtils.receiveMessage(clientPoolSocket, buffer1);
		
		System.out.println("Start testing while(true) active code for ActiveCodeWorker...");
		JSONObject obj1 = new JSONObject();
		obj1.put("testGuid", "success");
		ValuesMap valuesMap = new ValuesMap(obj1);
		final String guid1 = "guid1";
		final String field1 = "testGuid";
		final String read_action = "read";
		String mal_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/mal.js"))); 
		
		ActiveCodeParams acp = new ActiveCodeParams();
		acp.setAction(read_action);
		acp.setCode(mal_code);
		acp.setField(field1);
		acp.setGuid(guid1);
		acp.setHopLimit(1);
		acp.setValuesMapString(valuesMap.toString());
		
		ActiveCodeMessage acm1 = new ActiveCodeMessage();
		acm1.setAcp(acp);
		
		//There is no guarantee for the order of the UDP packet
		ActiveCodeUtils.sendMessage(socket, acm1, workerPort);
		
		ActiveCodeMessage acmResp = ActiveCodeUtils.receiveMessage(socket, buffer1);		
		assert(acmResp == null);
		
		ActiveCodeMessage acm2 = new ActiveCodeMessage();
		acm2.setCrashed(ActiveCodeUtils.TIMEOUT_ERROR);
		ActiveCodeUtils.sendMessage(guardSocket, acm2, workerPort);		
		ActiveCodeMessage acmResp2 = ActiveCodeUtils.receiveMessage(guardSocket, buffer2);
		assert(acmResp2.getValuesMapString() == null);		
		assert(acmResp2.isCrashed() == true);
		
		
		
		// Initialize a normal request to test the worker
		String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
		
		acp = new ActiveCodeParams();
		acp.setAction(read_action);
		acp.setCode(noop_code);
		acp.setField(field1);
		acp.setGuid(guid1);
		acp.setHopLimit(1);
		acp.setValuesMapString(valuesMap.toString());
		
		ActiveCodeMessage acm3 = new ActiveCodeMessage();
		acm3.setAcp(acp);
		
		ActiveCodeUtils.sendMessage(socket, acm3, workerPort);
		
		ActiveCodeMessage acmResp3 = ActiveCodeUtils.receiveMessage(socket, buffer1);
		//System.out.println(acmResp3.isFinished()+" "+acmResp3.isCrashed());
		//System.out.println(acmResp3+" "+valuesMap.toString());
		assert(acmResp3.getValuesMapString().equals(valuesMap.toString()));
		System.out.println("Normal while(true) active code test passed!\n\n");
		
		
		//Shutdown the worker
		ActiveCodeMessage acmShutdown = new ActiveCodeMessage();
		acmShutdown.setShutdown(true);
		ActiveCodeUtils.sendMessage(socket, acmShutdown, workerPort);
	}
	
	static private class WorkerGeneratorRunanble implements Runnable{		
		public void run(){			
			ActiveCodeWorker worker = new ActiveCodeWorker(workerPort);
			try {
				worker.run();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
