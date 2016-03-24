package edu.umass.cs.gnsserver.activecode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DefaultTest;

/**
 * @author gaozy
 *
 */
public class ActiveCodeWorkerTest extends DefaultTest {
	
	private final static int workerPort = 40000;
	private final static int fakeClientPort = 50000;
	
	/**
	 * @throws JSONException
	 * @throws IOException
	 * @throws ParseException
	 * @throws ClassNotFoundException 
	 */		
	@Test
	public void testActiveWorker() throws JSONException, IOException, ParseException, ClassNotFoundException{
		// Start a worker
		(new Thread(new WorkerGeneratorRunanble())).start();		
		DatagramSocket socket = new DatagramSocket(fakeClientPort);
		byte[] buffer = new byte[1024];
				
		
		System.out.println("Start testing normal active code for ActiveCodeWorker...");
		JSONObject obj1 = new JSONObject();
		obj1.put("testGuid", "val2");
		ValuesMap valuesMap = new ValuesMap(obj1);
		final String guid1 = "guid1";
		final String field1 = "testGuid";
		final String read_action = "read";
		String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
		
		ActiveCodeParams acp = new ActiveCodeParams();
		acp.setAction(read_action);
		acp.setCode(noop_code);
		acp.setField(field1);
		acp.setGuid(guid1);
		acp.setHopLimit(1);
		acp.setValuesMapString(valuesMap.toString());
		
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setAcp(acp);
		
		ActiveCodeUtils.sendMessage(socket, acm, workerPort);
		
		
		ActiveCodeMessage acmResp = ActiveCodeUtils.receiveMessage(socket, buffer);
		ActiveCodeQueryResponse resp = acmResp.getAcqresp();
		assert(resp.getValuesMapString().equals(valuesMap.toString()));
		System.out.println("Normal active code test passed!\n\n");
		
		
		System.out.println("Start tesing normal chain active code for ActiveCodeWorker");
		String chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain-normal.js"))); 
		acp.setCode(chain_code);
		ActiveCodeUtils.sendMessage(socket, acm, workerPort);
		
		DatagramPacket pkt = ActiveCodeUtils.receivePacket(socket, buffer);
		acmResp = (ActiveCodeMessage) (new ObjectInputStream(new ByteArrayInputStream(pkt.getData()))).readObject();
		
		ActiveCodeQueryRequest acqreq = acmResp.getAcqreq();
		String targetGuid = acqreq.getGuid();
		assert(targetGuid.equals("guid2"));
		int hopLimit = acqreq.getLimit();
		assert(hopLimit == 0);
		String field2 = acqreq.getField();
		assert(field2.equals("testGuid"));
		
		System.out.println("Normal chain active code first round passed for ActiveCodeWorker!");
		
		JSONObject obj2 = new JSONObject();
		obj2.put(field2, "success");
		ValuesMap newResult = new ValuesMap(obj2);
		ActiveCodeQueryResponse acqr = new ActiveCodeQueryResponse(true, newResult.toString());
		ActiveCodeMessage acmres = new ActiveCodeMessage();
		acmres.setAcqresp(acqr);
		
		ActiveCodeUtils.sendMessage(socket, acmres, pkt.getPort());
		
		acmResp = ActiveCodeUtils.receiveMessage(socket, buffer);
		
		JSONParser parser = new JSONParser();
		org.json.simple.JSONObject obj = (org.json.simple.JSONObject) parser.parse(acmResp.getValuesMapString());
		assert(obj.get(field1).equals("success"));
		System.out.println("Normal chain active code for ActiveCodeWorker passed!");
		
		
		
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
}
