package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationClient;
import edu.umass.cs.utils.Utils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runners.MethodSorters;
import org.springframework.scheduling.Trigger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Arrays;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TriggerTest extends DefaultGNSTest {

	private static GuidEntry masterGuid;

	private static final int WAIT_SETTLE = 200;

	public TriggerTest() {
		try {
			masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
		} catch (Exception e) {
			Utils.failWithStackTrace("Exception while creating account guid: "
				+ e);
		}
	}

	@Test
	public void test_010_addTrigger() {
		String guid1 = "guid" + RandomString.randomString(12);
		String guid2 = "guid" + RandomString.randomString(12);

		GuidEntry guid1Entry=null, guid2Entry = null;
		try {
			client.execute(GNSCommand.guidCreate(masterGuid, guid1));
			guid1Entry = GuidUtils.getGUIDKeys(guid1);
			client.execute(GNSCommand.guidCreate(masterGuid, guid2));
			guid2Entry = GuidUtils.getGUIDKeys(guid2);
		} catch (IOException | ClientException e) {
			Utils.failWithStackTrace("Exception creating guids: " + e);
		}
		// guid1 created
		String key1 = "key1", key2="key2";
		try {
			client.execute(GNSCommand.fieldUpdate(guid1Entry, key1, "value1"));
			System.out.println(client.execute(GNSCommand.fieldRead(guid1Entry,
				key1)).getResult());
			waitSettle(WAIT_SETTLE);
		} catch (IOException | ClientException e) {
			Utils.failWithStackTrace("Exception creating and reading field" +
				key1);
		}

		DatagramSocket datagramSocket = null;
		try {
			 datagramSocket = new DatagramSocket();
		} catch (SocketException e) {
			Utils.failWithStackTrace("Exception creating UDP socket");
		}

		try {
			// add trigger
			client.execute(GNSCommand.addTrigger(guid1, guid2Entry, Arrays
				.asList(key1).toArray(new String[0]), datagramSocket
				.getLocalAddress().getHostAddress().toString(), datagramSocket
				.getLocalPort()));
			// update to test trigger
			client.execute(GNSCommand.fieldUpdate(guid1Entry, key1,
				"value11"));
			// read expected notification
			byte[] buf = new byte[1024];
			DatagramPacket datagramPacket = new DatagramPacket(buf,buf.length);
			datagramSocket.receive(datagramPacket);
			System.out.println("Received notification: " + datagramPacket.getData());

			// update another field that should not trigger notification
			client.execute(GNSCommand.fieldUpdate(guid1Entry, key2,
				"value2"));
			waitSettle(WAIT_SETTLE);


			byte[] buf2 = new byte[1024];
			DatagramPacket datagramPacket2 = new DatagramPacket(buf,buf.length);
			datagramSocket.setSoTimeout(1000);
			datagramSocket.receive(datagramPacket2);
			System.out.println("Received datagram: " +
				new String(datagramPacket2.getData()));

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClientException e) {
			e.printStackTrace();
		}
	}

	private static void waitSettle(long wait) {
		try {
			if (wait > 0) {
				Thread.sleep(wait);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		Result result = JUnitCore.runClasses(TESTReconfigurationClient.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
			failure.getException().printStackTrace();
		}
	}
}
