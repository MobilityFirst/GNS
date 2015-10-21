/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.nio.nioutils;

import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         A simple local tester for SSL. Run this class in two separate
 *         terminals, one with argument 100, and the other with argument 101.
 *         You also need to configure SSL parameters like keyStore, trustStore
 *         and their passwords for the JVM.
 */
public class SSLNIOTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NIOTransport<Integer> niot = null;
		Util.assertAssertionsEnabled();
		Integer myID;
		int numTestMessages = 10;

		try {
			myID = Integer.parseInt(args[0]);
		} catch (Exception e) {
			System.err
					.println("An integer node ID (either 100 or 101) should be the first command-line argument");
			return;
		}
		try {
			SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>();
			snc.addLocal(100);
			snc.addLocal(101);

			niot = new NIOTransport<Integer>(myID, snc,
					(new DataProcessingWorkerDefault()), SSL_MODES.MUTUAL_AUTH);
			(new Thread(niot)).start();

			if (myID == 101) {
				for (int i = 0; i < numTestMessages; i++) {
					niot.send(100, ("Hello from client#" + i).getBytes());
					Thread.sleep(256);
				}
			}
			if (myID == 100) {
				for (int i = 0; i < numTestMessages; i++) {
					niot.send(
							101,
							("Hello message from server (not necessarily in response to hello from client)#" + i)
									.getBytes());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			niot.stop();
		}
	}
}
