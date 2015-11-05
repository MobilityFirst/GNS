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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import edu.umass.cs.nio.interfaces.DataProcessingWorker;

/**
 * @author V. Arun
 * 
 *         DefaultDataProcessingWorker simply prints the received data to
 *         standard output.
 */
public class DataProcessingWorkerDefault implements
		DataProcessingWorker {
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		byte[] rcvd = new byte[incoming.remaining()];
		incoming.get(rcvd);
		System.out.println("Received: " + new String(rcvd));
	}

	@Override
	public void demultiplexMessage(Object message) {
		System.out.println("Received: " + message.toString());		
	}
}
