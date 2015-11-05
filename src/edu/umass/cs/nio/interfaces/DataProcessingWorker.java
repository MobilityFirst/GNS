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
package edu.umass.cs.nio.interfaces;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author V. Arun
 * 
 *         An interface used by NIOTransport to process incoming byte stream
 *         data. 
 */
public interface DataProcessingWorker {
	/**
	 * @param socket The socket channel on which the bytes were received.
	 * @param incoming The bytes received.
	 */
	public abstract void processData(SocketChannel socket, ByteBuffer incoming);
	
	/**
	 * @param message
	 */
	public void demultiplexMessage(Object message);
	
}
