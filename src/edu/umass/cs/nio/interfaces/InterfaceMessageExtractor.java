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

import java.net.InetSocketAddress;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;

/**
 * @author arun
 *
 * A common interface really just to make it easy to work interchangeably
 * with MessageExtractor and SSLDataProcessingWorker.
 */
public interface InterfaceMessageExtractor extends
		DataProcessingWorker {

	/**
	 * @param pd
	 */
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd);

	/**
	 * 
	 */
	public void stop();

	/**
	 * @param sockAddr
	 * @param msg
	 */
	public void processLocalMessage(InetSocketAddress sockAddr, String msg);

	/**
	 * @param message
	 */
	public void demultiplexMessage(Object message);
}
