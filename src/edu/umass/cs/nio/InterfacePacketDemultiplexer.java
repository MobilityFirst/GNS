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
package edu.umass.cs.nio;

/**
 * @author V. Arun
 * @param <MessageType> 
 */
public interface InterfacePacketDemultiplexer<MessageType> {
	/**
	 * @param message
	 * @return The return value should return true if the handler handled the
	 *         message and doesn't want any other BasicPacketDemultiplexer to
	 *         handle the message.
	 */

	public boolean handleMessage(MessageType message);
}
