/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket;

/**
 * Used to instrument the times of various code segments 
 * in the msocket.
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketInstrumenter {
	// Stores the time for multisocket read in milli secs
	private static double multiSocketReadTime 				= 0;
	// stores the number of samples
	private static double multiSocketReadNumSamples 		= 0;
	
	// Stores the time for sending dataAcks in milli secs
	private static double dataAckSendTime 					= 0;
	// stores the number of samples
	private static double dataAckSendNumSamples 			= 0;
	
	// Stores the time for singleSocket read in milli secs
	private static double singleSocketReadTime 				= 0;
	// stores the number of samples
	private static double singleSocketReadNumSamples 		= 0;
	
	// Stores the time for inbuffer read in milli secs
	private static double inbufferReadTime 					= 0;
	// stores the number of samples
	private static double inbufferReadNumSamples 			= 0;
	
	// Stores the time for inbuffer insert in milli secs
	private static double inbufferInsertTime 				= 0;
	// stores the number of samples
	private static double inbufferInsertNumSamples 			= 0;
	
	
	// Stores the time for actual singleSocket read in milli secs
	private static double actualSingleSocketReadTime 		= 0;
	// stores the number of samples
	private static double actualSingleSocketReadNumSamples 	= 0;
	
	
	// Stores the time for actual singleSocket read in milli secs
	private static double dataMessageHeaderReadTime 		= 0;
	// stores the number of samples
	private static double dataMessageHeaderNumSamples 		= 0;
	
	
	// stores the max inbuffer size reached
	private static double maxInbufferSize 					= 0;
	
	
	// stores the num of socketID1Reads
	private static double bytesReadSocketID1 				= 0;
	private static double numSocketID1Reads 				= 0;
	
	// stores the num of socketID2Reads
	private static double bytesReadSocketID2 				= 0;
	private static double numSocketID2Reads 				= 0;
	
	
	// stores the  recv buffer size of socketID 1
	private static double recvBufferSizeID1 				= 0;
	private static double recvBufferSizeID1NumSamp 			= 0;
	
	// stores the recv buffer size of socketID 2
	private static double recvBufferSizeID2 				= 0;
	private static double recvBufferSizeID2NumSamp 			= 0;
	
	private static double numOfConnAttempts					= 0;
	
	
	private static double socketAddressFromGNS				= 0;
	private static double getGUID							= 0;
	
	
	private static boolean enabled							= true;
	
	public static void addMultiSocketReadSample(long currentSample) {
		multiSocketReadTime = multiSocketReadTime + currentSample;
		multiSocketReadNumSamples++;
	}
	
	public static void addDataAckSendSample(long currentSample) {
		dataAckSendTime = dataAckSendTime + currentSample;
		dataAckSendNumSamples++;
	}
	
	public static void addSingleSocketReadSample(long currentSample) {
		singleSocketReadTime = singleSocketReadTime + currentSample;
		singleSocketReadNumSamples++;
	}
	
	public static void addInbufferReadSample(long currentSample) {
		inbufferReadTime = inbufferReadTime + currentSample;
		inbufferReadNumSamples++;
	}
	
	public static void addActualSingleSample(long currentSample) {
		actualSingleSocketReadTime = actualSingleSocketReadTime + currentSample;
		actualSingleSocketReadNumSamples++;
	}
	
	public static void addDataMessageHeaderSample(long currentSample) {
		dataMessageHeaderReadTime = dataMessageHeaderReadTime + currentSample;
		dataMessageHeaderNumSamples++;
	}
	
	public static void addInbufferInsertSample(long currentSample) {
		inbufferInsertTime = inbufferInsertTime + currentSample;
		inbufferInsertNumSamples++;
	}
	
	public static void updateMaxInbufferSize(long currentSample) {
		if( currentSample > maxInbufferSize ) 
		{
			maxInbufferSize = currentSample;
		}
	}
	
	public static void updateSocketReads(long currentSample, int socketID) {
		switch(socketID) 
		{
			case 1:
			{
				bytesReadSocketID1 = bytesReadSocketID1 + currentSample;
				numSocketID1Reads++;
				break;
			}
			
			case 2:
			{
				bytesReadSocketID2 = bytesReadSocketID2 + currentSample;
				numSocketID2Reads++;
				break;
			}
		}
	}
	
	public static void updateRecvBufferSize(long currentSample, int socketID) {
		switch(socketID)
		{
			case 1:
			{
				recvBufferSizeID1 = recvBufferSizeID1 + currentSample;
				recvBufferSizeID1NumSamp++;
				break;
			}
			
			case 2:
			{
				recvBufferSizeID2 = recvBufferSizeID2 + currentSample;
				recvBufferSizeID2NumSamp++;
				break;
			}
		}
	}
	
	public static void updateNumConnAttempt() {
		numOfConnAttempts++;
	}
	
	public static void updateSocketAddressFromGNS(long currentSample) {
		socketAddressFromGNS = currentSample;
	}
	
	public static void updateGetGUID(long currentSample) {
		getGUID = currentSample;
	}
	
	public void disable() {
		enabled = false;
	}
	
	public void enable() {
		enabled = true;
	}
	
	public String toString()
	{
		String s="";
		return s+"MSocketInstrumenter Stats: [ " +
		" AvgInbufferReadTime = "+(inbufferReadTime/inbufferReadNumSamples)
		+" singleSocketReadNumSamples = "+inbufferReadNumSamples+ "\n" + 
		
		"AvgMultiSocketReadTime = "+(multiSocketReadTime/multiSocketReadNumSamples)
		+" multiSocketReadNumSamples = "+multiSocketReadNumSamples+ "\n" +
		
		" AvgSingleSocketReadTime = "+(singleSocketReadTime/singleSocketReadNumSamples)
		+" singleSocketReadNumSamples = "+singleSocketReadNumSamples+ "\n"+
		
		" AvgActualSingleSocketReadTime = "+(actualSingleSocketReadTime/actualSingleSocketReadNumSamples)
		+" actualSingleSocketReadNumSamples = "+actualSingleSocketReadNumSamples+ "\n"+
		
		" AvgDataMessageHeaderReadTime = "+(dataMessageHeaderReadTime/dataMessageHeaderNumSamples)
		+" dataMessageHeaderNumSamples = "+dataMessageHeaderNumSamples+ "\n"+
		
		" AvgInbufferInsertTime = "+(inbufferInsertTime/inbufferInsertNumSamples)
		+" inbufferInsertNumSamples = "+inbufferInsertNumSamples+ "\n" + 
		
		" AvgDataAckSendTime = "+(dataAckSendTime/dataAckSendNumSamples) + 
		" dataAckSendNumSamples = "+dataAckSendNumSamples+ "\n" +
		
		" maxInbufferSize " + maxInbufferSize + "\n" +
		
		" bytesReadSocketID1 " + bytesReadSocketID1 + 
		" numSocketID1Reads " + numSocketID1Reads +     "\n"   +
		
		" bytesReadSocketID2 " + bytesReadSocketID2 +
		" numSocketID2Reads " + numSocketID2Reads +     "\n"   +
		
		" AvgRecvBufferSizeID1 " + (recvBufferSizeID1/recvBufferSizeID1NumSamp) + 
		" recvBufferSizeID1NumSamp " + recvBufferSizeID1NumSamp +     "\n"   +
		
		" AvgRecvBufferSizeID2 " + (recvBufferSizeID2/recvBufferSizeID2NumSamp) +
		" recvBufferSizeID2NumSamp " + recvBufferSizeID2NumSamp +     "\n"   +
		" numOfConnAttempts " + numOfConnAttempts +"\n" + 
		" socketAddressFromGNS "+ socketAddressFromGNS + "\n" +
		" getGUID " + getGUID +"\n" +
		"]";
	}
	
	/**
	 * Resets all values to zero
	 */
	public static void resetInstrumenter() 
	{
		// Stores the time for multisocket read in milli secs
		multiSocketReadTime 					= 0;
		// stores the number of samples
		multiSocketReadNumSamples 				= 0;
		
		// Stores the time for sending dataAcks in milli secs
		dataAckSendTime 						= 0;
		// stores the number of samples
		dataAckSendNumSamples 					= 0;
		
		// Stores the time for singleSocket read in milli secs
		singleSocketReadTime 					= 0;
		// stores the number of samples
		singleSocketReadNumSamples 				= 0;
		
		// Stores the time for inbuffer read in milli secs
		inbufferReadTime 						= 0;
		// stores the number of samples
		inbufferReadNumSamples 					= 0;
		
		// Stores the time for inbuffer insert in milli secs
		inbufferInsertTime 						= 0;
		// stores the number of samples
		inbufferInsertNumSamples 				= 0;
		
		
		// Stores the time for actual singleSocket read in milli secs
		actualSingleSocketReadTime 				= 0;
		// stores the number of samples
		actualSingleSocketReadNumSamples 		= 0;
		
		
		// Stores the time for actual singleSocket read in milli secs
		dataMessageHeaderReadTime 				= 0;
		// stores the number of samples
		dataMessageHeaderNumSamples 			= 0;
		
		
		// stores the max inbuffer size reached
		maxInbufferSize 						= 0;
		
		
		// stores the num of socketID1Reads
		bytesReadSocketID1 						= 0;
		numSocketID1Reads 						= 0;
		
		// stores the num of socketID2Reads
		bytesReadSocketID2 						= 0;
		numSocketID2Reads 						= 0;
		
		
		// stores the  recv buffer size of socketID 1
		recvBufferSizeID1 						= 0;
		recvBufferSizeID1NumSamp 				= 0;
		
		// stores the recv buffer size of socketID 2
		recvBufferSizeID2 						= 0;
		recvBufferSizeID2NumSamp 				= 0;
		
		numOfConnAttempts						= 0;
		
		socketAddressFromGNS					= 0;
		getGUID									= 0;
	}
	
}