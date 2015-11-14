package edu.umass.cs.msocket.apps;

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




public class BothMemberWriterExp 
{
	/*public static final String UDPServer				= "ananas.cs.umass.edu";
	public static final int UDPPort					= 54321;
	
	private ContextMember groupMember;
	private ContextWriter groupWriter;
	
	public static void main(String[] args) throws InterruptedException
	{
		String memberName = args[0];
		String writerName = args[1];
		BothMemberWriterExp obj = new BothMemberWriterExp();
		obj.startProcess(memberName, writerName);
	}
	
	public void startProcess(String memberName, String writerName) throws InterruptedException
	{
		GroupMemberThread memObj = new GroupMemberThread(memberName);
		GroupWriterThread writeObj = new GroupWriterThread(writerName);
		new Thread(memObj).start();
		
		Thread.sleep(5000);
		new Thread(writeObj).start();
		
		Thread.sleep(20000);
		
		int count = 0;
		while(true)
		{
			String mesg = "Attr Update Start ";
			System.out.println("attr Update Start "+System.currentTimeMillis());
			
			BothMemberWriterExp.sendUDP(mesg);
			
			if(count%2 == 0)
			{
				groupMember.setAttributes("contextATT0", 200);
			} else
			{
				groupMember.setAttributes("contextATT0", 50);
			}
			
			count++;
			Thread.sleep(10000);
		}
	}

	public  class GroupMemberThread implements Runnable
	{
		private final String memberName;
		
		public GroupMemberThread(String name)
		{
			this.memberName = name;
		}
		
		@Override
		public void run()
		{
			try
			{
				groupMember =  new ContextMember(memberName, new HashMap<String, Object>());
				
				String mesg = "Initial update attribute ";
				System.out.println("Initial update attribute "+System.currentTimeMillis());
				
				BothMemberWriterExp.sendUDP(mesg);
				
				groupMember.setAttributes("contextATT0", 50);
				
				ContextSocketInputStream min = (ContextSocketInputStream) groupMember;
				System.out.println("group Member working");
				
				while(true)
				{	
					byte[] b =  min.readAny();
					if(b!=null)
					{
						System.out.println("Recvd "+new String(b));
					}
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public class GroupWriterThread implements Runnable
	{
		public final String writerName;
		public GroupWriterThread(String name)
		{
			this.writerName = name;
		}
		
		@Override
		public void run() 
		{
			try
			{
				String groupName = "21 <= contextATT0 <= 147";
				
				long start = System.currentTimeMillis();
				groupWriter =  new ContextWriter(writerName, groupName);
				long end = System.currentTimeMillis();
				System.out.println("Wrtier time "+(end-start));
				
				OutputStream gout = groupWriter.getOutputStream();
				int count = 0;
				while(true) 
				{
					String hello = "hello group members of"+groupName+" count "+count++;
					gout.write(hello.getBytes());
					System.out.println(hello+" written");
					Thread.sleep(2000);
				}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public static void sendUDP( String mesg )
	{
		try
		{
			DatagramSocket client_socket = new DatagramSocket();
			
			byte[] send_data = new byte[1024];	
			//BufferedReader infromuser = 
	        //                new BufferedReader(new InputStreamReader(System.in));
			
			InetAddress IPAddress =  InetAddress.getByName(UDPServer);
			
			//String data = infromuser.readLine();
			send_data = mesg.getBytes();
			DatagramPacket send_packet = new DatagramPacket(send_data,
	                                                        send_data.length, 
	                                                        IPAddress, UDPPort);
			client_socket.send(send_packet);
			
			client_socket.close();
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}*/
	
}