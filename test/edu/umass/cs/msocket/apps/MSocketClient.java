package edu.umass.cs.msocket.apps;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.mobility.MobilityManagerClient;

public class MSocketClient
{
	public static void main(String[] args) throws IOException
	{
		String serverName = args[0];
		MSocket msock = new MSocket(serverName, 0);
		
		OutputStream outstream = msock.getOutputStream();
		InputStream inpstream = msock.getInputStream();
		
		byte[] byteArray = new byte[1000];
		
		int i=0;
		
		while(i < 10)
		{
			outstream.write( new String("hello world from client").getBytes() );
			inpstream.read(byteArray);
			System.out.println(new String(byteArray));
			
			try
			{
				Thread.sleep(2000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			
			i++;
		}
		msock.close();
		
		MobilityManagerClient.shutdownMobilityManager();
	}
}