package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Message;
import edu.umass.cs.gnsserver.activecode.prototype.utils.CircularBufferedRandomAccessFile;

/**
 * @author gaozy
 *
 */
public class ActiveCircularChannel implements Channel {
	
	CircularBufferedRandomAccessFile writer;
	CircularBufferedRandomAccessFile reader;
	
	/**
	 * @param ifile
	 * @param ofile
	 */
	public ActiveCircularChannel(String ifile, String ofile){
		reader = new CircularBufferedRandomAccessFile(ifile);
		writer = new CircularBufferedRandomAccessFile(ofile);
	}
	
	@Override
	public void sendMessage(Message msg) throws IOException {
		writer.write(msg.toBytes());
	}

	@Override
	public Message receiveMessage() throws IOException {
		byte[] data = reader.read();
		Message msg = null;
		if(data != null){
			try {
				msg = new ActiveMessage(data);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return msg;
	}

	@Override
	public void shutdown() {
		writer.shutdown();
		reader.shutdown();
	}

}
