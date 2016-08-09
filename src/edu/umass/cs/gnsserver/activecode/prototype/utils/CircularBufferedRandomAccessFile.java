package edu.umass.cs.gnsserver.activecode.prototype.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a simple implementation of a circular buffer with {@code RandomAccessFile}.
 * <p> The writer writes the data into the shared memory from the current position of the
 * writer, if the end of the file is about to be reached, set a negative integer and
 * write from the beginning of the file. To write from the beginning, the writer needs
 * to make sure the reader has already read the data. 
 * 
 * <p> The reader reads the data from the shared memory from the current position of the
 * reader, of it reads a negative value of the length of the target array, it knows 
 * 
 * @author gaozy
 *
 */
public class CircularBufferedRandomAccessFile {
	/**
	 * Only write operation will increase the value of this variable.
	 */
	private AtomicLong writerPosition = new AtomicLong();
	/**
	 * Only read operation will increase the value of this variable.
	 */
	private AtomicLong readerPosition = new AtomicLong();
	/**
	 * The size of the file
	 */
	private long size;
	
	/**
	 * Underlying file
	 */
	private RandomAccessFile mem;
	private String file;
	
	// default size 1MB
	private final static long MAX_SIZE = 1000000;
	
	/**
	 * The first 32 byte is used for writing header of the channel
	 */
	private final static long HEADER_LENGTH = Long.BYTES;
	
	/**
	 * @param file
	 * @param size
	 */
	public CircularBufferedRandomAccessFile(String file, long size){
		this.file = file;
		this.size = size;
		
		try {
			mem = new RandomAccessFile(new File(file), "rw");
			mem.setLength(size);
			mem.seek(HEADER_LENGTH);
		} catch ( IOException e) {
			e.printStackTrace();
		}
		
		writerPosition.set(HEADER_LENGTH);
		readerPosition.set(HEADER_LENGTH);
		
	}
	
	/**
	 * @param file
	 */
	public CircularBufferedRandomAccessFile(String file){
		this(file, MAX_SIZE);	
	}
		
	/**
	 * Delete the file if it already exists
	 * @param file
	 */
	private static void cleanup(String file){
		if((new File(file)).exists()){
			(new File(file)).delete();
		}		
	}

	/**
	 * @param data data to write
	 * @throws IOException
	 */
	public synchronized void write(byte[] data) throws IOException {
		/**
		 * The total length of bytes to be written, there should a
		 * integer byte left for the writer to inform reader that
		 * the end of the file has been reached.
		 */
		int length = data.length + Integer.BYTES + Integer.BYTES;
		
		/**
		 * If the written length of data is longer than the 
		 * file length from the current position, set a length
		 * to negative then write from the beginning of the file.
		 */
		if(length + writerPosition.get() > size ){
			byte[] buf = ByteBuffer.allocate(Integer.BYTES).putInt(-1).array();
			mem.write(buf, 0, Integer.BYTES);
			writerPosition.set(HEADER_LENGTH);
			mem.seek(writerPosition.get());
		}
		
		
		byte[] buffer = ByteBuffer.allocate(Integer.BYTES).putInt(data.length).array();
		mem.write(buffer, 0, Integer.BYTES);
		mem.write(data, 0, data.length);
		writerPosition.set(writerPosition.get()+Integer.BYTES+data.length);
	}


	/**
	 * @return bytes read
	 * @throws IOException
	 */
	public byte[] read() throws IOException {
		byte[] buffer = new byte[Integer.BYTES];
		mem.readFully(buffer);
		int length = ByteBuffer.wrap(buffer).getInt();
		
		if(length < 0){
			mem.seek(HEADER_LENGTH);
			mem.readFully(buffer);
			length = ByteBuffer.wrap(buffer).getInt();
			readerPosition.set(HEADER_LENGTH+Integer.BYTES+length);
		}else if(length == 0){
			mem.seek(readerPosition.get());
			return null;
		}else{
			readerPosition.set(readerPosition.get()+Integer.BYTES+length);
		}
				
		byte[] data = new byte[length];
		mem.readFully(data);
		return data;
	}

	/**
	 * 
	 */
	public void shutdown() {
		try {
			mem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		cleanup(file);
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException{
		String cfile = "/tmp/client0";
		String sfile = "/tmp/server0";
		
		CircularBufferedRandomAccessFile client = new CircularBufferedRandomAccessFile(cfile);
		
		CircularBufferedRandomAccessFile server = new CircularBufferedRandomAccessFile(sfile);
		
		client.shutdown();
		server.shutdown();
	}
}
