
package edu.umass.cs.gnsserver.activecode.prototype.utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.min;


public class QueueFile implements Closeable {

  private static final Logger LOGGER = Logger.getLogger(QueueFile.class.getName());


  private static final int INITIAL_LENGTH = 4096; // one file system block


  private static final byte[] ZEROES = new byte[INITIAL_LENGTH];


  static final int HEADER_LENGTH = 16;


  final RandomAccessFile raf;


  private int fileLength;


  private int elementCount;


  private Element first;


  private Element last;


  private final byte[] buffer = new byte[16];


  public QueueFile(File file) throws IOException {
    if (!file.exists()) {
      initialize(file);
    }
    raf = open(file);
    readHeader();
  }

  QueueFile(RandomAccessFile raf) throws IOException {
    this.raf = raf;
    readHeader();
  }


  private static void writeInt(byte[] buffer, int offset, int value) {
    buffer[offset] = (byte) (value >> 24);
    buffer[offset + 1] = (byte) (value >> 16);
    buffer[offset + 2] = (byte) (value >> 8);
    buffer[offset + 3] = (byte) value;
  }


  private static int readInt(byte[] buffer, int offset) {
    return ((buffer[offset] & 0xff) << 24)
            + ((buffer[offset + 1] & 0xff) << 16)
            + ((buffer[offset + 2] & 0xff) << 8)
            + (buffer[offset + 3] & 0xff);
  }

  private void readHeader() throws IOException {
    raf.seek(0);
    raf.readFully(buffer);
    fileLength = readInt(buffer, 0);
    if (fileLength > raf.length()) {
      throw new IOException(
              "File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length());
    } else if (fileLength <= 0) {
      throw new IOException(
              "File is corrupt; length stored in header (" + fileLength + ") is invalid.");
    }
    elementCount = readInt(buffer, 4);
    int firstOffset = readInt(buffer, 8);
    int lastOffset = readInt(buffer, 12);
    first = readElement(firstOffset);
    last = readElement(lastOffset);
  }


  private void writeHeader(int fileLength, int elementCount, int firstPosition, int lastPosition)
          throws IOException {
    writeInt(buffer, 0, fileLength);
    writeInt(buffer, 4, elementCount);
    writeInt(buffer, 8, firstPosition);
    writeInt(buffer, 12, lastPosition);
    raf.seek(0);
    raf.write(buffer);
  }

  private Element readElement(int position) throws IOException {
    if (position == 0) {
      return Element.NULL;
    }
    ringRead(position, buffer, 0, Element.HEADER_LENGTH);
    int length = readInt(buffer, 0);
    return new Element(position, length);
  }

  private static void initialize(File file) throws IOException {
    // Use a temp file so we don't leave a partially-initialized file.
    File tempFile = new File(file.getPath() + ".tmp");
    RandomAccessFile raf = open(tempFile);
    try {
      raf.setLength(INITIAL_LENGTH);
      raf.seek(0);
      byte[] headerBuffer = new byte[16];
      writeInt(headerBuffer, 0, INITIAL_LENGTH);
      raf.write(headerBuffer);
    } finally {
      raf.close();
    }

    // A rename is atomic.
    if (!tempFile.renameTo(file)) {
      throw new IOException("Rename failed!");
    }
  }


  private static RandomAccessFile open(File file) throws FileNotFoundException {
    return new RandomAccessFile(file, "rw");
  }


  private int wrapPosition(int position) {
    return position < fileLength ? position
            : HEADER_LENGTH + position - fileLength;
  }


  private void ringWrite(int position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.write(buffer, offset, count);
    } else {
      // The write overlaps the EOF.
      // # of bytes to write before the EOF.
      int beforeEof = fileLength - position;
      raf.seek(position);
      raf.write(buffer, offset, beforeEof);
      raf.seek(HEADER_LENGTH);
      raf.write(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  private void ringErase(int position, int length) throws IOException {
    while (length > 0) {
      int chunk = min(length, ZEROES.length);
      ringWrite(position, ZEROES, 0, chunk);
      length -= chunk;
      position += chunk;
    }
  }


  private void ringRead(int position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.readFully(buffer, offset, count);
    } else {
      // The read overlaps the EOF.
      // # of bytes to read before the EOF.
      int beforeEof = fileLength - position;
      raf.seek(position);
      raf.readFully(buffer, offset, beforeEof);
      raf.seek(HEADER_LENGTH);
      raf.readFully(buffer, offset + beforeEof, count - beforeEof);
    }
  }


  public void add(byte[] data) throws IOException {
    add(data, 0, data.length);
  }


  public synchronized void add(byte[] data, int offset, int count) throws IOException {
    if (data == null) {
      throw new NullPointerException("data == null");
    }
    if ((offset | count) < 0 || count > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    expandIfNecessary(count);

    // Insert a new element after the current last element.
    boolean wasEmpty = isEmpty();
    int position = wasEmpty ? HEADER_LENGTH
            : wrapPosition(last.position + Element.HEADER_LENGTH + last.length);
    Element newLast = new Element(position, count);

    // Write length.
    writeInt(buffer, 0, count);
    ringWrite(newLast.position, buffer, 0, Element.HEADER_LENGTH);

    // Write data.
    ringWrite(newLast.position + Element.HEADER_LENGTH, data, offset, count);

    // Commit the addition. If wasEmpty, first == last.
    int firstPosition = wasEmpty ? newLast.position : first.position;
    writeHeader(fileLength, elementCount + 1, firstPosition, newLast.position);
    last = newLast;
    elementCount++;
    if (wasEmpty) {
      first = last; // first element
    }
  }

  private int usedBytes() {
    if (elementCount == 0) {
      return HEADER_LENGTH;
    }

    if (last.position >= first.position) {
      // Contiguous queue.
      return (last.position - first.position) // all but last entry
              + Element.HEADER_LENGTH + last.length // last entry
              + HEADER_LENGTH;
    } else {
      // tail < head. The queue wraps.
      return last.position // buffer front + header
              + Element.HEADER_LENGTH + last.length // last entry
              + fileLength - first.position;        // buffer end
    }
  }

  private int remainingBytes() {
    return fileLength - usedBytes();
  }


  public synchronized boolean isEmpty() {
    return elementCount == 0;
  }


  private void expandIfNecessary(int dataLength) throws IOException {
    int elementLength = Element.HEADER_LENGTH + dataLength;
    int remainingBytes = remainingBytes();
    if (remainingBytes >= elementLength) {
      return;
    }

    // Expand.
    int previousLength = fileLength;
    int newLength;
    // Double the length until we can fit the new data.
    do {
      remainingBytes += previousLength;
      newLength = previousLength << 1;
      previousLength = newLength;
    } while (remainingBytes < elementLength);

    setLength(newLength);

    // Calculate the position of the tail end of the data in the ring buffer
    int endOfLastElement = wrapPosition(last.position + Element.HEADER_LENGTH + last.length);

    // If the buffer is split, we need to make it contiguous
    if (endOfLastElement <= first.position) {
      FileChannel channel = raf.getChannel();
      channel.position(fileLength); // destination position
      int count = endOfLastElement - HEADER_LENGTH;
      if (channel.transferTo(HEADER_LENGTH, count, channel) != count) {
        throw new AssertionError("Copied insufficient number of bytes!");
      }
      ringErase(HEADER_LENGTH, count);
    }

    // Commit the expansion.
    if (last.position < first.position) {
      int newLastPosition = fileLength + last.position - HEADER_LENGTH;
      writeHeader(newLength, elementCount, first.position, newLastPosition);
      last = new Element(newLastPosition, last.length);
    } else {
      writeHeader(newLength, elementCount, first.position, last.position);
    }

    fileLength = newLength;
  }


  private void setLength(int newLength) throws IOException {
    // Set new file length (considered metadata) and sync it to storage.
    raf.setLength(newLength);
    raf.getChannel().force(true);
  }


  public synchronized byte[] peek() throws IOException {
    if (isEmpty()) {
      return null;
    }
    int length = first.length;
    byte[] data = new byte[length];
    ringRead(first.position + Element.HEADER_LENGTH, data, 0, length);
    return data;
  }


  @Deprecated
  public synchronized void peek(ElementReader reader) throws IOException {
    if (elementCount > 0) {
      reader.read(new ElementInputStream(first), first.length);
    }
  }


  public synchronized void peek(ElementVisitor visitor) throws IOException {
    if (elementCount > 0) {
      visitor.read(new ElementInputStream(first), first.length);
    }
  }


  @Deprecated
  public synchronized void forEach(final ElementReader reader) throws IOException {
    forEach(new ElementVisitor() {
      @Override
      public boolean read(InputStream in, int length) throws IOException {
        reader.read(in, length);
        return true;
      }
    });
  }


  public synchronized int forEach(ElementVisitor reader) throws IOException {
    int position = first.position;
    for (int i = 0; i < elementCount; i++) {
      Element current = readElement(position);
      boolean shouldContinue = reader.read(new ElementInputStream(current), current.length);
      if (!shouldContinue) {
        return i + 1;
      }
      position = wrapPosition(current.position + Element.HEADER_LENGTH + current.length);
    }
    return elementCount;
  }

  private final class ElementInputStream extends InputStream {

    private int position;
    private int remaining;

    private ElementInputStream(Element element) {
      position = wrapPosition(element.position + Element.HEADER_LENGTH);
      remaining = element.length;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      if ((offset | length) < 0 || length > buffer.length - offset) {
        throw new ArrayIndexOutOfBoundsException();
      }
      if (remaining == 0) {
        return -1;
      }
      if (length > remaining) {
        length = remaining;
      }
      ringRead(position, buffer, offset, length);
      position = wrapPosition(position + length);
      remaining -= length;
      return length;
    }

    @Override
    public int read() throws IOException {
      if (remaining == 0) {
        return -1;
      }
      raf.seek(position);
      int b = raf.read();
      position = wrapPosition(position + 1);
      remaining--;
      return b;
    }
  }


  public synchronized int size() {
    return elementCount;
  }


  public synchronized void remove() throws IOException {
    remove(1);
  }


  public synchronized void remove(int n) throws IOException {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    if (n < 0) {
      throw new IllegalArgumentException("Cannot remove negative (" + n + ") number of elements.");
    }
    if (n == 0) {
      return;
    }
    if (n == elementCount) {
      clear();
      return;
    }
    if (n > elementCount) {
      throw new IllegalArgumentException(
              "Cannot remove more elements (" + n + ") than present in queue (" + elementCount + ").");
    }

    final int eraseStartPosition = first.position;
    int eraseTotalLength = 0;

    // Read the position and length of the new first element.
    int newFirstPosition = first.position;
    int newFirstLength = first.length;
    for (int i = 0; i < n; i++) {
      eraseTotalLength += Element.HEADER_LENGTH + newFirstLength;
      newFirstPosition = wrapPosition(newFirstPosition + Element.HEADER_LENGTH + newFirstLength);
      ringRead(newFirstPosition, buffer, 0, Element.HEADER_LENGTH);
      newFirstLength = readInt(buffer, 0);
    }

    // Commit the header.
    writeHeader(fileLength, elementCount - n, newFirstPosition, last.position);
    elementCount -= n;
    first = new Element(newFirstPosition, newFirstLength);

    // Commit the erase.
    ringErase(eraseStartPosition, eraseTotalLength);
  }


  public synchronized void clear() throws IOException {
    // Commit the header.
    writeHeader(INITIAL_LENGTH, 0, 0, 0);

    // Zero out data.
    raf.seek(HEADER_LENGTH);
    raf.write(ZEROES, 0, INITIAL_LENGTH - HEADER_LENGTH);

    elementCount = 0;
    first = Element.NULL;
    last = Element.NULL;
    if (fileLength > INITIAL_LENGTH) {
      setLength(INITIAL_LENGTH);
    }
    fileLength = INITIAL_LENGTH;
  }


  @Override
  public synchronized void close() throws IOException {
    raf.close();
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName()).append('[');
    builder.append("fileLength=").append(fileLength);
    builder.append(", size=").append(elementCount);
    builder.append(", first=").append(first);
    builder.append(", last=").append(last);
    builder.append(", element lengths=[");
    try {
      forEach(new ElementReader() {
        boolean first = true;

        @Override
        public void read(InputStream in, int length) throws IOException {
          if (first) {
            first = false;
          } else {
            builder.append(", ");
          }
          builder.append(length);
        }
      });
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "read error", e);
    }
    builder.append("]]");
    return builder.toString();
  }


  static class Element {

    static final Element NULL = new Element(0, 0);


    static final int HEADER_LENGTH = 4;


    final int position;


    final int length;


    Element(int position, int length) {
      this.position = position;
      this.length = length;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "["
              + "position = " + position
              + ", length = " + length + "]";
    }
  }


  @Deprecated
  public interface ElementReader {



    void read(InputStream in, int length) throws IOException;
  }


  public interface ElementVisitor {


    boolean read(InputStream in, int length) throws IOException;
  }
}
