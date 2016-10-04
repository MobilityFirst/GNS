/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the Output buffer of MSocket. Data is stored in the
 * outbput buffer, before it is sent out to the other side.
 * 
 * @author aditya
 */

public class OutBuffer
{
  public static final int MAX_OUTBUFFER_SIZE = 30000000;                                   // 30MB

  ArrayList<byte[]>       sbuf               = null;

  /*
   * Same as ConnectionInfo.dataSendSeq, this is the sequence number of the next
   * byte to be sent.
   */
  long                    dataSendSeq        = 0;

  /*
   * dataBaseSeq is the sequence number of the lowest byte that has not been
   * cumulatively acknowledged by the receiver.
   */
  long                    dataBaseSeq        = 0;

  /*
   * dataStartSeq is the sequence number of first byte in the buffer. It may be
   * less than dataBaseSeq as dataStartSeq is advanced only when dataBaseSeq
   * moves beyond the first whole buffer in the buffer list sbuf.
   */
  long                    dataStartSeq       = 0;

  boolean                 Close_Obuffer      = false;                                      // indicates
                                                                                            // that
                                                                                            // outbuffer
                                                                                            // contains
                                                                                            // close
                                                                                            // mesg,
                                                                                            // actually
                                                                                            // it
                                                                                            // doesn't,
                                                                                            // flag
                                                                                            // indicates
                                                                                            // that
                                                                                            // at
                                                                                            // the
                                                                                            // end
                                                                                            // of
                                                                                            // sending
                                                                                            // all
                                                                                            // data
                                                                                            // through
                                                                                            // outbuffer
                                                                                            // also
                                                                                            // send
                                                                                            // close
                                                                                            // messgae
  boolean                 ACK_Obuffer        = false;                                      // similar
                                                                                            // for
                                                                                            // ACK

  OutBuffer()
  {
    sbuf = new ArrayList<byte[]>();
  }

  public synchronized boolean add(byte[] src, int offset, int length)
  {
    if (src.length < offset + length)
      return false;
    // FIXME: may need to improve here
    if ((getOutbufferSize() + length) > (java.lang.Runtime.getRuntime().maxMemory() / 2))
    {
      MSocketLogger.getLogger().fine("Local write fail JVM Heap memeory threshold exceeded");
      return false;
    }
    byte[] dst = null;

    dst = new byte[length];

    System.arraycopy(src, offset, dst, 0, length);
    sbuf.add(dst);
    dataSendSeq += length;
    return true;
  }

  public synchronized int getOutbufferSize()
  {
    int i = 0;
    int sizeinbytes = 0;
    for (i = 0; i < sbuf.size(); i++)
    {
      sizeinbytes += sbuf.get(i).length;
    }
    return sizeinbytes;
  }

  public boolean add(byte[] b)
  {
    return add(b, 0, b.length);
  }

  public synchronized boolean ack(long ack)
  {
    if (ack - dataBaseSeq <= 0 || ack - dataSendSeq > 0)
      return false;
    dataBaseSeq = ack;
    while ((ack - dataStartSeq > 0) && sbuf.size() > 0)
    {
      byte[] b = sbuf.get(0);
      if (ack - (dataStartSeq + b.length) >= 0)
      {
        sbuf.remove(0);
        dataStartSeq += b.length;
      }
      else
        break;
    }
    return true;
  }

  public void freeOutBuffer()
  {
    long curStart = dataStartSeq;
    int freeIndex = -1;
    for (int i = 0; i < sbuf.size(); i++)
    {
      byte[] b = sbuf.get(i);
      if (curStart + b.length - dataBaseSeq > 0)
      {
        freeIndex = i;
        break;
      }
      curStart += b.length;
    }
    dataStartSeq = curStart;

    int i = 0;
    while (i < freeIndex)
    {
      sbuf.remove(0); // remove the first element, as element slides left
      i++;
    }
  }

  public synchronized void releaseOutBuffer()
  {
    sbuf.clear();
  }

  public synchronized long getDataBaseSeq()
  {
    return dataBaseSeq;
  }

  public synchronized void setDataBaseSeq(long bs)
  {
    if ((bs - dataStartSeq >= 0) && (bs - dataSendSeq <= 0) && (bs > dataBaseSeq))
    {
      dataBaseSeq = bs;
      freeOutBuffer();
    }
  }

  public synchronized byte[] getUnacked()
  {
    if (dataSendSeq - dataBaseSeq <= 0)
      return null;
    ByteBuffer buf = ByteBuffer.wrap(new byte[(int) (dataSendSeq - dataBaseSeq)]);
    long curStart = dataStartSeq;
    for (int i = 0; i < sbuf.size(); i++)
    {
      byte[] b = sbuf.get(i);
      if (curStart + b.length - dataBaseSeq > 0)
      {
        int srcPos = (int) Math.max(0, dataBaseSeq - curStart);
        buf.put(b, srcPos, b.length - srcPos);
      }
      curStart += b.length;
    }
    if (buf.array().length == 0)
      MSocketLogger.getLogger().fine("base=" + this.dataBaseSeq + "send=" + this.dataSendSeq);
    return buf.array();
  }

  public synchronized byte[] getDataFromOutBuffer(long startSeqNum, long EndSeqNum)
  {
    if (EndSeqNum - startSeqNum <= 0)
      return null;
    ByteBuffer buf = ByteBuffer.wrap(new byte[(int) (EndSeqNum - startSeqNum)]);
    long curStart = dataStartSeq;

    for (int i = 0; i < sbuf.size(); i++)
    {
      byte[] b = sbuf.get(i);
      if (curStart + b.length - startSeqNum > 0)
      {
        int srcPos = (int) Math.max(0, startSeqNum - curStart);
        int copy = 0;
        if (buf.remaining() > (b.length - srcPos))
        {
          copy = (b.length - srcPos);
          buf.put(b, srcPos, copy);
        }
        else
        {
          copy = buf.remaining();
          buf.put(b, srcPos, copy);
          break;
        }
      }
      curStart += b.length;
    }
    if (buf.array().length == 0)
      MSocketLogger.getLogger().fine("base=" + startSeqNum + "send=" + EndSeqNum);
    return buf.array();
  }

  public String toString()
  {
    String s = "[";
    s += "dataSendSeq=" + dataSendSeq + ", ";
    s += "dataBaseSeq=" + dataBaseSeq + ", ";
    s += "dataStartSeq=" + dataStartSeq + ", ";
    s += "numbufs=" + sbuf.size();
    s += "]";

    return s;
  }

  public static void main(String[] args)
  {

    OutBuffer ob = new OutBuffer();
    byte[] b1 = "Test1".getBytes();
    byte[] b2 = "Test2".getBytes();
    ob.add(b1);
    MSocketLogger.getLogger().fine(ob.toString());
    ob.add(b2);
    MSocketLogger.getLogger().fine(ob.toString());
    ob.ack(3);
    MSocketLogger.getLogger().fine(ob.toString());
    ob.ack(4);
    MSocketLogger.getLogger().fine(ob.toString());
    MSocketLogger.getLogger().fine(new String(ob.getUnacked()));
  }
}