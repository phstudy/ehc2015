package org.ehc.inputv2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream.
 * Depending on the constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR),
 * or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated
 * line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MyLineReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private BufferedReader reader;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  private static final byte CR = '\r';
  private static final byte LF = '\n';

  // The line delimiter
  private final byte[] recordDelimiterBytes;

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public MyLineReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public MyLineReader(InputStream in, int bufferSize) {
    this.in = in;
    this.reader = new BufferedReader(new InputStreamReader(in));
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = null;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public MyLineReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param recordDelimiterBytes The delimiter
   */
  public MyLineReader(InputStream in, byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * given buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public MyLineReader(InputStream in, int bufferSize,
      byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>, and using a custom delimiter of array of
   * bytes.
   * @param in input stream
   * @param conf configuration
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public MyLineReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    this.in = in;
    this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }


  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  /**
   * Read one line from the InputStream into the given Text.
   *
   * @param str the object to store the given line (without newline)
   * @param maxLineLength the maximum number of bytes to store into str;
   *  the rest of the line is silently discarded.
   * @param maxBytesToConsume the maximum number of bytes to consume
   *  in this call.  This is only a hint, because if the line cross
   *  this threshold, we allow it to happen.  It can overshoot
   *  potentially by as much as one buffer length.
   *
   * @return the number of bytes read including the (longest) newline
   * found.
   *
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
      int consumedBytes = 0;
        if (this.recordDelimiterBytes != null) {

            consumedBytes = readCustomLine(str, maxLineLength, maxBytesToConsume);
            // System.out.println("xd: " + str);
            return consumedBytes;
        } else {

            for (int i = 0; i < 256; i++) {
                consumedBytes = readDefaultLine(str, maxLineLength, maxBytesToConsume);
                
                // 用 contains 要掃完全部才會停，改自己判斷比較快一些
                String s = str.toString();
                int idx = s.indexOf(";act=");
                if(idx >=0){
                    idx+=";act=".length() ;
                    if (s.charAt(idx)=='o'){
                        break;
                    }
                }
//                if (str.toString().contains(";act=order;")) {
//                    break;
//                }
            }

            return consumedBytes;
        }
  }
  
  boolean noMoreData = false;

    /**
     * Read a line terminated by one of CR, LF, or CRLF.
     */
    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        
        
        if(noMoreData){
            return 0;
        }
        
        int countDown = 1024;
        ArrayList<String> batchData = new ArrayList<String>();

        while (countDown-- > 0) {
            String input = reader.readLine();
            if (input == null) {
                noMoreData=true;
                break ;
            }
            String plist = extractValidOrder(input);
            if(plist!=null){
                batchData.add(plist);
            }
        }
        
  
        StringBuilder sb = new StringBuilder();
        sb.append(";act=order;plist=");
        for (String p : batchData) {
            sb.append(p).append(",");
        }
        sb.setLength(sb.length() - 1);
        sb.append(";");

        if(batchData.isEmpty()){
            sb.setLength(0);
            sb.append(";act=order;plist=;");
        }
 

        try {
            byte[] data = sb.toString().getBytes();
            str.append(data, 0, data.length);
            return data.length;
        } catch (Exception e) {
            return 0;
        }
  }

    protected String extractValidOrder(String input) {
        
        boolean orderAct = false;
        boolean plistWithItems = false;
        int idx;
        
        /* 不是 order act */
        idx = input.indexOf(";act=");
        if (idx >= 0) {
            idx += ";act=".length();
            if (input.charAt(idx) == 'o') {
                orderAct = true;
            }
        }
        
        if(!orderAct){
            return null;
        }

        /* 空的 plist skip */
        idx = input.indexOf(";plist=");
        if (idx >= 0) {
            idx += ";plist=".length();
            if (input.charAt(idx) != ';') {
                plistWithItems = true;
            }
        }
        
        boolean valid = orderAct && plistWithItems;
        if(!valid){
            return null;
        }
        
        int endIdx = input.indexOf(";", idx);

//        System.err.println(String.format("%s %s %s", idx, endIdx, input.substring(idx, endIdx)));
        return input.substring(idx, endIdx);
    }

  /**
   * Read a line terminated by a custom delimiter.
   */
  private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    str.clear();
    int txtLength = 0; // tracks str.getLength(), as an optimization
    long bytesConsumed = 0;
    int delPosn = 0;
    do {
      int startPosn = bufferPosn; // starting from where we left off the last
      // time
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        bufferLength = in.read(buffer);
        if (bufferLength <= 0)
          break; // EOF
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) {
        if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
          delPosn++;
          if (delPosn >= recordDelimiterBytes.length) {
            bufferPosn++;
            break;
          }
        } else {
          delPosn = 0;
        }
      }
      int readLength = bufferPosn - startPosn;
      bytesConsumed += readLength;
      int appendLength = readLength - delPosn;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (delPosn < recordDelimiterBytes.length
        && bytesConsumed < maxBytesToConsume);
    if (bytesConsumed > (long) Integer.MAX_VALUE)
      throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
    return (int) bytesConsumed;
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
}

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

}
