package gr.phaistosnetworks.tank;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import java.net.Socket;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TankClient implementation for java.
 */
public class TankClient {

  /**
   * Constructor for the TankClient object.
   * Upon initialization TankClient will attempt to connect to server.
   * Upon success, it will check for ping response from server.
   * If it's unsuccesful an error will be logged, and it will retry indefinately.
   */
  public TankClient(String tankHost, int tankPort) {
    this.tankHost = tankHost;
    this.tankPort = tankPort;
    try {
      clientId = ByteManipulator.getStr8("JC01");
    } catch (TankException | UnsupportedEncodingException te) {
      log.severe("Cannot generate default clientID str8. " + te.getMessage());
      System.exit(1);
    }
    clientReqId = 0;
    log = Logger.getLogger("tankClient");

    try {
      while (true) {
        try {
          client = new Socket(tankHost, tankPort);
        } catch (IOException ioe) {
          log.severe(ioe.getMessage());
          log.log(Level.FINEST, "ERROR opening socket", ioe);
          Thread.sleep(TankClient.RETRY_INTERVAL);
          continue;
        }
        //client.setSoTimeout(1000);
        client.setTcpNoDelay(true);
        client.setKeepAlive(true);
        client.setReuseAddress(true);
        log.config("Connected to " + client.getRemoteSocketAddress());
        log.config(" + recv buffer size: " + client.getReceiveBufferSize());
        log.config(" + send buffer size: " + client.getSendBufferSize());
        log.config(" + timeout: " + client.getSoTimeout());
        log.config(" + soLinger: " + client.getSoLinger());
        log.config(" + nodelay: " + client.getTcpNoDelay());
        log.config(" + keepalive: " + client.getKeepAlive());
        log.config(" + oobinline: " + client.getOOBInline());
        log.config(" + reuseAddress: " + client.getReuseAddress());

        bis = new BufferedInputStream(client.getInputStream());
        socketOutputStream = client.getOutputStream();
        break;
      }
      while (true) {
        if (!getPing(bis)) {
          log.severe("ERROR: No Ping Received");
        } else {
          log.fine("PING OK");
          break;
        }
      }
    } catch (IOException | InterruptedException ioe) {
      log.log(Level.SEVERE, "ERROR opening Streams", ioe);
      System.exit(1);
    }
  }

  /**
   * get a valid ping response from server.
   * 
   * @param bis BufferedInputStream to read from
   * @return true if valid ping response is received.
   */
  private boolean getPing(BufferedInputStream bis) {
    try {
      int av = bis.available();
      if (av == 0) {
        return false;
      }

      byte readByte = (byte)bis.read();
      if (readByte != PING_REQ) {
        return false;
      }
      // payload size:u32 is 0 for ping
      bis.skip(U32);
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "ERROR getting ping", ioe);
      return false;
    }
    return true;
  }

  /**
   * tries to get data from the network socket.
   * It will loop and retry if there's no data.
   * If the data is incomplete, i.e. available less than payload,
   * then it will loop and append until payload size is reached.
   */
  private List<TankResponse> poll(short requestType, TankRequest request) throws TankException {
    ByteManipulator input = new ByteManipulator(null);
    int remainder = 0;
    int toRead = 0;
    while (true) {
      int av = 0;
      try {
        av = bis.available();
      } catch (IOException ioe) {
        log.log(Level.SEVERE, "Unable to read from socket", ioe);
      }
      log.finest("bytes available: " + av);
      if (av == 0) {
        try {
          Thread.sleep(TankClient.RETRY_INTERVAL);
        } catch (InterruptedException ie) {
          log.warning("Cmon, lemme get some sleep ! " + ie.getCause());
        } finally {
          continue;
        }
      }

      if (remainder >= 0) {
        toRead = av;
      } else {
        toRead = remainder;
      }

      byte [] ba = new byte[toRead];
      try {
        bis.read(ba, 0, toRead);
      } catch (IOException ioe) {
        log.log(Level.SEVERE, "Unable to read from socket", ioe);
      }

      if (remainder > 0) {
        input.append(ba);
      } else {
        input = new ByteManipulator(ba);
      }

      byte resp = (byte)input.deSerialize(U8);
      long payloadSize = input.deSerialize(U32);

      if (resp != requestType) {
        log.severe("Bad Response type. Expected " + requestType + ", got " + resp);
        throw new TankException("Bad Response type. Expected " + requestType + ", got " + resp);
      }

      if (payloadSize > input.getRemainingLength()) {
        log.info("Received packet incomplete ");
        remainder = (int)(payloadSize - input.getRemainingLength());
        input.resetOffset();
        continue;
      } else {
        remainder = 0;
      }

      log.fine("resp: " + resp);
      log.fine("payload size: " + payloadSize);

      if (requestType == CONSUME_REQ) {
        return processConsumeResponse(input, request);
      } else if (requestType == PUBLISH_REQ) {
        return getPubResponse(input, request);
      }

      for (Handler h : log.getHandlers()) {
        h.flush();
      }
      break;
    }
    return new ArrayList<TankResponse>();
  }

  /**
   * Send consume request to broker.
   */
  public List<TankResponse> consume(TankRequest request) throws TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    log.fine("Issuing consume with " + request.getTopicsCount() + " topics");

    try {
      baos.write((byte)CONSUME_REQ);
      baos.write(ByteManipulator.serialize(0, U32));

      //clientVersion not implemented
      baos.write(ByteManipulator.serialize(0L, U16));
      baos.write(ByteManipulator.serialize(clientReqId++, U32));
      baos.write(clientId);
      baos.write(ByteManipulator.serialize(maxWait, U64));
      baos.write(ByteManipulator.serialize(minBytes, U32));

      baos.write(ByteManipulator.serialize(request.getTopicsCount(), U8));
      baos.write(request.serialize());
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "ERROR creating consume request", ioe);
      System.exit(1);
    }

    byte [] req = baos.toByteArray();
    byte [] rsize = (ByteManipulator.serialize(req.length - 5, U32));
    log.fine("consume request size is " + req.length);

    for (int i = 0; i < U32; i++) {
      req[i + 1] = rsize[i];
    }

    try {
      socketOutputStream.write(req);
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "ERROR writing publish request to socket", ioe);
      System.exit(1);
    }
    return poll(CONSUME_REQ, request);
  }

  /**
   * Processes a response from tank server after we issue a publish request.
   *
   * @param input a ByteManipulator object containing the data received from server.
   */
  private List<TankResponse> getConsumeResponse(ByteManipulator input, TankRequest request) {
    List<TankResponse> response = processConsumeResponse(input, request);
    return response;
  }

  /**
   * Processes the headers of the input received from tank server.
   *
   * @param input the ByteManipulator object that contains the bytes received from server.
   * @param topics the request topics. Used to crosscheck between request and response.
   */
  private List<TankResponse> processConsumeResponse(ByteManipulator input, TankRequest request) {
    ArrayList<Chunk> chunkList = new ArrayList<Chunk>();
    // Headers
    long headerSize  = input.deSerialize(U32);
    long requestId = input.deSerialize(U32);
    long totalTopics = input.deSerialize(U8);
    log.fine("header size: " + headerSize);
    log.fine("reqid: " + requestId);
    log.fine(String.format("topics count: %d", totalTopics));

    for (int t = 0; t < totalTopics; t++) {
      String topic = input.getStr8();
      long totalPartitions = input.deSerialize(U8);
      log.fine("topic name: " + topic);
      log.fine("Total Partitions: " + totalPartitions);

      int partition = (int)input.deSerialize(U16);
      if (partition == U16_MAX) {
        log.warning("Topic " + topic + " Not Found ");
        continue;
      } else {
        // Partitions
        for (int p = 0; p < totalPartitions; p++) {
          if (p != 0) {
            partition = (int)input.deSerialize(U16);
          }

          byte errorOrFlags = (byte)input.deSerialize(U8);
          log.fine("Partition : " + partition);
          log.fine(String.format("ErrorOrFlags : %x", errorOrFlags));

          if ((errorOrFlags & U8_MAX) == U8_MAX) {
            log.warning("Unknown Partition");
            continue;
          }

          long baseAbsSeqNum = 0L;
          if ((errorOrFlags & U8_MAX) != 0xFE) {
            baseAbsSeqNum = input.deSerialize(U64);
            log.fine("Base Abs Seq Num : " + baseAbsSeqNum);
          }

          long highWaterMark = input.deSerialize(U64);
          long chunkLength = input.deSerialize(U32);
          log.fine("High Watermark : " + highWaterMark);
          log.fine("Chunk Length : " + chunkLength);

          if (errorOrFlags == 0x1) {
            long firstAvailSeqNum = input.deSerialize(U64);
            log.warning(
                "Sequence out of bounds. Range: " 
                + firstAvailSeqNum + " - " + highWaterMark);
            continue;
          }
          chunkList.add(
              new Chunk(topic, partition, errorOrFlags, baseAbsSeqNum, highWaterMark, chunkLength));
        }
      }
    }
    return processChunks(requestId, input, request, chunkList);
  }

  /**
   * Processes the chunks of the input received from tank server.
   *
   * @param input the ByteManipulator object that contains the bytes received from server.
   * @param topics the request topics. Used to crosscheck between request and response.
   * @param chunkList the chunkList as read from headers.
   */
  private List<TankResponse> processChunks(
      long requestId,
      ByteManipulator input,
      TankRequest request,
      ArrayList<Chunk> chunkList) {

    ArrayList<TankResponse> response = new ArrayList<TankResponse>();
    for (Chunk c : chunkList) {
      long bundleLength = 0;
      long minSeqNum = 0L;
      TankResponse topicPartition = new TankResponse(c.topic, c.partition, c.errorOrFlags);
      minSeqNum = request.getSequenceId(c.topic, c.partition);
      topicPartition.setRequestSeqId(minSeqNum);
      if (c.length == 0) {
        response.add(topicPartition);
        continue;
      }

      log.finer("Remaining Length: " + input.getRemainingLength());
      try {
        bundleLength = input.getVarInt();
      } catch (ArrayIndexOutOfBoundsException aioobe) {
        log.info("Bundle length varint incomplete");
        return response;
      }
      log.finer("Bundle length : " + bundleLength);

      if (bundleLength > topicPartition.getFetchSize()) {
        topicPartition.setFetchSize(bundleLength);
      }

      if (bundleLength > input.getRemainingLength()) {
        log.fine("Bundle Incomplete (remaining bytes: " + input.getRemainingLength() + ")");
        response.add(topicPartition);
        return response;
      }
      input.flushOffset();

      byte flags = (byte)input.deSerialize(U8);
      // See TANK tank_encoding.md for flags
      long messageCount = (flags >> 2) & 0xF;
      long compressed = flags & 0x3;
      long sparse = (flags >> 6) & 0xF;
      log.finer("Bundle compressed : " + compressed);
      log.finer("Bundle SPARSE : " + sparse);

      if (messageCount == 0) {
        messageCount = input.getVarInt();
      }
      log.finer("Messages in set : " + messageCount);

      long firstMessageNum = 0L;
      long lastMessageNum = 0L;
      if (sparse == 1) {
        firstMessageNum = input.deSerialize(U64);
        log.finer("First message: " + firstMessageNum);
        if (messageCount > 1) {
          lastMessageNum = input.getVarInt() + 1 + firstMessageNum;
          log.finer("Last message: " + lastMessageNum);
        }
      } else {
        firstMessageNum = c.baseAbsSeqNum;
      }

      ByteManipulator chunkMsgs;
      if (compressed == 1) {
        log.finer("Snappy uncompression");
        try {
          chunkMsgs = new ByteManipulator(
              input.snappyUncompress(
                  bundleLength - input.getOffset()));
        } catch (IOException ioe) {
          log.log(Level.SEVERE, "ERROR uncompressing", ioe);
          return response;
        }
      } else {
        chunkMsgs = input;
      }

      long timestamp = 0L;
      long prevSeqNum = firstMessageNum;
      long curSeqNum = firstMessageNum;

      for (int i = 0; i < messageCount; i++) {
        log.finer("#### Message " + (i + 1) + " out of " + messageCount);
        flags = (byte)chunkMsgs.deSerialize(U8);
        log.finer(String.format("flags : %d", flags));

        if (sparse == 1) {
          if (i == 0) {
            //do nothing. curSeqNum is already set.
          } else if ((flags & SEQ_NUM_PREV_PLUS_ONE) != 0) {
            log.fine("SEQ_NUM_PREV_PLUS_ONE");
            curSeqNum = prevSeqNum + 1;
          } else if (i != (messageCount - 1)) {
            curSeqNum = (chunkMsgs.getVarInt() + 1 + prevSeqNum);
          } else {
            curSeqNum = lastMessageNum;
          }
          log.finer("sparse msg: " + i + " seq num: " + curSeqNum);
          prevSeqNum = curSeqNum;
        }
        log.finer("cur seq num: " + curSeqNum);

        if ((flags & USE_LAST_SPECIFIED_TS) == 0) {
          timestamp = chunkMsgs.deSerialize(U64);
          log.finer("New Timestamp : " + timestamp);
        } else {
          log.finer("Using last Timestamp : " + timestamp);
        }

        String key = new String();
        if ((flags & HAVE_KEY) != 0) {
          key = chunkMsgs.getStr8();
          log.finer("We have a key and it is : " + key);
        }

        long contentLength = chunkMsgs.getVarInt();
        log.finer("Content Length: " + contentLength);

        byte [] message = chunkMsgs.getNextBytes((int)contentLength);
        log.finest(new String(message));

        // Don't save the message if it has a sequence number lower than we requested.
        if (curSeqNum >= minSeqNum) {
          topicPartition.addMessage(new TankMessage(curSeqNum, timestamp, key.getBytes(), message));
        }
        curSeqNum++;
      }
      response.add(topicPartition);
    }
    return response;
  }


  /**
   * Send publish request to broker.
   */
  public List<TankResponse> publish(TankRequest request) throws TankException {
    ByteArrayOutputStream baos  = new ByteArrayOutputStream();
    log.info("Issuing publish with " + request.getTopicsCount() + " topics");

    try {
      baos.write((byte)PUBLISH_REQ);
      baos.write(ByteManipulator.serialize(0, U32));

      //clientVersion not implemented
      baos.write(ByteManipulator.serialize(0L, U16));
      baos.write(ByteManipulator.serialize(clientReqId++, U32));
      baos.write(clientId);

      //requestAcks not implemented
      baos.write(ByteManipulator.serialize(0L, U8));
      //ackTimeout not implemented
      baos.write(ByteManipulator.serialize(0L, U32));

      baos.write(ByteManipulator.serialize(request.getTopicsCount(), U8));
      baos.write(request.serialize());
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "ERROR creating publish request", ioe);
      System.exit(1);
    }

    byte [] req = baos.toByteArray();
    byte [] rsize = (ByteManipulator.serialize(req.length - 5, U32));
    log.fine("Publish request size is " + req.length);

    for (int i = 0; i < U32; i++) {
      req[i + 1] = rsize[i];
    }

    try {
      socketOutputStream.write(req);
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "ERROR writing publish request to socket", ioe);
      System.exit(1);
    }
    return poll(PUBLISH_REQ, request);
  }



  /**
   * Processes a response from tank server after we issue a publish request.
   *
   * @param input a ByteManipulator object containing the data received from server.
   */
  private List<TankResponse> getPubResponse(ByteManipulator input, TankRequest tr) {
    ArrayList<TankResponse> response = new ArrayList<TankResponse>();
    log.fine("Getting response for Request id: " + input.deSerialize(U32));

    long error = 0L;
    String noTopic = new String();
    for (SimpleEntry<String, Long> tuple : tr.getTopicPartitions()) {
      log.fine("Processing response for " + tuple.getKey() + ":" + tuple.getValue());
      if (tuple.getKey().equals(noTopic)) {
        //No such topic. No errors encoded for further partitions
      } else {
        error = input.deSerialize(U8);
      }

      if (error == ERROR_NO_SUCH_TOPIC) {
        noTopic = tuple.getKey();
      }

      response.add(
          new TankResponse(
              tuple.getKey(),
              tuple.getValue(),
              error));
    }
    return response;
  }

  /**
   * see tank_protocol.md for chunk details.
   */
  private class Chunk {
    /**
     * constructor.
     *
     * @param eof errorOrFlags
     * @param basn base absolute sequence number.
     * @param hwm high water mark
     */
    public Chunk(String topic, int partition, byte eof, long basn, long hwm, long length) {
      this.topic = topic;
      this.partition = partition;
      this.errorOrFlags = eof;
      this.baseAbsSeqNum = basn;
      this.highWaterMark = hwm;
      this.length = length;
    }

    private String topic;
    private long partition;
    private byte errorOrFlags;
    private long highWaterMark;
    private long length;
    private long baseAbsSeqNum;
  }

  /**
   * See TANK tank_protocol.md for maxWait semantics.
   *
   * @return the maxWait to wait for (ms) (default 5s)
   */
  public long getFetchRespMaxWaitMs() {
    return maxWait;
  }

  /**
   * See TANK tank_protocol.md for maxWait semantics.
   *
   * @param maxWaitMs the maxWait to wait for (ms)
   */
  public void setFetchRespMaxWait(long maxWaitMs) {
    this.maxWait = maxWaitMs;
  }

  /**
   * See TANK tank_protocol.md for minBytes semantics.
   *
   * @return the minBytes to wait for (default 20k)
   */
  public long getFetchRespMinBytes() {
    return minBytes;
  }

  /**
   * See TANK tank_protocol.md for minBytes semantics.
   *
   * @param byteCount the minBytes to wait for
   */
  public void setFetchRespMinBytes(long byteCount) {
    this.minBytes = byteCount;
  }

  /**
   * Set the client identification string (optional) for requests.
   * See tank_protocol.md for details
   *
   * @param clientId the client id (max 255 chars)
   */
  public void setClientId(String clientId) {
    try {
      this.clientId = ByteManipulator.getStr8(clientId);
    } catch (TankException | UnsupportedEncodingException te) {
      log.warning("Unable to set requested clientId. Ignoring." + te.getMessage());
    }
  }

  private String tankHost;
  private int tankPort;
  private int reqSeqNum;
  private Socket client;
  private BufferedInputStream bis;
  private OutputStream socketOutputStream;
  private Logger log;
  private int clientReqId;
  private long maxWait = 5000L;
  private long minBytes = 0L;
  private byte [] clientId;

  private static final long FETCH_SIZE_LEEWAY = 10000L;
  public static final int U16_MAX = 65535;
  public static final int U8_MAX = 255;
  public static final int U4_MAX = 15;

  public static final byte HAVE_KEY = 1;
  public static final byte USE_LAST_SPECIFIED_TS = 2;
  public static final byte SEQ_NUM_PREV_PLUS_ONE = 4;
  private static final long RETRY_INTERVAL = 50;

  public static final short PUBLISH_REQ = 1;
  public static final short CONSUME_REQ = 2;
  private static final short PING_REQ = 3;

  public static final byte U8 = 1;
  public static final byte U16 = 2;
  public static final byte U32 = 4;
  public static final byte U64 = 8;

  public static final long ERROR_NO_SUCH_TOPIC = 255;
  public static final long ERROR_NO_SUCH_PARTITION = 1;
  public static final long ERROR_INVALID_SEQNUM = 2;
}
