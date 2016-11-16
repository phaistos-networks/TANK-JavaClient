package gr.phaistosnetworks.tank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;


/**
 * A tank message is an object with sequenceNum, timestamp and data.
 */
public class TankMessage {

  /**
   * Blank Constructor.
   */
  public TankMessage() {
    haveKey = false;
  }

  /**
   * Constructor with sequence id, used in consume request.
   */
  public TankMessage(long sequenceNum, long timestamp, ByteBuffer key, ByteBuffer message) {
    this();
    this.setSequenceNum(sequenceNum);
    this.setTimestamp(timestamp);
    this.setMessage(message);
    this.setKey(key);
  }

  /**
   * Constructor with sequence id, used in consume request.
   */
  public TankMessage(long sequenceNum, long timestamp, byte [] key, byte [] message) {
    this();
    this.setSequenceNum(sequenceNum);
    this.setTimestamp(timestamp);
    this.setMessage(message);
    this.setKey(key);
  }

  /**
   * Constructor with sequence id, used in consume request.
   */
  public TankMessage(long sequenceNum, long timestamp, String key, String message) {
    this();
    this.setSequenceNum(sequenceNum);
    this.setTimestamp(timestamp);
    this.setMessage(message);
    this.setKey(key);
  }

  /**
   * Constructor without sequence id used for publish requests.
   */
  public TankMessage(ByteBuffer key, ByteBuffer message) {
    this();
    this.setKey(key);
    this.setMessage(message);
  }

  /**
   * Constructor without sequence id used for publish requests.
   */
  public TankMessage(byte [] key, byte [] message) {
    this();
    this.setKey(key);
    this.setMessage(message);
  }

  /**
   * Constructor without sequence id used for publish requests.
   */
  public TankMessage(String key, String message) {
    this();
    this.setKey(key);
    this.setMessage(message);
  }

  /**
   * Constructor without sequence id or key used for publish requests.
   */
  public TankMessage(ByteBuffer message) {
    this();
    this.setMessage(message);
  }

  /**
   * Constructor without sequence id or key used for publish requests.
   */
  public TankMessage(byte [] message) {
    this();
    this.setMessage(message);
  }

  /**
   * Constructor without sequence id or key used for publish requests.
   */
  public TankMessage(String message) {
    this();
    this.setMessage(message);
  }

  /**
   * Sets the message.
   */
  public void setMessage(ByteBuffer message) {
    this.message = message;
  }

  /**
   * Sets the message.
   */
  public void setMessage(byte[] message) {
    setMessage(ByteBuffer.wrap(message));
  }

  /**
   * Sets the message.
   */
  public void setMessage(String message) {
    setMessage(ByteBuffer.wrap(message.getBytes()));
  }

  /**
   * Sets the key.
   */
  public void setKey(ByteBuffer key) {
    this.key = key;
    if (key.remaining() > 0) {
      haveKey = true;
    }
  }

  /**
   * Sets the key.
   */
  public void setKey(byte[] key) {
    setKey(ByteBuffer.wrap(key));
  }

  /**
   * Sets the key.
   */
  public void setKey(String key) {
    setKey(ByteBuffer.wrap(key.getBytes()));
  }

  /**
   * Sets the sequenceNum.
   */
  public void setSequenceNum(long sequenceNum) {
    this.seqNum = sequenceNum;
  }

  /**
   * Sets the timestamp.
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Serialize tank message into byte array.
   *
   * @param useLastTs set current timestamp or skip and use last set timestamp
   * @return serialized byte array
   */
  byte[] serialize(boolean useLastTs) throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte flags = 0;
    if (useLastTs) {
      flags |= TankClient.USE_LAST_SPECIFIED_TS;
    }
    if (haveKey) { 
      flags |= TankClient.HAVE_KEY;
    }
    baos.write(ByteManipulator.serialize(flags, TankClient.U8));

    if (!useLastTs) {
      baos.write(ByteManipulator.serialize(System.currentTimeMillis(), TankClient.U64));
    }

    if (haveKey) {
      baos.write(ByteManipulator.getStr8(key.array()));
    }

    baos.write(ByteManipulator.getVarInt(message.remaining()));
    //baos.write(message);
    while (message.hasRemaining()) {
      baos.write(message.get());
    }
    message.rewind();
    return baos.toByteArray();
  }

  /**
   * Returns the message sequence id.
   */
  public long getSeqNum() {
    return seqNum;
  }

  /**
   * Returns the message's timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the message.
   */
  public ByteBuffer getMessage() {
    return message;
  }

  /**
   * Returns the message as a byte array.
   */
  public byte [] getMessageAsArray() {
    return message.array();
  }

  /**
   * Returns the message as a String.
   */
  public String getMessageAsString() {
    return new String(message.array());
  }

  /**
   * Does this message has a key.
   */
  public boolean haveKey() {
    return haveKey;
  }

  /**
   * Returns the message's key.
   */
  public ByteBuffer getKey() {
    return key;
  }

  /**
   * Returns the message's key as a byte array.
   */
  public byte [] getKeyAsArray() {
    return key.array();
  }

  /**
   * Returns the message's key as a String.
   */
  public String getKeyAsString() {
    return new String(key.array());
  }

  private boolean haveKey;
  private long seqNum;
  private long timestamp;
  private ByteBuffer key;
  private ByteBuffer message;
}
