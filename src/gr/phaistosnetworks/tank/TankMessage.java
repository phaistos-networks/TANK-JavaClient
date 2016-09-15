package gr.phaistosnetworks.tank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * A tank message is an object with sequenceNum, timestamp and data.
 */
public class TankMessage {

  /**
   * Constructor with sequence id, used in consume request.
   */
  public TankMessage(long sequenceNum, long timestamp, byte[] key, byte[] message) {
    this.seqNum = sequenceNum;
    this.timestamp = timestamp;
    this.message = message;
    this.key = key;

    if (key.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
  }

  /**
   * Constructor without sequence id used for publish requests.
   */
  public TankMessage(byte[] key, byte[] message) {
    this.key = key;
    if (key.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
    this.message = message;
  }

  /**
   * Constructor without sequence id or key used for publish requests.
   */
  public TankMessage(byte[] message) {
    haveKey = false;
    this.message = message;
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
      baos.write(ByteManipulator.getStr8(key));
    }

    baos.write(ByteManipulator.getVarInt(message.length));
    baos.write(message);
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
  public byte[] getMessage() {
    return message;
  }

  /**
   * Does if this message has a key.
   */
  public boolean haveKey() {
    return haveKey;
  }

  /**
   * Returns the message's key.
   */
  public byte[] getKey() {
    return key;
  }

  private boolean haveKey;
  private long seqNum;
  private long timestamp;
  private byte[] key;
  private byte[] message;
}
