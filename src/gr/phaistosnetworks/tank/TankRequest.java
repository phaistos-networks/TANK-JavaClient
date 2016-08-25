package gr.phaistosnetworks.tank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Request to publish or consume.
 */
public class TankRequest {

  /**
   * Constructor.
   *
   * @param requestType can be TankClient.PUBLISH_REQ or TankClient.CONSUME_REQ
   *
   * @throws TankException if any other requestType is given.
   */
  public TankRequest(short requestType) throws TankException {
    log = Logger.getLogger("tankClient");
    this.requestType = requestType;
    if (requestType == TankClient.CONSUME_REQ) {
      consumeRequestTopics = new HashMap<String, HashMap<Integer, Long>>();
    } else if (requestType == TankClient.PUBLISH_REQ) {
      publishRequestTopics = new HashMap<String, HashMap<Integer, Bundle>>();
    } else {
      throw new TankException(
          "Request Type can only be TankClient.CONSUME_REQ or TankClient.PUBLISH_REQ");
    }
  }

  /**
   * Adds a topic, partition, seqeuenceId combo to a CONSUME_REQ.
   *
   * @param seqId the sequence id to request.
   *
   * @throws TankException if the request type is not CONSUME_REQ
   */
  public void consumeTopicPartition(
      String topicName,
      int partition,
      long seqId)
      throws TankException {

    if (requestType != TankClient.CONSUME_REQ) {
      throw new TankException("Can only add consumeTopicPartitions to CONSUME TankRequests");
    }
    if (consumeRequestTopics.containsKey(topicName)) {
      consumeRequestTopics.get(topicName).put(partition, seqId);
    } else {
      consumeRequestTopics.put(topicName, new HashMap<Integer, Long>(partition, seqId));
    }
  }

  /**
   * Adds a topic, partition, TankMessage combo to be published.
   *
   * @param message the TankMessage to be published
   *
   * @throws TankException if the request type is not PUBLISH_REQ
   */
  public void publishMessage(
      String topicName,
      int partition,
      TankMessage message)
      throws TankException {

    if (requestType != TankClient.PUBLISH_REQ) {
      throw new TankException("Can only add publish messages to PUBLISH TankRequests");
    }
    if (publishRequestTopics.containsKey(topicName)) {
      if (publishRequestTopics.get(topicName).containsKey(partition)) {
        publishRequestTopics.get(topicName).get(partition).addMsg(message);
      } else {
        publishRequestTopics.get(topicName).put(partition, new Bundle(message));
      }
    } else {
      HashMap<Integer, Bundle> toPut = new HashMap<Integer, Bundle>();
      toPut.put(partition, new Bundle(message));
      publishRequestTopics.put(topicName, toPut);
    }
  }

  /**
   * Serializes the current TankRequest into a byte array,
   * suitable for sending to the TANK broker.
   *
   * @return the serialized byte array to be sent.
   */
  byte[] serialize() throws IOException, TankException {
    if (requestType == TankClient.CONSUME_REQ) {
      return serializeConsumeRequest();
    } else {
      return serializePublishRequest();
    }
  }

  /**
   * The serialization method for consume requests.
   *
   * @return the serialized data
   */
  private byte[] serializeConsumeRequest() throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    HashMap<Integer, Long> partitionRequest;
    for (String topic : consumeRequestTopics.keySet()) {
      baos.write(ByteManipulator.getStr8(topic));

      partitionRequest = consumeRequestTopics.get(topic);
      baos.write(ByteManipulator.serialize(partitionRequest.size(), TankClient.U8));

      for (int partition : partitionRequest.keySet()) {
        baos.write(ByteManipulator.serialize(partition, TankClient.U16));
        baos.write(ByteManipulator.serialize(partitionRequest.get(partition), TankClient.U64));
        baos.write(ByteManipulator.serialize(fetchSize, TankClient.U32));
      }
    }
    return baos.toByteArray();
  }

  /**
   * The serialization method for publish requests.
   */
  private byte[] serializePublishRequest() throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    HashMap<Integer, Bundle> partitionRequest;
    for (String topic : publishRequestTopics.keySet()) {
      baos.write(ByteManipulator.getStr8(topic));

      partitionRequest = publishRequestTopics.get(topic);
      baos.write(ByteManipulator.serialize(partitionRequest.size(), TankClient.U8));

      for (int partition : partitionRequest.keySet()) {
        baos.write(ByteManipulator.serialize(partition, TankClient.U16));
        byte[] bb = partitionRequest.get(partition).serialize();
        baos.write(ByteManipulator.getVarInt(bb.length));
        baos.write(bb);
      }
    }
    return baos.toByteArray();
  }

  /**
   * Return how many topics are in this request.
   */
  int getTopicsCount() {
    if (requestType == TankClient.CONSUME_REQ) {
      return consumeRequestTopics.size();
    } else {
      return publishRequestTopics.size();
    }
  }

  /**
   * See tank_encoding.md for bundle details.
   */
  private class Bundle {

    /**
     * Constructor for new empty bundle.
     */
    private Bundle() {
      this.messages = new ArrayList<TankMessage>();
    }

    /**
     * Constructor for new bundle.
     *
     * @param message the first message to be included in the bundle
     */
    private Bundle(TankMessage message) {
      this.messages = new ArrayList<TankMessage>();
      this.addMsg(message);
    }

    /**
     * Add TankMessage to bundle.
     */
    void addMsg(TankMessage message) {
      messages.add(message);
    }

    /**
     * Serialize bundle into array of bytes.
     */
    public byte[] serialize() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        byte flags = 0;
        if (messages.size() <= TankClient.U4_MAX) {
          flags |= (messages.size() << 2);
        }

        baos.write(ByteManipulator.serialize(flags, TankClient.U8));

        if (messages.size() > TankClient.U4_MAX) {
          baos.write(ByteManipulator.getVarInt(messages.size()));
        }

        for (TankMessage tm : messages) {
          baos.write(tm.serialize(false));
        }
      } catch (IOException | TankException ioe) {
        log.log(Level.SEVERE, "ERROR serializing request bundle", ioe);
        System.exit(1);
      }
      return baos.toByteArray();
    }

    private ArrayList<TankMessage> messages;
  }

  private HashMap<String, HashMap<Integer, Long>> consumeRequestTopics;
  private HashMap<String, HashMap<Integer, Bundle>> publishRequestTopics;
  private long fetchSize = 20000L;
  private Logger log;
  private short requestType;
}
