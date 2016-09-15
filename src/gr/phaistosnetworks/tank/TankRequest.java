package gr.phaistosnetworks.tank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;
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
      consumeRequestTopics = new HashMap<String, HashMap<Long, SimpleEntry<Long, Long>>>();
    } else if (requestType == TankClient.PUBLISH_REQ) {
      publishRequests = new HashMap<String, HashMap<Long, Bundle>>();
    } else {
      throw new TankException(
          "Request Type can only be TankClient.CONSUME_REQ or TankClient.PUBLISH_REQ");
    }
  }

  /**
   * Adds a topic, partition, seqeuenceNum combo to a CONSUME_REQ.
   *
   * @param seqNum the sequence id to request.
   *
   * @throws TankException if the request type is not CONSUME_REQ
   */
  public void consumeTopicPartition(
      String topicName,
      long partition,
      long seqNum,
      long fetchSize)
      throws TankException {

    log.fine("Adding request: " + topicName + ":" + partition + " @" + seqNum + " #" + fetchSize);

    if (requestType != TankClient.CONSUME_REQ) {
      throw new TankException("Can only add consumeTopicPartitions to CONSUME TankRequests");
    }
    if (consumeRequestTopics.containsKey(topicName)) {
      consumeRequestTopics.get(topicName).put(
          partition, 
          new SimpleEntry<Long, Long>(seqNum, fetchSize));
    } else {
      HashMap<Long, SimpleEntry<Long, Long>> toPut = new HashMap<Long, SimpleEntry<Long, Long>>();
      toPut.put(partition, new SimpleEntry<Long, Long>(seqNum, fetchSize));
      consumeRequestTopics.put(topicName, toPut);
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
      long partition,
      TankMessage message)
      throws TankException {

    if (requestType != TankClient.PUBLISH_REQ) {
      throw new TankException("Can only add publish messages to PUBLISH TankRequests");
    }
    if (publishRequests.containsKey(topicName)) {
      if (publishRequests.get(topicName).containsKey(partition)) {
        publishRequests.get(topicName).get(partition).addMsg(message);
      } else {
        publishRequests.get(topicName).put(partition, new Bundle(message));
      }
    } else {
      HashMap<Long, Bundle> toPut = new HashMap<Long, Bundle>();
      toPut.put(partition, new Bundle(message));
      publishRequests.put(topicName, toPut);
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
    HashMap<Long, SimpleEntry<Long, Long>> partitionRequest;
    for (String topic : consumeRequestTopics.keySet()) {
      baos.write(ByteManipulator.getStr8(topic));

      partitionRequest = consumeRequestTopics.get(topic);
      baos.write(ByteManipulator.serialize(partitionRequest.size(), TankClient.U8));

      for (long partition : partitionRequest.keySet()) {
        baos.write(ByteManipulator.serialize(partition, TankClient.U16));

        baos.write(ByteManipulator.serialize(
            partitionRequest.get(partition).getKey(), 
            TankClient.U64));

        baos.write(ByteManipulator.serialize(
            partitionRequest.get(partition).getValue(), 
            TankClient.U32));
      }
    }
    return baos.toByteArray();
  }

  /**
   * The serialization method for publish requests.
   */
  private byte[] serializePublishRequest() throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    HashMap<Long, Bundle> partitionRequest;
    publishRequestTopics = new ArrayList<SimpleEntry<String, Long>>();

    for (String topic : publishRequests.keySet()) {
      baos.write(ByteManipulator.getStr8(topic));

      partitionRequest = publishRequests.get(topic);
      baos.write(ByteManipulator.serialize(partitionRequest.size(), TankClient.U8));

      for (long partition : partitionRequest.keySet()) {
        baos.write(ByteManipulator.serialize(partition, TankClient.U16));
        byte[] bb = partitionRequest.get(partition).serialize();
        baos.write(ByteManipulator.getVarInt(bb.length));
        baos.write(bb);

        log.fine("Adding to request: " + topic + ":" + partition);
        publishRequestTopics.add(new SimpleEntry<String, Long>(topic, partition));
      }
    }
    return baos.toByteArray();
  }

  /**
   * Returns a list with topic, partition tuples in the same order as the request.
   */
  ArrayList<SimpleEntry<String, Long>> getTopicPartitions() {
    return publishRequestTopics;
  }

  /**
   * Return how many topics are in this request.
   */
  int getTopicsCount() {
    if (requestType == TankClient.CONSUME_REQ) {
      return consumeRequestTopics.size();
    } else {
      return publishRequests.size();
    }
  }

  long getSequenceNum(String topic, long partition) {
    return consumeRequestTopics.get(topic).get(partition).getKey();
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

        ByteArrayOutputStream allMessages = new ByteArrayOutputStream();
        byte [] messageData = new byte[0] ;
        for (TankMessage tm : messages) {
          allMessages.write(tm.serialize(false));
        }

        if (allMessages.size() > TankClient.COMPRESS_MIN_SIZE) {
          log.fine("allMessage size is " + allMessages.size() + " -> snappy compressing");
          messageData = ByteManipulator.snappyCompress(allMessages.toByteArray());
          // Set compressed flag
          flags |= 1;
        } else {
          messageData = allMessages.toByteArray();
        }

        baos.write(ByteManipulator.serialize(flags, TankClient.U8));

        if (messages.size() > TankClient.U4_MAX) {
          baos.write(ByteManipulator.getVarInt(messages.size()));
        }

        baos.write(messageData);

      } catch (IOException | TankException ioe) {
        log.log(Level.SEVERE, "ERROR serializing request bundle", ioe);
        System.exit(1);
      }
      return baos.toByteArray();
    }

    private ArrayList<TankMessage> messages;
  }



  private HashMap<String, HashMap<Long, SimpleEntry<Long, Long>>> consumeRequestTopics;
  private HashMap<String, HashMap<Long, Bundle>> publishRequests;
  private ArrayList<SimpleEntry<String, Long>> publishRequestTopics;
  private Logger log;
  private short requestType;
}
