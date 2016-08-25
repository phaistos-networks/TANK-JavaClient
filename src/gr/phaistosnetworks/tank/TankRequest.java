package gr.phaistosnetworks.tank;


import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

class TankRequest {
  public TankRequest(short requestType) {
    log = Logger.getLogger("tankClient");
    this.requestType = requestType;
    if (requestType == TankClient.CONSUME_REQ) {
      consumeRequestTopics = new HashMap<String, HashMap<Integer, Long>>();
    } else {
      publishRequestTopics = new HashMap<String, HashMap<Integer, Bundle>>();
    }
  }

  public void consumeTopicPartition(String topicName, int partition, long seqId) throws TankException {
    if (requestType != TankClient.CONSUME_REQ) {
      throw new TankException("Can only add consumeTopicPartitions to CONSUME TankRequests");
    }
    if (consumeRequestTopics.containsKey(topicName)) {
      consumeRequestTopics.get(topicName).put(partition, seqId);
    } else {
      consumeRequestTopics.put(topicName, new HashMap<Integer, Long>(partition, seqId));
    }
  }

  public void publishMessage(String topicName, int partition, TankMessage message) throws TankException {
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

  public byte[] serialize() throws IOException, TankException {
    if (requestType == TankClient.CONSUME_REQ) {
      return serializeFetchRequest();
    } else {
      return serializePublishRequest();
    }
  }

  private byte[] serializeFetchRequest() throws IOException, TankException {
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

  private byte[] serializePublishRequest() throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    HashMap<Integer, Bundle> partitionRequest;
    for (String topic : publishRequestTopics.keySet()) {
      baos.write(ByteManipulator.getStr8(topic));

      partitionRequest = publishRequestTopics.get(topic);
      baos.write(ByteManipulator.serialize(partitionRequest.size(), TankClient.U16));

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
   * see tank_encoding.md for chunk details.
   */
  private class Bundle {

    /**
     * constructor for new bundle.
     *
     * @param msgs the messages to be included in the bundle
     */
    private Bundle(TankMessage message) {
      this.messages = new ArrayList<TankMessage>();
      this.addMsg(message);
    }

    /**
     * add TankMessage to bundle.
     *
     * @param tm tankMessage to add
     */
    void addMsg(TankMessage message) {
      messages.add(message);
    }


    /**
     * serialize bundle into array of bytes.
     *
     * @return properly encoded byte[]
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
