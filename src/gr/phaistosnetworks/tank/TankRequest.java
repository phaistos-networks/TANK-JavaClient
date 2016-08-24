package gr.phaistosnetworks.tank;

import java.util.HashMap;

class TankRequest {
  public TankRequest(short requestType) {
    requestTopics = new HashMap<String, HashMap<Integer, Long>>();
  }

  public void fetchTopicPartition(String topicName, int partition, long seqId) {
    if (requestTopics.containsKey(topicName) {
      requestTopics.get(topicName).put(partition, seqId);
    } else {
      requestTopic.put(topicName, new HashMap(partition, seqId));
    }
  }

  public byte[] serialize() {
  }

  private HashMap<String, HashMap<Integer, Long>> requestTopics;
}
