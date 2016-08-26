package gr.phaistosnetworks.tank;

import java.util.ArrayList;

/**
 * Response data per partition / topic combo.
 */
public class TankResponse {
  TankResponse(String topic, long partition, long error) {
    messages = new ArrayList<TankMessage>();
    this.topic = topic;
    this.partition = partition;
    this.error = error;
    if (error != 0) {
      this.hasError = true;
    }
  }

  /**
   * Add a message to this partition / topic response.
   */
  void addMessage(TankMessage message) {
    messages.add(message);
  }

  /**
   * Returns the messages
   */
  public ArrayList<TankMessage> getMessages() {
    return messages;
  }

  /**
   * Returns if the response contained errors.
   */
  public boolean hasError() {
    return hasError;
  }

  /**
   * Returns the TankErrors.
   */
  public long getError() {
    return error;
  }

  /**
   * Returns the topic.
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Returns the partition.
   */
  public long getPartition() {
    return partition;
  }

  private ArrayList<TankMessage> messages;
  private String topic;
  private long partition = 0L;
  private boolean hasError = false;
  private long error = 0L;
}
