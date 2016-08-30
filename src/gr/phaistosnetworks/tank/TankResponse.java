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
   * Returns the messages.
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

  /**
   * Sets the fetchSize.
   * If the fetchSize is smaller than the size of the first bundle,
   * the client will get stuck in a loop requesting incomplete bundles.
   * Setting this to the largest bundle size, guarantees client will
   * get at least 1 complete bundle.
   */
  void setFetchSize(long fetchSize) {
    this.fetchSize = fetchSize;
  }

  /**
   * Gets the response fetchSize.
   * Set fetchSize of next request to minimum this amount.
   * Else client may get stuck in infinite loop receiving incomplete bundles.
   */
  public long getFetchSize() {
    return fetchSize;
  }

  /**
   * Not used.
   */
  void setRequestSeqId(long seqId) {
    this.requestSeqId = seqId;
  }

  /**
   * Returns the sequence id for next request.
   * This will be the same as last request if no messages received.
   * This may happen is fetchSize is too small.
   * Else it will return highest messages sequence id + 1
   */
  public long getNextSeqId() {
    if (messages.size() == 0) {
      return requestSeqId;
    } else {
      return (messages.get(messages.size() - 1).getSeqId()) + 1;
    }
  }

  private ArrayList<TankMessage> messages;
  private String topic;
  private long partition = 0L;
  private boolean hasError = false;
  private long error = 0L;
  private long requestSeqId;
  private long fetchSize;
}
