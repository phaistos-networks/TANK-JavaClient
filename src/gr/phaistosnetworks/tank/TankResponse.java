package gr.phaistosnetworks.tank;

import java.util.ArrayList;
import java.util.List;

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
  public List<TankMessage> getMessages() {
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
   * Sets the highWaterMark.
   */
  void setHighWaterMark(long highWaterMark) {
    this.highWaterMark = highWaterMark;
  }

  /**
   * Returns the highWaterMark.
   */
  public long getHighWaterMark() {
    return highWaterMark;
  }

  /**
   * Sets the firstAvailSeqNum.
   */
  void setFirstAvailSeqNum(long firstAvailSeqNum) {
    this.firstAvailSeqNum = firstAvailSeqNum;
  }

  /**
   * Returns the firstAvailSeqNum.
   * This may be used in the event of an Out of bounds request.
   */
  public long getFirstAvailSeqNum() {
    return firstAvailSeqNum;
  }

  /**
   * Sets the requestSeqNum.
   * Used when requesting seqNum out of range.
   */
  void setRequestSeqNum(long seqNum) {
    this.requestSeqNum = seqNum;
  }

  /**
   * Returns the requestSeqNum.
   */
  public long getRequestSeqNum() {
    return requestSeqNum;
  }

  /**
   * Returns the sequence id for next request.
   * This will be the same as last request if no messages received.
   * This may happen is fetchSize is too small.
   * Else it will return highest messages sequence id + 1
   */
  public long getNextSeqNum() {
    if (messages.size() == 0) {
      return requestSeqNum;
    } else {
      return (messages.get(messages.size() - 1).getSeqNum()) + 1;
    }
  }

  private List<TankMessage> messages;
  private String topic;
  private long partition = 0L;
  private boolean hasError = false;
  private long error = 0L;
  private long requestSeqNum;
  private long fetchSize;
  private long highWaterMark = -1L;
  private long firstAvailSeqNum = 0L;
}
