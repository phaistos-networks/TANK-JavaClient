package gr.phaistosnetworks.tank;

/**
 * POJO for errors.
 */
public class TankError {
  /**
   * Constructs a new TankError.
   *
   * @param error can be one of TankClient.ERROR_NO_SUCH_TOPIC, TankClient.ERROR_NO_SUCH_PARTITION
   */
  TankError(String topic, long partition, long error) {
    this.topic = topic;
    this.partition = partition;
    this.error = error;
  }

  /**
   * Returns the topic.
   */
  public String getTopic() {
    return topic;
  }
  
  /**
   * Returns the partrition.
   */
  public long getPartition() {
    return partition;
  }
  
  /**
   * Returns the error.
   *
   * @return can be one of TankClient.ERROR_NO_SUCH_TOPIC, TankClient.ERROR_NO_SUCH_PARTITION
   */
  public long getError() {
    return error;
  }

  private String topic;
  private long partition = 0;
  private long error = 0;
}
