package gr.phaistosnetworks.tank;

import java.util.ArrayList;

/**
 * Parse response data to make it usable.
 */
public class TankResponse {
  TankResponse(long requestId) {
    this.requestId = requestId;
    tankErrors = new ArrayList<TankError>();
  }

  /**
   * Adds a response from a publish request.
   */
  void addPublishResponse(String topic, long partition, long error) {
    if (error == TankClient.U8_MAX) {
      tankErrors.add(new TankError(topic, partition, TankClient.ERROR_NO_SUCH_TOPIC));
    } else if (error != 0) {
      tankErrors.add(new TankError(topic, partition, TankClient.ERROR_NO_SUCH_PARTITION));
    }
    if (error != 0) {
      this.hasErrors = true;
    }
  }

  /**
   * Returns if the response contained errors.
   */
  public boolean hasErrors() {
    return hasErrors;
  }

  /**
   * Returns the TankErrors.
   */
  public ArrayList<TankError> getErrors() {
    return tankErrors;
  }

  private ArrayList<TankError> tankErrors;
  private boolean hasErrors = false;
  private long requestId;
}
