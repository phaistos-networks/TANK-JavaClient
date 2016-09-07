import gr.phaistosnetworks.tank.ByteManipulator;
import gr.phaistosnetworks.tank.TankClient;
import gr.phaistosnetworks.tank.TankException;
import gr.phaistosnetworks.tank.TankMessage;
import gr.phaistosnetworks.tank.TankRequest;
import gr.phaistosnetworks.tank.TankResponse;

import java.nio.charset.Charset;
import java.util.List;

class Tool {
  public static void main(String[] args) throws Exception {
    String host = new String("localhost");
    String key = new String();
    int port = 11011;
    String topic = new String("foo");
    int partition = 0;
    String arg = new String();
    long id = 0;
    boolean consumate = false;
    boolean doProduce = false;
    TankRequest publish = new TankRequest(TankClient.PUBLISH_REQ);

    for (int i = 0 ; i < args.length; i++) {
      arg = args[i];
      if (!arg.substring(0,1).equals("-")) {
        continue;
      }
      switch (arg) {
        case "-host":
        case "--host":
          host = args[++i];
          break;
        case "-port":
        case "--port":
          try {
            port = Integer.parseInt(args[++i]);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
            System.out.println("This is not the port you are looking for. Using default 11011");
          }
          break;
        case "-t":
        case "-topic":
          topic = args[++i];
          break;
        case "-p":
        case "-partition":
          try {
            partition = Integer.parseInt(args[++i]);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
            System.out.println("I'll give you 0 now and 0 when we reach Alderaan. Partition 0.");
          }
          break;
        case "-get":
        case "-consume":
          consumate = true;
          try {
            id = Long.parseLong(args[++i]);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
            System.out.println("I have a baad feeling about this. Commencing from sequence 0");
          }
          break;
        case "-tail":
          consumate = true;
          id = TankClient.U64_MAX;
          break;
        case "-k":
        case "-key":
          key = args[++i];
          break;
        case "-put":
        case "-set":
        case "-publish":
          doProduce = true;
          for (i++; i < args.length; i++) {
            publish.publishMessage(
                topic,
                partition,
                new TankMessage(
                    key.getBytes(),
                    args[i].getBytes(Charset.forName("UTF-8"))));
          }
          break;
        default:
          continue;
      }
    }

    TankClient tc = new TankClient(host, port);
    if (doProduce) {
      List<TankResponse> response = tc.publish(publish);
      for (TankResponse tr : response) {
        if (tr.hasError()) {
          if (tr.getError() == TankClient.ERROR_NO_SUCH_TOPIC) {
            System.out.println("Error, topic " + tr.getTopic() + " does not exist !");
          } else if (tr.getError() == TankClient.ERROR_NO_SUCH_PARTITION) {
            System.out.println(
                "Error, topic " + tr.getTopic() + " does not have a partition " + tr.getPartition());
          } else {
            System.out.println(
                "Unknown error for topic: " + tr.getTopic() + " partition: " + tr.getPartition());
          }
        }
      }
    }


    TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
    consume.consumeTopicPartition(topic, partition, id, fetchSize);
    List<TankResponse> response;

    if (consumate) {
      while (true) {
        response = tc.consume(consume);
        consume = new TankRequest(TankClient.CONSUME_REQ);
        for (TankResponse tr : response) {
          //System.out.println("topic: " + tr.getTopic() + " partition: " + tr.getPartition());
          for (TankMessage tm : tr.getMessages()) {
            System.out.println("seq: " + tm.getSeqId()
                + " ts: " + tm.getTimestamp()
                + " key: " + new String(tm.getKey())
                + " message: " + new String(tm.getMessage()));
          }
          if (tr.getFetchSize() > fetchSize) {
            fetchSize = tr.getFetchSize();
          }
          consume.consumeTopicPartition(
              tr.getTopic(),
              tr.getPartition(),
              tr.getNextSeqId(),
              fetchSize
          );
        }
      }
    }
  }

  private static long fetchSize = 20000L;
}
