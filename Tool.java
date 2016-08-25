import gr.phaistosnetworks.tank.ByteManipulator;
import gr.phaistosnetworks.tank.TankClient;
import gr.phaistosnetworks.tank.TankException;
import gr.phaistosnetworks.tank.TankMessage;
import gr.phaistosnetworks.tank.TankRequest;
import gr.phaistosnetworks.tank.TankResponse;
import gr.phaistosnetworks.tank.TankError;

import java.nio.charset.Charset;
import java.util.ArrayList;

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
    ArrayList<TankMessage> pushData = new ArrayList<TankMessage>();

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
        /*
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
        case "-k":
        case "-key":
          key = args[++i];
          break;
        case "-put":
        case "-set":
        case "-publish":
          doProduce = true;
          pushData = new ArrayList<TankMessage>();
          for (i++; i < args.length; i++) {
            pushData.add(
                new TankMessage(
                    key.getBytes(),
                    args[i].getBytes(Charset.forName("UTF-8"))));
          }
          break;
          */
        default:
          continue;
      }
    }


    TankRequest publish = new TankRequest(TankClient.PUBLISH_REQ);
    publish.publishMessage("foo", 0, new TankMessage("myKey".getBytes(), "my message".getBytes()));
    publish.publishMessage("foo", 0, new TankMessage("myOtherKey".getBytes(), "other key".getBytes()));
    publish.publishMessage("foo", 0, new TankMessage("my keyless message".getBytes()));
    publish.publishMessage("foo", 0, new TankMessage("myKey".getBytes(), "Same key, new message".getBytes()));
    publish.publishMessage("foo", 1, new TankMessage("partition does not exist".getBytes()));
    publish.publishMessage("bar", 0, new TankMessage("This topic does not exist".getBytes()));

    TankClient tc = new TankClient(host, port);
    if (false) {
      TankResponse tr = tc.publish(publish);
      if (tr.hasErrors()) {
        for (TankError te : tr.getErrors()) {
          if (te.getError() == TankClient.ERROR_NO_SUCH_TOPIC) {
            System.out.println("Error, topic " + te.getTopic() + " does not exist !");
          } else if (te.getError() == TankClient.ERROR_NO_SUCH_PARTITION) {
            System.out.println("Error, topic " + te.getTopic() + " doe not have a partition " + te.getPartition());
          } else {
            System.out.println("Unknown error for topic: " + te.getTopic() + " partition: " + te.getPartition());
          }
        }
      }
    }

    TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
    consume.consumeTopicPartition("foo", 0, 0);
    consume.consumeTopicPartition("foo", 0, 60);
    consume.consumeTopicPartition("foo", 1, 0);
    consume.consumeTopicPartition("bar", 0, 0);
    TankResponse tr = tc.consume(consume)
    /*
    if (doProduce) {
      tc.publish(topic, partition, pushData);
    }

    ArrayList<TankMessage> data = new ArrayList<TankMessage>();
    if (consumate) {
      while (true) {
        data = tc.consume(topic, partition, id);
        for (TankMessage tm : data) {
          System.out.println("seq: " + tm.getSeqId()
              + " ts: " + tm.getTimestamp()
              + " key: " + new String(tm.getKey())
              + " message: " + new String(tm.getMessage()));
          id = tm.getSeqId() + 1;
        }
      }
    }
    */
  }
}
