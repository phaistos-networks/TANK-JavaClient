import gr.phaistosnetworks.tank.ByteManipulator;
import gr.phaistosnetworks.tank.TankClient;
import gr.phaistosnetworks.tank.TankException;
import gr.phaistosnetworks.tank.TankMessage;
import gr.phaistosnetworks.tank.TankRequest;
import gr.phaistosnetworks.tank.TankResponse;
import gr.phaistosnetworks.tank.TankError;

import java.nio.charset.Charset;
import java.util.ArrayList;

class TestApp {
  public static void main(String[] args) throws Exception {
    byte [] foo;
    String [] testStrings = new String[4];
    testStrings[0] = "Hello World";
    testStrings[1] = "Here is the super long string that should make my str8 implementation explode. How many chars can 1 byte enumerate anyway? How about a spider bite. That has 8 legs and maybe 8 eyes. It may have spider sense too. yada yada bladi blah Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch";
    testStrings[2] = "";
    testStrings[3] = "myKey";

    for (String testString : testStrings) {
      try {
        foo = ByteManipulator.getStr8(testString);
        String myStr8 = new ByteManipulator(foo).getStr8();
        if (! myStr8.equals(testString)) {
          System.err.println("Str8 conversion is broken");
          System.err.println("Expected:" + testString + "\n but got:" + myStr8);
          System.exit(1);
        }
      } catch (TankException te) {
        System.err.println(te.getMessage());
      }
    }

    long[] testVals = new long[9];
    testVals[0] = 5L;
    testVals[1] = 180L;
    testVals[2] = 307L;
    testVals[3] = 512L;
    testVals[4] = 1790L;
    testVals[5] = 23456L;
    testVals[6] = 9990004L;
    testVals[7] = 1470905444L;
    testVals[8] = 1470905444156L;

    long testOutput;
    //Test long
    for (long testVal : testVals) {
      foo = ByteManipulator.serialize(testVal, 64);

      testOutput = new ByteManipulator(foo).deSerialize(64);
      if (testOutput != testVal) {
        System.err.println("deserialization is broken");
        System.err.println("Expected:" + testVal + " but got:" + Long.toString(testOutput));
        System.exit(1);
      }
    }

    //Test varInt
    for (long testVal : testVals) {
      try {
        foo = ByteManipulator.getVarInt(testVal);
        testOutput = new ByteManipulator(foo).getVarInt();
        if (testOutput != testVal) {
          System.err.println("Varint conversion is broken");
          System.err.println("Expected:" + testVal + "\n but got:" + testOutput);
          System.exit(1);
        }
      } catch (TankException te) {
        System.err.println(te.getMessage());
      }

    }

    String host = new String("localhost");
    String key = new String();
    int port = 11011;
    String topic = new String("foo");
    int partition = 0;
    String arg = new String();
    long id = 0;
    boolean consumate = false;
    boolean doProduce = false;
    //ArrayList<TankMessage> pushData = new ArrayList<TankMessage>();
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
        case "-k":
        case "-key":
          key = args[++i];
          break;
        case "-put":
        case "-set":
        case "-publish":
          doProduce = true;
          //pushData = new ArrayList<TankMessage>();
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
      TankResponse tr = tc.publish(publish);
      //tc.publish(topic, partition, pushData);
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

    /*
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
