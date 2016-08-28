import gr.phaistosnetworks.tank.ByteManipulator;
import gr.phaistosnetworks.tank.TankClient;
import gr.phaistosnetworks.tank.TankException;
import gr.phaistosnetworks.tank.TankMessage;
import gr.phaistosnetworks.tank.TankRequest;
import gr.phaistosnetworks.tank.TankResponse;

import java.nio.charset.Charset;
import java.util.List;

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

    TankClient tc = new TankClient("localhost", 11011);
    List<TankResponse> responses;

    TankRequest publish = new TankRequest(TankClient.PUBLISH_REQ);
    publish.publishMessage("foo", 0, new TankMessage("akey".getBytes(), "Some Random Text".getBytes()));
    publish.publishMessage("foo", 0, new TankMessage("anotherkey".getBytes(), "Some other Random Text".getBytes()));
    publish.publishMessage("foo", 0, new TankMessage("Who needs keys anyway?".getBytes()));
    publish.publishMessage("foo", 1, new TankMessage("here, have some partition magic".getBytes()));
    publish.publishMessage("foo", 1, new TankMessage("akey".getBytes(), "more partition magic".getBytes()));
    publish.publishMessage("foo", 2, new TankMessage("This one should fail".getBytes()));
    publish.publishMessage("bar", 0, new TankMessage("hello world".getBytes()));
    publish.publishMessage("randomness", 0, new TankMessage("This should fail too".getBytes()));

    if (false) {
      responses = tc.publish(publish);
      for (TankResponse tr : responses) {
        if (tr.hasError()) {
          if (tr.getError() == TankClient.ERROR_NO_SUCH_TOPIC) {
            System.out.println("Error, topic " + tr.getTopic() + " does not exist !");
          } else if (tr.getError() == TankClient.ERROR_NO_SUCH_PARTITION) {
            System.out.println("Error, topic " + tr.getTopic() + " doe not have a partition " + tr.getPartition());
          } else {
            System.out.println("Unknown error for topic: " + tr.getTopic() + " partition: " + tr.getPartition());
          }
        }
      }
    }


    TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
    consume.consumeTopicPartition("foo", 0, 0, fetchSize);
    //consume.consumeTopicPartition("foo", 1, 0, fetchSize);
    consume.consumeTopicPartition("bar", 0, 0, fetchSize);
    //consume.consumeTopicPartition("randomness", 0, 0, fetchSize);

    while (true) {
      responses = tc.consume(consume);
      consume = new TankRequest(TankClient.CONSUME_REQ);
      for (TankResponse tr : responses) {
        System.out.println("topic: " + tr.getTopic() + " partition: " + tr.getPartition());
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
        System.out.println("Next: " + tr.getTopic() + ":" + tr.getPartition() + " @" + tr.getNextSeqId() + " #"+tr.getFetchSize());
      }
    }
  }
  private static long fetchSize = 20000L;
}
