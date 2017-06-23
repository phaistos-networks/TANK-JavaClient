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
    boolean doBench = false;
    boolean doReport = false;
    boolean doExit = false;
    boolean days = false;
    boolean hours = false;
    boolean mins = false;
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
        case "--topic":
          topic = args[++i];
          break;
        case "-p":
        case "-partition":
        case "--partition":
          try {
            partition = Integer.parseInt(args[++i]);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
            System.out.println("I'll give you 0 now and 0 when we reach Alderaan. Partition 0.");
          }
          break;
        case "-bench":
        case "--bench":
          doBench = true;
          break;
        case "-get":
        case "--get":
        case "-consume":
        case "--consume":
          consumate = true;
          String getArg = args[++i];
          if (getArg.substring(getArg.length() - 1).equals("h")) {
            hours = true;
          } else if (getArg.substring(getArg.length() - 1).equals("d")) {
            days = true;
          } else if (getArg.substring(getArg.length() - 1).equals("m")) {
            mins = true;
          }
          if (hours || days || mins) {
            getArg = getArg.substring(0, getArg.length() - 1);
          }
          try {
            id = Long.parseLong(getArg);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
            System.out.println("I have a baad feeling about this. Commencing from sequence 0");
          }
          break;
        case "-tail":
        case "--tail":
          consumate = true;
          id = TankClient.U64_MAX;
          break;
        case "-k":
        case "-key":
        case "--key":
          key = args[++i];
          break;
        case "-put":
        case "--put":
        case "-set":
        case "--set":
        case "-publish":
        case "--publish":
          doProduce = true;
          for (i++; i < args.length; i++) {
            publish.publishMessage(
                topic,
                partition,
                new TankMessage(key, args[i]));
          }
          break;
        case "-report":
        case "--report":
          doReport = true;
          id = TankClient.U64_MAX;
          break;
        case "-exit":
        case "--exit":
          doExit = true;
          break;
        default:
          System.out.println(" Usage options:\n"
              + "-host <hostname> -port <port> -t <topic> -p <partition>\n"
              + "-key <messageKey> -set <message1 message2 ... messagen>\n"
              + "-get <seqNum|time (e.g. 30m|h|d)> | -tail\n"
              + "-exit (to exit when reaching HighWaterMark)\n"
              + "-report (reports HighWaterMark)\n"
              );
          System.exit(0);
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


    if (doReport) {
      TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
      consume.consumeTopicPartition(topic, partition, id, fetchSize);
      List<TankResponse> response = tc.consume(consume);

      for (TankResponse tr : response) {
        System.out.println("topic: " + tr.getTopic() + " partition: " + tr.getPartition());
        System.out.println("HighWaterMark: " + tr.getHighWaterMark());
      }
    }


    if (consumate) {

      TankRequest consume = new TankRequest(TankClient.CONSUME_REQ);
      List<TankResponse> response;
      TankResponse tr0;

      if (hours || days || mins) {
        long lastTs = 0L;
        long lastSeqNum = 0L;

        while (lastTs == 0L) {
          consume.consumeTopicPartition(topic, partition, TankClient.U64_MAX, fetchSize);
          response = tc.consume(consume);
          if (response.size() > 0) {
            tr0 = response.get(0);
            fetchSize = tr0.getFetchSize();
            if (tr0.hasMessages()) {
              lastTs = tr0.getMessages().get(0).getTimestamp();
              lastSeqNum = tr0.getMessages().get(0).getSeqNum();
            }
          }
        }

        long intTs = 0L;
        long intSeqNum = 0L;
        if (lastSeqNum > 10000) intSeqNum = lastSeqNum - 10000;
        else if (lastSeqNum > 1000) intSeqNum = lastSeqNum - 1000;
        else if (lastSeqNum > 100) intSeqNum = lastSeqNum - 100;
        else if (lastSeqNum > 10) intSeqNum = lastSeqNum - 10;
        else {
          System.out.println("Not enough seqnums for calculation. Check stuff");
          System.exit(1);
        }

        while (intTs == 0L) {
          consume.consumeTopicPartition(topic, partition, intSeqNum, fetchSize);
          response = tc.consume(consume);
          fetchSize = response.get(0).getFetchSize();

          for (TankMessage tm: response.get(0).getMessages()) {
            if (tm.getSeqNum() == intSeqNum) {
              intTs = tm.getTimestamp();
            }
          }
        }
        
        float eventsPerS = 1000 * (lastSeqNum - intSeqNum) / (lastTs - intTs);
        System.out.println("Events / s: " + eventsPerS);
        System.out.println("lastSeqNum: " + lastSeqNum + " intSeqNum: " + intSeqNum);
        System.out.println("lastTs: " + lastTs + " intTs: " + intTs);

        if (mins) {
          id = lastSeqNum - (long)(id * 60 * eventsPerS);
        } else if (hours) {
          id = lastSeqNum - (long)(id * 3600 * eventsPerS);
        } else {
          id = lastSeqNum - (long)(id * 86400 * eventsPerS);
        }
      }

      long storedReq = id;
      long storedReqTime = System.currentTimeMillis();
      consume.consumeTopicPartition(topic, partition, id, fetchSize);
      long nextSeqNum = 0L;
      while (true) {
        response = tc.consume(consume);
        consume = new TankRequest(TankClient.CONSUME_REQ);

        // Exit if response is empty.
        if (response.size() == 0 ) {
          break;
        }

        for (TankResponse tr : response) {
          // System.out.println("topic: " + tr.getTopic() + " partition: " + tr.getPartition());
          if (!doBench) {
            for (TankMessage tm : tr.getMessages()) {
              System.out.println("seq: " + tm.getSeqNum()
                  + " date: " + convertTs(tm.getTimestamp())
                  + " key: " + tm.getKeyAsString()
                  + " message: " + tm.getMessageAsString());
            }
          }

          if (tr.getFetchSize() > fetchSize) {
            fetchSize = tr.getFetchSize();
          }

          if (tr.hasError() && tr.getError() == TankClient.ERROR_OUT_OF_BOUNDS) {
            if (tr.getRequestSeqNum() < tr.getFirstAvailSeqNum()) {
              nextSeqNum = tr.getFirstAvailSeqNum();
            } else if (tr.getRequestSeqNum() > tr.getHighWaterMark()) {
              nextSeqNum = tr.getHighWaterMark();
            }
          } else {
            nextSeqNum = tr.getNextSeqNum();
          }

          // Don't add new consume request if asked to exit
          if (doExit && nextSeqNum >= tr.getHighWaterMark()) {
            continue;
          }

          consume.consumeTopicPartition(
              tr.getTopic(),
              tr.getPartition(),
              nextSeqNum,
              fetchSize
          );
        }
        if (doBench) {
          if ((nextSeqNum - storedReq) > 10000) {
            long curTime = System.currentTimeMillis();
            System.out.format("%d requests in %d ms (%.0f /ms)\n", 
                nextSeqNum - storedReq,
                curTime - storedReqTime,
                (float)((nextSeqNum - storedReq) / (curTime - storedReqTime)));
            storedReq = nextSeqNum;
            storedReqTime = curTime;
          }
        }
      }
    }
  }

  private static String convertTs(long ts) {
    return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
  }

  private static long fetchSize = 1000L;
}
