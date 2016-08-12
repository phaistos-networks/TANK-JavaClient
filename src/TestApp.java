import java.nio.file.*;
import java.nio.charset.*;
import java.io.*;
import java.util.ArrayList;

class TestApp {
    public static void main(String[] args) throws Exception {
        /*
           byte foo[] = new ByteManipulator().getStr8("Hello World");
           String myStr8 = new ByteManipulator(foo).getStr8();
           if (! myStr8.equals("Hello World")) {
           System.err.println("Str8 conversion is broken");
           System.exit(1);
           }

           long[] tVals = new long[9];
           tVals[0] = 5l;
           tVals[1] = 180l;
           tVals[2] = 307l;
           tVals[3] = 512l;
           tVals[4] = 1790l;
           tVals[5] = 23456l;
           tVals[6] = 9990004l;
           tVals[7] = 1470905444l;
           tVals[8] = 1470905444156l;


           long testOP;
        //Test long
        for (long testVal : tVals) {
        foo = new ByteManipulator().serialize(testVal, 64);

        testOP = new ByteManipulator(foo).deSerialize(64);
        if (testOP != testVal) {
        System.err.println("deserialization is broken");
        System.err.println("Expected "+testVal+ " but got "+Long.toString(testOP));
        System.exit(1);
        }
        }

        //Test varInt
        for (long testVal : tVals) {
        if (testVal > 4294967295l)
        continue;
        foo = new ByteManipulator().getVarInt(testVal);
        testOP = new ByteManipulator(foo).getVarInt();
        if (testOP != testVal) {
        System.err.println("Varint conversion is broken");
        System.err.println("Expected "+testVal+ " but got "+testOP);
        System.exit(1);
        }

        }
         */



        String host = new String("localhost");
        int port = 11011;
        String topic = new String("foo");
        int partition = 0;
        String arg = new String();
        long id = 0;
        boolean consumate = false, doProduce = false;
        ArrayList<byte[]> pushData = new ArrayList<byte[]>();

        for (int i = 0 ; i < args.length; i++) {
            arg = args[i];
            if (!arg.substring(0,1).equals("-"))
                continue;
            switch (arg) {
                case "-host":
                case "--host":
                    host = args[++i];
                    break;
                case "-port":
                case "--port":
                    try { 
                        port = Integer.parseInt(args[++i]); 
                    } catch (Exception e) {
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
                    } catch (Exception e) {
                        System.out.println("I'll give you 0 now and 0 when we reach Alderaan. Partition 0 eh ?");
                    }
                    break;
                case "-get":
                case "-consume":
                    consumate = true;
                    try {
                        id = Long.parseLong(args[++i]);
                    } catch (Exception e) {
                        System.out.println("I have a baad feeling about this. Commencing from sequence 0");
                    }
                    break;
                case "-put":
                case "-set":
                case "-publish":
                    doProduce = true;
                    pushData = new ArrayList<byte[]>();
                    for (i++;i<args.length;i++) {
                        pushData.add(args[i].getBytes(Charset.forName("UTF-8")));
                    }
                    break;
            }
        }

        TankClient tc = new TankClient(host, port);
        if (doProduce)
            tc.publish(topic, partition, pushData);

        ArrayList<TankMessage> data = new ArrayList<TankMessage>();
        if (consumate) {
            while (true) {
                data = tc.consume(topic, partition, id);
                for (TankMessage tm : data) {
                    System.out.println("seq: " + tm.getSeqID() + " ts: "+tm.getTimestamp()+" message: " + new String(tm.getMessage()));
                    id = tm.getSeqID()+1;
                }
                Thread.sleep(50);
            }
        }
    }
}
