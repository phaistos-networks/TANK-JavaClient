import java.nio.file.*;
import java.io.*;
import java.util.ArrayList;

class TestApp {
	public static void main(String[] args) throws Exception {

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



		String host = new String("localhost");
		int port = 11011;
		String topic = new String("foo");
		int partition = 0;
		String arg = new String();
		long id = 0;
		boolean consumate = false, doProduce = false;
		ArrayList<String> pushData = new ArrayList<String>();

		for (int i = 0 ; i < args.length; i++) {
			arg = args[i];
			if (!arg.substring(0,1).equals("-"))
				continue;
			switch (arg) {
				case "--host":
					host = args[++i];
					break;
				case "--port":
					try { 
						port = Integer.parseInt(args[++i]); 
					} catch (NumberFormatException e) {
						System.err.println(e.getCause());
						System.exit(1);
					}
					break;
				case "-t":
				case "--topic":
					topic = args[++i];
					break;
				case "-p":
				case "--partition":
					try {
						partition = Integer.parseInt(args[++i]);
					} catch (NumberFormatException e) {
						System.err.println(e.getCause());
						System.exit(1);
					}
					break;
				case "-get":
				case "--get":
				case "-consume":
				case "--consume":
					consumate = true;
					try {
						id = Long.parseLong(args[++i]);
					} catch (NumberFormatException e) {
						System.err.println(e.getCause());
						System.exit(1);
					}
					break;
				case "-put":
				case "--put":
				case "-produce":
				case "--produce":
					doProduce = true;
					pushData = new ArrayList<String>();
					for (;i<args.length;i++) {
						pushData.add(args[i]);
					}
					break;
			}
		}

		TankClient tc = new TankClient(host, port, topic, partition);
		if (doProduce)
			tc.publish(pushData);

		if (consumate) {
			ArrayList<TankMessage> myData = new ArrayList<TankMessage>();
			while (true) {
				myData = tc.consume(id);
				for (TankMessage tm : myData) {
					System.out.println("seq: " + tm.getSeqID() + " ts: "+tm.getTimestamp()+" message: " + new String(tm.getMessage()));
					id = tm.getSeqID()+1;
				}
			}
		}
	}
}
