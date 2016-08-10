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

		long[] tVals = new long[7];
		tVals[0] = 5l;
		tVals[1] = 180l;
		tVals[2] = 307l;
		tVals[3] = 512l;
		tVals[4] = 1790l;
		tVals[5] = 23456l;
		tVals[6] = 9990004l;
		for (long testVal : tVals) {
			Process p = Runtime.getRuntime().exec("./varint_generator "+testVal);
			p.waitFor();
			foo = Files.readAllBytes(new File("/tmp/FOOOO").toPath());
			long varInt = new ByteManipulator(foo).getVarInt();
			if (varInt != testVal) {
				System.err.println("Varint conversion is broken");
				System.err.println("Expected "+testVal+ " but got "+varInt);
				System.exit(1);
			}
		}

		String host = new String("localhost");
		int port = 11011;
		String topic = new String("foo");
		int partition = 0;
		String arg = new String();
		long id = 0;

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
				case "-id":
				case "--id":
					try {
						id = Long.parseLong(args[++i]);
					} catch (NumberFormatException e) {
						System.err.println(e.getCause());
						System.exit(1);
					}
					break;

			}
		}

		TankClient tc = new TankClient(host, port, topic, partition);
		ArrayList<TankMessage> myData = new ArrayList<TankMessage>();
		while (true) {
			myData = tc.get(id);
			for (TankMessage tm : myData) {
				//System.out.println("seq " + tm.getSeqID() + ": " + new String(tm.getMessage()));
				id = tm.getSeqID()+1;
			}
		}
	}
}
