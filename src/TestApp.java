class TestApp {
	public static void main(String[] args) {

		byte foo[] = new ByteManipulator().getStr8("Hello World");
		String myStr8 = new ByteManipulator(foo).getStr8();
		if (! myStr8.equals("Hello World")) {
			System.err.println("Str8 conversion is broken");
			System.exit(1);
		}


		String host = new String("localhost");
		int port = 11011;
		String topic = new String("foo");
		int partition = 0;
		String arg = new String();
		int id = 0;

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
					try {
						id = Integer.parseInt(args[++i]);
					} catch (NumberFormatException e) {
						System.err.println(e.getCause());
						System.exit(1);
					}
					break;

			}
		}
			
		TankClient tc = new TankClient(host, port, topic, partition, id);
                new Thread(tc).start();
	}
}
