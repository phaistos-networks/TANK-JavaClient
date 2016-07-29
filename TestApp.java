class TestApp {
	public static void main(String[] args) {

		byte foo[] = new ByteManipulator().getStr8("Hello World");
		String myStr8 = new ByteManipulator(foo).getStr8();
		if (! myStr8.equals("Hello World")) {
			System.err.println("Str8 conversion is broken");
			System.exit(1);
		}
			
		
		TankClient tc = new TankClient("localhost", 11011, "foo", 0);
                new Thread(tc).start();
	}
}
