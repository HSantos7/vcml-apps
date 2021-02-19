package edu.msu.cse.cops.server;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.util.Scanner;

public class MainClass {
	public static COPSServer<String, ByteString> gServer;

	public static void main(String[] args) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		gServer = new COPSServer<>(cnfReader);
		gServer.runAll();

		Scanner sc = new Scanner(System.in);
		String line;
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			System.exit(0);
		}
	}
}