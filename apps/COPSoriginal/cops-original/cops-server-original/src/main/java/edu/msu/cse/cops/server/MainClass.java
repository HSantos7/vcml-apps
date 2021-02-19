package edu.msu.cse.cops.server;

import edu.msu.cse.dkvf.config.ConfigReader;

import java.util.Scanner;

public class MainClass {
	public static COPSServer gServer;

	public static void main(String[] args) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		gServer = new COPSServer(cnfReader);
		gServer.runAll();

		Scanner sc = new Scanner(System.in);
		String line;
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			System.exit(0);
		}
	}
}