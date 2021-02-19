package edu.msu.cse.cops.client;

import edu.msu.cse.dkvf.config.ConfigReader;

import java.util.Scanner;

public class MainClass {
	public static void main(String[] args) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		COPSClient copsClient = new COPSClient(cnfReader);
		copsClient.runAll();
		Scanner sc;
		/*
		if(Files.exists(Paths.get("/home/santos/masterthesis/apps/COPS/cops/inputs/" + cnfReader.getConfig().getId()))) {
			try {
				sc = new Scanner(new File("/home/santos/masterthesis/apps/COPS/cops/inputs/" + cnfReader.getConfig().getId()));
				readInput(sc, copsClient);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}*/
		sc = new Scanner(System.in);
		readInput(sc, copsClient);
	}

	private static void readInput(Scanner sc, COPSClient copsClient){
		String line;
		System.out.print("-> ");
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			if (line.startsWith("put")) {
				String[] command = line.split(" ");
				if (command.length < 3){
					System.out.print("-> ");
					continue;
				}
				copsClient.put(command[1], command[2].getBytes());
			} else if (line.startsWith("get")){
				String[] command = line.split(" ");
				if (command.length < 2) {
					System.out.print("-> ");
					continue;
				}
				byte[] result = copsClient.get(command[1]);
				if (result!=null)
					System.out.println(new String(result));
			} else if (line.equals("exit")){
				System.exit(0);
			} else if (line.equals("sleep")){
				String[] command = line.split(" ");
				if (command.length < 2) {
					System.out.print("-> ");
					continue;
				}
				try {
					Thread.sleep(Long.parseLong(command[1]));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.print("-> ");
		}
	}

}