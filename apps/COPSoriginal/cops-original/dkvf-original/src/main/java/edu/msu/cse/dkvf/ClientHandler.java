package edu.msu.cse.dkvf;


import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.msu.cse.cops.metadata.ClientMessage;

import java.net.Socket;
import java.text.MessageFormat;
import java.util.logging.Logger;
/**
 * The handler for incoming clients
 *
 */
public class ClientHandler implements Runnable {
	/**
	 * The protocol to run its client handler upon receiving a client message.
	 */
	DKVFServer protocol;

	Socket clientSocket;
	Logger LOGGER;
	/**
	 * Constructor for ClientHandler. 
	 * @param clientSocket The Socket object of the client
	 * @param protocol The Protocol object that is used to handle client requests
	 * @param logger The logger
	 */
	public ClientHandler(Socket clientSocket, DKVFServer protocol, Logger logger) {
		this.LOGGER = logger;
		this.clientSocket = clientSocket;
		this.protocol = protocol;
	}

	/**
	 * Handles client message. It calls the protocol
	 * {@link edu.msu.cse.dkvf.DKVFServer#handleClientMessage} upon
	 * receiving a client message.
	 */
	public void run() {
		try {
			CodedInputStream in = CodedInputStream.newInstance(clientSocket.getInputStream());
			CodedOutputStream out = CodedOutputStream.newInstance(clientSocket.getOutputStream());
			while (true) {
				int size = in.readInt32();
				byte[] newMessageBytes = in.readRawBytes(size);
				ClientMessage cm = ClientMessage.parseFrom(newMessageBytes);
				if (cm == null) {
					System.out.println("Null message from client");
					LOGGER.info("Null message from client");
					protocol.decrementNumberOfClients();
					return;
				}
				//System.out.println(MessageFormat.format("New clinet message arrived:\n{0}", cm.toString()));
				LOGGER.finer(MessageFormat.format("New clinet message arrived:\n{0}", cm.toString()));
				ClientMessageAgent cma = new ClientMessageAgent(cm, out, LOGGER);
				protocol.handleClientMessage(cma);
			}
		} catch (Exception e) {
			System.out.println(Utils.exceptionLogMessge("Error in reading client message. toString: {0} Message:\n{1}", e));
			LOGGER.severe(Utils.exceptionLogMessge("Error in reading client message. toString: {0} Message:\n{1}", e));
			protocol.decrementNumberOfClients();
		}
	}
}
