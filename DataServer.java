import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataServer {
    private static int dataStore; // Primary data store
    private static int numBackups = 0;
    private static ArrayList<Integer> backupDataStore = new ArrayList<Integer>();
    private static HashMap<Integer, Integer> backupServers = new HashMap<>();
    private static final Object lock = new Object(); // To ensure sequential consistency

    public static void main(String[] args) throws IOException {
        if (args.length == 1) {
            int primaryPort = Integer.parseInt(args[0]);
            startPrimaryServer(primaryPort);
        } else if (args.length == 2) {
            int backupPort = Integer.parseInt(args[0]);
            int primaryPort = Integer.parseInt(args[1]);
            startBackupServer(backupPort, primaryPort);
        } else {
            System.out.println("Invalid arguments.");
        }
    }

    /** PRIMARY SERVER CODE **/
    /**
     * 
     * @param primaryPort
     * @throws IOException
     */
    public static void startPrimaryServer(int primaryPort) throws IOException {
        System.out.println("I am the primary!");
        // 1 - Set up primary data store, defaults to zero
        dataStore = 0;

        // 2 - Create TCP server socket
        ServerSocket serverSocket = new ServerSocket(primaryPort);
        System.out.println("Data Server is listening on port " + primaryPort);

        // 3 - Wait at port for requests
        while (true) {
            Socket requestingSocket = serverSocket.accept();
            new Thread(() -> handlePrimaryRequest(requestingSocket)).start();
        }
    }

    /**
     * 
     * @param clientSocket
     */
    private static void handlePrimaryRequest(Socket requestingSocket) {
        try {
            InputStream input = requestingSocket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            PrintWriter writer = new PrintWriter(requestingSocket.getOutputStream(), true);
            String request = reader.readLine();
            System.out.println("Received request: " + request);

            /* Read Request */
            if (request.startsWith("READ")) {
                synchronized (lock) {
                    writer.println("COMPLETE_READ: " + dataStore);
                }
            } 

            /* Write Request */
            else if (request.startsWith("WRITE:")) {
                // 1 - Update own data store replica
                int newValue = Integer.parseInt(request.split(":")[1]);
                
                //synchronized (lock) {
                    dataStore = newValue;
                    // 2 - Update backup servers
                    for (Integer port : backupServers.values()) {
                        Socket backupSocket = new Socket("localhost", port);
                        PrintWriter backupWriter = new PrintWriter(backupSocket.getOutputStream(), true);
                        backupWriter.println("UPDATE:" + newValue);
                    }

                    // 3 - Reply to client
                    writer.println("COMPLETE_WRITE");
                //}
            } 

            /* Join Request */
            else if (request.startsWith("JOIN:")) {
                // 1 - Record backup's port number
                int backupPort = Integer.parseInt(request.split(":")[1]);
                numBackups++;
                backupServers.put(numBackups, backupPort);
                
                // 2 - Send acknowledgement
                writer.println("COMPLETE_JOIN");
            } 

            /* Update Request */
            else if (request.startsWith("UPDATE:")) {
                // Get the new data store value
                int newValue = Integer.parseInt(request.split(":")[1]);

                // 1 - Have all backups update
                //synchronized (lock) {
                    dataStore = newValue;
                    // Propagate to all backups
                    for (Integer port : backupServers.values()) {
                        Socket backupSocket = new Socket("localhost", port);
                        PrintWriter backupWriter = new PrintWriter(backupSocket.getOutputStream(), true);
                        backupWriter.println("UPDATE:" + newValue);
                    }
                //}

                // 2 - Send acknowledgement to requesting backup server
                writer.println("COMPLETE_UPDATE");
            } 
            
            /* Unknown Commands */
            else {
                writer.println("ERROR: Unknown command");
            }
        } 
        /* Exceptions */
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    /** BACKUP SERVER CODE **/
    /**
     * 
     * @param backupPort
     * @param primaryPort
     * @throws IOException
     */
    public static void startBackupServer(int backupPort, int primaryPort) throws IOException {
        // Set up writer to primary server
        System.out.println("I am a backup with port: " + backupPort);
        Socket primarySocket = new Socket("localhost", primaryPort);
        PrintWriter primaryWriter = new PrintWriter(primarySocket.getOutputStream(), true);

        // 1 - Send join request
        primaryWriter.println("JOIN:" + backupPort);

        // 2 - Set up backup replica of data store
        int i = getKey(backupServers, backupPort);
        backupDataStore.add(i, 0);

        // Wait for response from join request
        InputStream input = primarySocket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String joinResponse = reader.readLine();
        System.out.println(joinResponse);

        // After acknowledged, set up the backup server
        ServerSocket backupServerSocket = new ServerSocket(backupPort);
        System.out.println("Data Server is listening on port " + backupPort);

        // 3 - Wait at port for requests
        while (true) {
            Socket clientSocket = backupServerSocket.accept();
            new Thread(() -> handleBackupRequest(clientSocket, primarySocket, backupPort)).start();
        }
    }

    /**
     * 
     * @param clientSocket
     * @param primarySocket
     * @param backupSocket The current backup
     */
    private static void handleBackupRequest(Socket clientSocket, Socket primarySocket, int backupPort) {
        try {
            // Setup reader and writer for client
            InputStream input = clientSocket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            String request = reader.readLine();

            // Get the current backup server's index/key
            int key = getKey(backupServers, backupPort);

            /* Read Request */
            if (request.startsWith("READ")) {
                synchronized (lock) {
                    writer.println("COMPLETE_READ: " + backupDataStore.get(key));
                }
            } 

            /* Write Request */
            else if (request.startsWith("WRITE:")) {
                // Get new value 
                int newValue = Integer.parseInt(request.split(":")[1]);

                // 1 - Send UPDATE to primary server 
                PrintWriter primaryWriter = new PrintWriter(primarySocket.getOutputStream(), true);
                primaryWriter.println("UPDATE:" + newValue);

                // 1 - Wait for response from server
                input = primarySocket.getInputStream();
                BufferedReader primaryReader = new BufferedReader(new InputStreamReader(input));
                String updateResponse = primaryReader.readLine();
                System.out.println(updateResponse);

                // 2 - Send acknowledge to client
                writer.println("COMPLETE_WRITE");
            } 
            
            /* Update Request */
            else if (request.startsWith("UPDATE:")) {
                int newValue = Integer.parseInt(request.split(":")[1]);
                
                // Update data store replica
                //synchronized (lock) {
                    backupDataStore.set(key, newValue);
                //}

                // Send acknowledgement
                writer.println("COMPLETE_UPDATE");
            }
        } 
        /* Exceptions */
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the key associated with a given value
     * Source: https://www.baeldung.com/java-map-key-from-value
     */
    private static int getKey(Map<Integer, Integer> map, Integer value) {
        return map
          .entrySet()
          .stream()
          .filter(entry -> value.equals(entry.getValue()))
          .map(Map.Entry::getKey)
          .findFirst().get();
    }
}
