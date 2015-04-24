package deegan;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.nio.ByteBuffer;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LamportClock;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.hsqldb.SchemaManager;
import org.apache.cassandra.locator.SimpleStrategy;

import java.util.Scanner;

public class TestClient {

    private final int DEFAULT_THRIFT_PORT = 9160;
    private final String MAIN_KEYSPACE = "KeySpace1";
    private final String MAIN_COLUMN_FAMILY = "ColumnFam1";
    
    private Map<String, Integer> localServerIPAndPorts = new HashMap<String, Integer>();
    private List<Map<String, Integer>> dcToServerIPAndPorts = null;
    private ConsistencyLevel consistencyLevel;
    
    public static void main(String[] args) {
        TestClient client = new TestClient();
    }

    /**
     * Constructor
     */
    public TestClient() {
        this.setup();
        try{
        	print("yoyo");
        	this.stressTest();
        }
        catch(Exception e){
        	e.printStackTrace();
        }
    }
    
    /**
     * Prints to System out
     * @param str
     */
    private void print(String str) {
    	System.out.println(str);
    }
    
    //// Helpers 
    private static Column newColumn(String name) {
        return new Column(ByteBufferUtil.bytes(name));
    }

    private static Column newColumn(String name, String value) {
        return new Column(ByteBufferUtil.bytes(name)).setValue(ByteBufferUtil.bytes(value)).setTimestamp(0L);
    }

    private static Column newColumn(String name, String value, long timestamp) {
        return new Column(ByteBufferUtil.bytes(name)).setValue(ByteBufferUtil.bytes(value)).setTimestamp(timestamp);
    }

    private static CounterColumn newCounterColumn(String name, long value) {
        return new CounterColumn(ByteBufferUtil.bytes(name), value);
    }

    
    private void waitForKeyspacePropagation(Map<String, Integer> allServerIPAndPorts, String keyspace) throws TException
    {
        System.out.println("Waiting for key propagation...");
        for (Entry<String, Integer> ipAndPort : allServerIPAndPorts.entrySet()) {
            String ip = ipAndPort.getKey();
            Integer port = ipAndPort.getValue();

            TTransport tFramedTransport = new TFramedTransport(new TSocket(ip, port));
            TProtocol binaryProtoOnFramed = new TBinaryProtocol(tFramedTransport);
            Cassandra.Client client = new Cassandra.Client(binaryProtoOnFramed);
            tFramedTransport.open();

            // FIXME: This is a hideous way to ensure the earlier system_add_keyspace has propagated everywhere
            while(true) {
                try {
                    client.set_keyspace(keyspace, LamportClock.sendTimestamp());
                    break;
                } catch (InvalidRequestException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        //ignore
                    }
                }
            }
        }
        System.out.println("Keys propagated.");

    }
   
    /**
     * Creates a keyspace if it dosent already exist
     * @param client the cassandra thrift client
     * @param name the name of the keyspace to create
     * @throws TException
     * @throws InvalidRequestException
     */
    private void setupKeyspace(Cassandra.Iface client, String keyspace) throws TException, InvalidRequestException
    {
    	List<KsDef> yo = client.describe_keyspaces();

    	print("Current Keyspaces: -------");
    	for(KsDef def: yo){
    		print(def.name);
    		if(def.name.equals(keyspace)){
    			print(keyspace + " already exists with strategy: " + def.getStrategy_class() + "... continue");
    			return;
    		}
    	}
    	print("---------------------------");
    	
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        CfDef columnFamily = new CfDef(keyspace, MAIN_COLUMN_FAMILY);
	columnFamily.setRead_repair_chance(0.0);
        cfDefList.add(columnFamily);

        try 
        {	
        	KsDef keySpaceDefenition = new KsDef();
        	keySpaceDefenition.name = keyspace;
        	keySpaceDefenition.strategy_class = NetworkTopologyStrategy.class.getName();
        			//SimpleStrategy.class.getName();
        	if (keySpaceDefenition.strategy_options == null) 
        		keySpaceDefenition.strategy_options = new LinkedHashMap<String, String>();
        	keySpaceDefenition.strategy_options.put("DC0", "1");
        	keySpaceDefenition.strategy_options.put("DC1", "1");
        	keySpaceDefenition.cf_defs = cfDefList;

            client.system_add_keyspace(keySpaceDefenition);
            
        	print("got this far");
            int magnitude = client.describe_ring(keyspace).size();
            print("magnitude: " + magnitude);
            try
            {
                Thread.sleep(1000 * magnitude);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        catch (InvalidRequestException probablyExists) 
        {
            System.out.println("Problem creating keyspace: " + probablyExists.getMessage()); 
        	
        }
        catch (Exception e){
        	print("excpetion here now...");
        	e.printStackTrace();
        }
    }

    /**
     * Modified from cops2 unit tests
     */
    private void setup() {
    	print("setup started");
    	Integer numDatacenters = 1;
    	Integer nodesPerDatacenter = 1;
    
    	//Eiger1: 104.236.140.240 ::SFO
    	//Eiger3: 104.236.191.32 :: SFO
    	//Eiger2: 188.226.251.145 :: AMST

    	String local_ip = "188.226.251.145";

        HashMap<String, Integer> localServerIPAndPorts = new HashMap<String, Integer>();
        localServerIPAndPorts.put(local_ip, DEFAULT_THRIFT_PORT);	

    	try{
        	//Create a keyspace with a replication factor of 1 for each datacenter
        	TTransport tr = new TFramedTransport(new TSocket(local_ip, DEFAULT_THRIFT_PORT));
        	TProtocol proto = new TBinaryProtocol(tr);
        	Cassandra.Client client = new Cassandra.Client(proto);
        	tr.open();

        	this.setupKeyspace(client, MAIN_KEYSPACE);
            this.waitForKeyspacePropagation(localServerIPAndPorts, MAIN_KEYSPACE);
    	}catch(Exception c){
            System.out.println("An exception occured: " + c);
    		return;
    	}
    	
    	
        
        this.localServerIPAndPorts = localServerIPAndPorts;
        this.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        
        print("setup done");
    }
    
    private void trySomePutsAndGets() throws Exception {
        //setup the client library
    	ClientLibrary lib = new ClientLibrary(this.localServerIPAndPorts, MAIN_KEYSPACE, this.consistencyLevel);
    	//intialize some sample keys
    	ByteBuffer key1 = ByteBufferUtil.bytes("tdeegan2");
    	ByteBuffer key2 = ByteBufferUtil.bytes("ltseng3");
    	
    	String firstNameColumn = "first_name";
    	ByteBuffer firstNameColumnBuffer = ByteBufferUtil.bytes(firstNameColumn);
    	
    	ColumnParent columnParent = new ColumnParent(MAIN_COLUMN_FAMILY);
    	long timestamp = System.currentTimeMillis();


    	try{
    		lib.insert(key1, columnParent, newColumn(firstNameColumn, "thomas", timestamp));
        	lib.insert(key2, columnParent, newColumn(firstNameColumn, "lewis", timestamp));
    	}
    	catch(InvalidRequestException e){
    		print("invalid request: ");
    		e.printStackTrace();
    	}
    	print("got this far");

    	
    	ColumnPath cp = new ColumnPath(MAIN_COLUMN_FAMILY);
        cp.column = firstNameColumnBuffer;
        ColumnOrSuperColumn got1 = lib.get(key1, cp);
        ColumnOrSuperColumn got2 = lib.get(key2, cp);
        
        print("-- tdeegan2: " + new String(got1.getColumn().getValue()));
        print("-- ltseng3: " + new String(got2.getColumn().getValue()));
        
    	print("done with insert");
    }
    
    private void getCommandsFromUser() throws Exception {
    	ClientLibrary lib = new ClientLibrary(this.localServerIPAndPorts, MAIN_KEYSPACE, this.consistencyLevel);    	
    	
    	Scanner in = new Scanner(System.in);
    	while(true){
    		System.out.println("Usage: GET <key> | SET <key> <val> | EXIT");
            String s = in.nextLine();
            if(s.equals("EXIT")) return;
            if(this.executeCommand(s, lib))
            	print("Success.");
            else
            	print("Bad command. Try again");
    	}
    }
    
    private String performGet(String key, ClientLibrary lib) {
    	String firstNameColumn = "column_name";
    	ByteBuffer firstNameColumnBuffer = ByteBufferUtil.bytes(firstNameColumn);
    	try{
    		ByteBuffer keyBytes = ByteBufferUtil.bytes(key);
    		ColumnPath cp = new ColumnPath(MAIN_COLUMN_FAMILY);
            cp.column = firstNameColumnBuffer;
        	ColumnOrSuperColumn got1 = lib.get(keyBytes, cp);
    		return new String(got1.getColumn().getValue());
        }
        catch(NotFoundException e) {
        	print("not found");
        	return null;
        }
    	catch(Exception e) {
    		e.printStackTrace();
    		return null;
    	}
    }
    
    private boolean performWrite(String key, String value, ClientLibrary lib) {
    	long timestamp = System.currentTimeMillis();
    	ColumnParent columnParent = new ColumnParent(MAIN_COLUMN_FAMILY);
    	String firstNameColumn = "column_name";
    	try{
    		ByteBuffer keyBytes = ByteBufferUtil.bytes(key);
			lib.insert(keyBytes, columnParent, newColumn(firstNameColumn, value, timestamp));
		}
		catch(InvalidRequestException e){
    		print("unable to insert :(");
    		e.printStackTrace();
    		return false;
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    		return false;
    	}
    	return true;
    }
    
    private boolean executeCommand(String command, ClientLibrary lib) throws Exception {    	
    	String[] components = command.split(" ");
    	if(components.length < 2) return false;
    	if(components[0].equals("GET")){
    		if(components.length != 2) return false;
            String val = this.performGet(components[1], lib);
            print( components[1] + ": " + val);
            return true;
    	}
    	if(components[0].equals("SET")){
    		if(components.length != 3) return false;
    		return this.performWrite(components[1], components[2], lib);
    	}
    	return false;
    }
    
    private void stressTest() throws Exception {
    	HashMap<String, Integer> eiger1ServerIPAndPorts = new HashMap<String, Integer>();
    	eiger1ServerIPAndPorts.put("104.236.140.240", DEFAULT_THRIFT_PORT);	
    	ClientLibrary eiger1 = new ClientLibrary(eiger1ServerIPAndPorts, MAIN_KEYSPACE, this.consistencyLevel);
    	
    	HashMap<String, Integer> eiger2ServerIPAndPorts = new HashMap<String, Integer>();
    	eiger2ServerIPAndPorts.put("188.226.251.145", DEFAULT_THRIFT_PORT);	
    	ClientLibrary eiger2 = new ClientLibrary(eiger2ServerIPAndPorts, MAIN_KEYSPACE, this.consistencyLevel);
    	
    	HashMap<String, Integer> eiger3ServerIPAndPorts = new HashMap<String, Integer>();
    	eiger3ServerIPAndPorts.put("104.236.191.32", DEFAULT_THRIFT_PORT);	
    	ClientLibrary eiger3 = new ClientLibrary(eiger3ServerIPAndPorts, MAIN_KEYSPACE, this.consistencyLevel);
    	
    	String chanceOfWriteEnvVar = System.getenv("chance_of_write");
    	String valueSizeEnvVar = System.getenv("value_size");
    	String numOperations = System.getenv("num_operations");
    	
    	int numOps = Integer.parseInt(numOperations);
    	
    	double chanceOfWrite = 0;
    	int valueSize = 0;
        try{
        	chanceOfWrite = Double.parseDouble(chanceOfWriteEnvVar);
        	valueSize = Integer.parseInt(valueSizeEnvVar);
        }
        catch(NumberFormatException e){
        	print("Something fucked up");
        	return;
        }
        
        int topkey = 0;
    	for(int i = 0; i < numOps; i++){
    		print(((double)i/(double)numOps*100.0) + "%");
    		if(Math.random() > chanceOfWrite) {
    			// Do a get on one of the keys we wrote
    			int keyIndex = topkey - (int)(Math.random() * (topkey-1)) -1;
    			String key = "" + keyIndex;
    			String result = this.performGet(key, eiger2);
    			print("GET " + key + " =" + result);
    		}
    		else {
    			// Do a write of this new key
    			String key = "" + topkey;
    			
    			char[] chars = new char[valueSize];
    			chars[0] = '*';
    			String value = new String(chars);
    			
    			this.performWrite(key, value, eiger2);
    			print("SET key: " + key);
    			topkey++;
    		}
    	}    	
    }
}

