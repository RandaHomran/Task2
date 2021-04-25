package com.example.SampleProject.service;
import com.aerospike.client.*;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.example.SampleProject.model.Server;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class ServerService {

    AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
    List<Server> serversPool = Collections.synchronizedList(new ArrayList<>());


    public Server allocate(int memorySize) {


        serversPool=scanServersPool();
        Server server;

        // to permit two request access the array list concurrently
        synchronized (serversPool){
            server = serversPool.stream()
                    .filter(s -> (s.getFreeSize() >= memorySize && s.getState().equals("active")))
                    .findAny()
                    .orElse(null);
        }

        // if there is enough space in servers pool then allocate memory
        if(server != null) {
            server = updateServerFreeMemory(server, memorySize);
        }

        else
        {
            server = serversPool.stream()
                    .filter(s -> (s.getFreeSize() >= memorySize && s.getState().equals("creating")))
                    .findAny()
                    .orElse(null);

            // if another request come while creating a new server and there is no enough space in servers pool it will wait to make sure that the new server may have space and then allocate memory
            if(server!=null)
            {
                try {
                    Thread.sleep(6500);
                }
                catch (InterruptedException exception) {
                    exception.printStackTrace();
                }
                server= get(server.getServerId());
                server= updateServerFreeMemory(server,memorySize);
            }

            //if there is no server in creating state then create new server
            else {
                server = create(memorySize);
                try {
                    Thread.sleep(6500);
                } catch (InterruptedException exception) {
                    exception.printStackTrace();
                }
                server = updateState(server);
            }

        }
        return server;
    }

    /**
     * create a new server, save it in the data base and allocate memory
     * @param size represent the size of server to allocate
     * @return
     */
    public Server create(int size)
    {
        synchronized (this){
            int serverId=0;
            Server server;
            if (!serversPool.isEmpty()) {
                serverId = Collections.max(serversPool, Comparator.comparing(s -> s.getServerId())).getServerId() + 1;
            }
            server = new Server(100, "creating", serverId);
            WritePolicy wPolicy = new WritePolicy();
            Key key = new Key("test", "servers", server.getServerId());
            Bin bin = new Bin("server", server);
            client.put(wPolicy, key, bin);
            server = updateServerFreeMemory(server, size);
            return server;
        }

    }


    /**
     * to allocate memory from a specific server by decrement the free size for the server
     * @param server
     * @param size
     * @return
     */
    public Server updateServerFreeMemory(Server server, int size)
    {
        server.setFreeSize(server.getFreeSize()-size);
        WritePolicy wPolicy = new WritePolicy();
        Key key = new Key("test", "servers" ,server.getServerId());
        Bin bin = new Bin("server", server);
        client.put(wPolicy, key, bin);
        return server;
    }


    /**
     * change the state of a specific server to active state
     * @param server
     * @return
     */
    public Server updateState(Server server)
    {
        server.setState("active");
        WritePolicy wPolicy = new WritePolicy();
        Key key = new Key("test", "servers" ,server.getServerId());
        Bin bin = new Bin("server", server);
        client.put(wPolicy, key, bin);
        return server;
    }


    /**
     * scan all servers from database and save them in a list
     * @return List of all servers
     */
    public List<Server> scanServersPool() {
        List<Server> serversList= new ArrayList<>();
        try {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = true;
            policy.priority = Priority.LOW;
            policy.includeBinData = true;
            client.scanAll(policy, "test", "servers", (key, record) -> serversList.add((Server)record.getValue("server")));

        } catch (AerospikeException e) {
            System.out.println("EXCEPTION - Message: " + e.getMessage());
        }
        return serversList;
    }

    public int delete(int serverId)  {
        WritePolicy wPolicy = new WritePolicy();
        Key key = new Key("test", "servers" ,serverId);
        if (client.delete(wPolicy,key)){
            return serverId;
        }
        return -1;
    }


    /**
     * to return a specific server object by server id
     * @param serverId
     * @return Server object
     */
    public Server get(int serverId) {
        Key serverKey = new Key("test", "servers", serverId);
        Record serverRecord = client.get(null, serverKey);

        if(serverRecord != null) {
            return (Server) serverRecord.getValue("server");
        }
        else
            return null;
    }

}



