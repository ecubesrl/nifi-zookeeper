package it.ecubecenter.processors.zookeeper.utils;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gaido on 07/11/2016.
 */
public class ThreadsafeZookeeperClient {
    private static String UNIX_SEPARATOR = "/";
    private static int SESSION_TIMEOUT = 10000;


    private static ThreadsafeZookeeperClient instance;
    private ZooKeeper zoo;
    private String host;

    private ThreadsafeZookeeperClient(String host) {
        LoggerFactory.getLogger(this.getClass()).info("Creating a new Threadsafe Zookeeper client.");
        this.host=host;
    }

    private void connect() throws IOException, InterruptedException {

        if(zoo==null || zoo.getState() != ZooKeeper.States.CONNECTED) {
            LoggerFactory.getLogger(this.getClass()).info("Creating a new connection to Zookeeper.");
            if(zoo!=null)
                LoggerFactory.getLogger(this.getClass()).info("Previous connection state is " + zoo.getState() + ". It is alive : "+ zoo.getState().isAlive()+". It is connected : "+zoo.getState().isConnected());

            final CountDownLatch connectionLatch = new CountDownLatch(1);
            zoo = new ZooKeeper(host, SESSION_TIMEOUT, new Watcher() {

                public void process(WatchedEvent we) {

                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connectionLatch.countDown();
                    }

                }
            });

            connectionLatch.await();
        }
    }

    public synchronized void close() throws InterruptedException {
        zoo.close();
    }

    public synchronized static ThreadsafeZookeeperClient getConnection(String host){
        if(instance == null || !instance.host.equals(host)){
            instance=new ThreadsafeZookeeperClient(host);
        }
        return instance;
    }

    public synchronized byte[] readZNode(String zNode) throws KeeperException, InterruptedException, IOException {
        connect();
        Stat stats = zoo.exists(zNode, true);
        if(stats==null) return null;
        return zoo.getData(zNode, true, stats);
    }

    public synchronized List<String> listZNode(String zNode) throws KeeperException, InterruptedException, IOException {
        connect();
        if(zoo.exists(zNode,true) == null) {
            return null;
        }
        return zoo.getChildren(zNode, true);
    }

    public synchronized boolean writeZNode(String zNode, byte[] value, boolean createIfNotExists) throws KeeperException, InterruptedException, IOException {
        connect();

        Stat stats = zoo.exists(zNode, true);
        if(stats==null && createIfNotExists){
            if(!zNode.startsWith(UNIX_SEPARATOR)){
                throw new InvalidObjectException("zNode path must start with "+UNIX_SEPARATOR);
            }
            createZNodeParentsIfNeeded(zNode);
            zoo.create(zNode,value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else{
            if (stats == null) return false;
            zoo.setData(zNode,value,stats.getVersion());
        }
        return true;
    }

    public synchronized boolean createZNode(String zNode, byte[] value, boolean failIfExists) throws KeeperException, InterruptedException, IOException {
        connect();

        Stat stats = zoo.exists(zNode, true);
        if(stats!=null && failIfExists){
            return false;
        }else{
            createZNodeParentsIfNeeded(zNode);
            zoo.create(zNode,value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        return true;
    }

    private void createZNodeParentsIfNeeded(String znode) throws KeeperException, InterruptedException {

        String[] pathParts = znode.substring(1).split(UNIX_SEPARATOR);
        StringBuilder sb =  new StringBuilder();

        for(int i=0; i < pathParts.length-1; ++i){
            sb.append(UNIX_SEPARATOR).append(pathParts[i]);

            String tmpPath = sb.toString();
            if(zoo.exists(tmpPath, true) == null){
                zoo.create(tmpPath,null,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }

        }


    }


    public synchronized boolean deleteZNode(String zNode) throws IOException, InterruptedException, KeeperException {
        connect();
        Stat stats = zoo.exists(zNode, true);
        if(stats == null) {
            return false;
        }
        zoo.delete(zNode,-1);
        return true;
    }
}
