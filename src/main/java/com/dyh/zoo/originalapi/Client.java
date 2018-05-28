package com.dyh.zoo.originalapi;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author bang
 * @date 2018/5/28 21:27
 */
public class Client implements Watcher{

    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private ZooKeeper zooKeeper;
    private String hostPort;

    public Client(String hostPort){
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(hostPort,15000,this);
    }

    /**
     * 添加任务
     * @param command
     * @return
     */
    String queueCommand(String command) throws InterruptedException {
        String name = "";
        while(true){
            try{
                name = zooKeeper.create("/tasks/task-",command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.NodeExistsException e){
                logger.info(name+"already appears to be running");
            } catch (KeeperException.ConnectionLossException e){

            } catch (KeeperException e){

            }

        }

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception{
        Client client = new Client("23.106.132.161:2181,23.106.132.161:2182,23.106.132.161:2183");
        client.startZk();
        client.queueCommand("Commane1");
        Thread.sleep(12000);
    }

}
