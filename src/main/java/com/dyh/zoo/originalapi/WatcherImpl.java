package com.dyh.zoo.originalapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * zookeeper watcher实现,用于监控zookeeper客户端连接
 * @author bang
 * @date 2018/5/27 17:57
 */
public class WatcherImpl implements Watcher{

    private ZooKeeper zooKeeper;
    private String connectString;
    String serverId = Integer.toHexString(new Random().nextInt());
    static boolean isLeader = false;

    public WatcherImpl(String connectString){
        this.connectString = connectString;
    }

    public void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,30000,this);
    }

    public void stopZK() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        WatcherImpl watcher = new WatcherImpl("23.106.132.161:2181");
        watcher.startZk();
        // 竞争成为主节点
        watcher.runForMaster();
        if (isLeader){
            System.out.println("i am the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }

        watcher.stopZK();
    }


    void runForMaster() throws InterruptedException {
        while (true){
            try{
                zooKeeper.create("/master",serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException e){
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e){
                // create 方法执行且发生了ConnectionLoss，此时无法判断是否为master 因此需要checkMaster
            } catch (KeeperException e){

            }
            if (checkMaster()) break;
        }

    }

    /**
     * returns true if there is a master
     */
    boolean checkMaster() throws InterruptedException{
        while (true){
            try{
                Stat stat = new Stat();
                byte[] data = zooKeeper.getData("/master",false,stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e){
                return false;
            } catch (KeeperException.ConnectionLossException e){

            } catch (KeeperException e){

            }
        }
    }
}
