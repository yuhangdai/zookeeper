package com.dyh.zoo.originalapi;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

/**
 * @author bang
 * @date 2018/5/28 21:45
 */
public class AdminClient implements Watcher{

    private ZooKeeper zookeeper;
    private String connectStr;

    public AdminClient(String connectStr){
        this.connectStr = connectStr;
    }

    void startZk() throws IOException {
        zookeeper = new ZooKeeper(connectStr,15000,this);
    }

    void listState() throws KeeperException, InterruptedException {
        try{
            Stat stat = new Stat();
            byte[] masterData = zookeeper.getData("/master",false,stat);
            // 获取主节点创建时间
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master:"+new String(masterData)+" since "+startDate);
        } catch (KeeperException.NoNodeException e){
            System.out.println("No master");
        } catch (KeeperException e){
            e.printStackTrace();
        }

        System.out.println("Workers:");
        for(String w:zookeeper.getChildren("/workers",false)){
            byte[] data = zookeeper.getData("/workers/"+w,false,null);
            String state = new String(data);
            System.out.println("\t"+w+":"+state);
        }

        System.out.println("Tasks:");
        for(String s:zookeeper.getChildren("/tasks",false)){
            System.out.println("\t"+s);
        }

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        AdminClient adminClient = new AdminClient("23.106.132.161:2181,23.106.132.161:2182,23.106.132.161:2183");
        adminClient.startZk();
        adminClient.listState();
    }
}
