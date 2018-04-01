package dyh.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by bang on 2018/4/1.
 */
public class Master implements Watcher{

    private ZooKeeper zooKeeper;

    private String connectString;

    public Master(String connectString){
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,15000,this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        Master master = new Master("23.106.132.161:2181");
        master.startZk();

        Thread.sleep(120000);
    }
}

