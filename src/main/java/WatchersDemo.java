import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class WatchersDemo implements Watcher {
    // Variables Required to process ZooKeeper instance.
    private static final String ZOOKEEPER_ADRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String TARGET_ZNODE = "/target_znode";

    // Initiate ZooKeeper instance on main thread.
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatchersDemo watchersDemo = new WatchersDemo();
        watchersDemo.connectToZooKeeper();
        watchersDemo.watchTargetZnode();
        watchersDemo.run();
        watchersDemo.close();
        System.out.println("Disconnected from ZooKeeper, exiting application");
    }

    public void connectToZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        // Keep main thread wait for events threads to process.
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException{
        Stat stat = zooKeeper.exists(TARGET_ZNODE,  this);
        if(stat == null){
            return;
        }
        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data : " + new String(data) + "children : " + children);
    }

    @Override
    public void process(WatchedEvent event){
        switch(event.getType()) {
            case None:
                if(event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully Connected to ZooKeeper");
                } else {
                    // Waking up main thread to continue executing tasks.
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from ZooKeeper event");
                        zooKeeper.notifyAll();
                    }
                }
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children changed");
                break;
        }
        try {
            watchTargetZnode();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
    }
}
