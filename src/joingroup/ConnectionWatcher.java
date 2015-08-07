package joingroup;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionWatcher implements Watcher {
	private static final int SESSION_TIMEOUT = 5;
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
		
		Thread.sleep(100);
		
		System.out.println("g exits"+zk.exists("/g", this));
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("Watching ->" + event.getState());
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	public void close() throws InterruptedException {
		System.out.println("Close");
		zk.close();
	}
}
