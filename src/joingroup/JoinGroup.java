package joingroup;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class JoinGroup extends ConnectionWatcher {

	public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
		//zk.create( "/" + groupName, ("value" + memberName).getBytes()/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		String ePath = "/" + groupName + "/" + memberName + "E";
		String pPath = "/" + groupName + "/" + memberName + "P";
		// zk.setData(path, ("value"+memberName).getBytes(), -1);
		String createdPath = zk.create(ePath, ("value" + memberName).getBytes()/* data */, 
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println("Created " + createdPath);
		createdPath = zk.create(pPath, ("value" + memberName).getBytes()/* data */, 
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Created " + createdPath);

	}

	public void list(String groupName) throws KeeperException, InterruptedException {
		String path = "/" + groupName;
		try {
			List<String> children = zk.getChildren(path, true);

			if (children.isEmpty()) {
				System.out.printf("No members in group %s\n", groupName);
				System.exit(1);
			}

			System.out.print("printing");
			for (String child : children) {
				String childPath = "/" + groupName + "/" + child;
				Stat st = new Stat();
				System.out.println("->" + child + " : " + new String(zk.getData(childPath, true, st)));
				System.out.println(st.getVersion());
			}
		} catch (KeeperException.NoNodeException e) {
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}

	public void delete(String groupName, String memberName) throws InterruptedException, KeeperException {
		String parentPath = "/" + groupName;
		String path = "/" + groupName + "/" + memberName;

		// zk.delete(parentPath, -1);
		zk.delete(path, -1);
		System.out.println("deleted");
	}

	public static void main(String[] args) throws Exception {
		JoinGroup joinGroup = new JoinGroup();
		joinGroup.connect(args[0]);
		System.out.println("connected to " + args[0]);
		joinGroup.join(args[1], args[2]);
		// joinGroup.join(args[1], args[2]);
		joinGroup.list(args[1]);
		
		joinGroup.delete(args[1], args[2]+"P");
		joinGroup.list(args[1]);
		// stay alive until process is killed or thread is interrupted
		Thread.sleep(100000);
		joinGroup.close();
		Thread.sleep(100000);
	}
}