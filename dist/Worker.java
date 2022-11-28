import java.net.UnknownHostException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.io.*;	
import java.util.logging.*;
import java.time.*;
import java.io.*;
import java.net.*;
import java.lang.management.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

public class Worker implements  AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback, Watcher
{
    ZooKeeper zk;
    String worker_id = "/dist10/workers/worker-";
    String task_path = "/dist10/tasks";
    String workerNodePath;
    String job_path = "/JOB";
    String pInfo;
    Thread thread;
    worker_thread workerThread = new worker_thread();
    public Worker(ZooKeeper zk)
        {
            this.zk = zk;
		    pInfo = ManagementFactory.getRuntimeMXBean().getName();
            thread = new Thread(workerThread);
            createWorkerNode();
        }

    void createWorkerNode()
    {
        try
        {
            workerNodePath = zk.create(worker_id, pInfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        catch (KeeperException ke) {System.out.println(ke);}
        catch (InterruptedException ie) {System.out.println(ie);}
        
    }

    public void process(WatchedEvent e)
	{
		if(e.getType() == Watcher.Event.EventType.NodeCreated && e.getPath().equals(workerNodePath))
		{
			zk.getChildren(workerNodePath, this, this, null);
		}
    }


    public void processResult(int rc, String path, Object ctx, List<String> children)
    {
        zk.getData(workerNodePath + job_path, null, this, null);
    }

    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
    {
        if (path.startsWith(task_path))
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInput in = new ObjectInputStream(bis);
                DistTask dt = (DistTask) in.readObject();
                workerThread.distTask = dt;
                thread.start();
            }
            catch (IOException ioe) {System.out.println(ioe);}
            catch (ClassNotFoundException cnfe) {System.out.println(cnfe);}
        }
        else if (path.startsWith(workerNodePath))
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInput in = new ObjectInputStream(bis);
                String child = (String) in.readObject();
                workerThread.childTask = child;
                zk.getData(task_path + "/" + child, null, this, null);
            }
            catch (IOException ioe) {System.out.println(ioe);}
            catch (ClassNotFoundException cnfe) {System.out.println(cnfe);}

        }
    }
    
    private class worker_thread implements Runnable
    {
        public DistTask distTask;
        public String childTask;
        public void run()
            {
			try
			{
                if (distTask == null || childTask == null)
                    {
                        System.out.println("dist task was null inside the workerThread's run method");
                        return;
                    }
				distTask.compute();
				
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(distTask); 
				oos.flush();

				// Store it inside the result node.
				zk.create(task_path+"/"+childTask+"/result",  bos.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.delete(workerNodePath + job_path, -1, null, null);
                distTask = null;
			}
			catch(NodeExistsException nee){System.out.println(nee);}
			catch(KeeperException ke){System.out.println(ke);}
			catch(InterruptedException ie){System.out.println(ie);}
			catch(IOException io){System.out.println(io);}
            }
    }
}