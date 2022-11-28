import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.

public class Master implements Watcher, AsyncCallback.ChildrenCallback
{
    ZooKeeper zk;
    String task_path = "/dist10/tasks";
    String worker_path = "/dist10/workers";
    String job_path = "/JOB";
	Dictionary<String, Boolean> workers = new Hashtable<String, Boolean>();
	List<String> tasks = new ArrayList<String>();
    Queue<String> queuedComputations = new LinkedList<String>();

    public Master(ZooKeeper zk)
    {
        this.zk = zk;
        
        getTasks();
        getWorkers();
    }

	public void process(WatchedEvent e)
	{
        
		// Master should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals(task_path))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();
		}

		if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals(worker_path))
		{
			//Means a new worker has been created and ready to do work.
			getWorkers();
		}
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		// This logic is for master !!
		//Every time a new task znode is created by the client, this will be invoked.

		// TODO: Filter out and go over only the newly created task znodes.
		//		Also have a mechanism to assign these tasks to a "Worker" process.
		//		The worker must invoke the "compute" function of the Task send by the client.
		//What to do if you do not have a free worker process?
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
        try
        {
            for(String c: children)
            {
                if (tasks.contains(c))
                {
                    System.out.println("Child " + c + " was already computed");
                    continue;
                }

                tasks.add(c);
                System.out.println("Adding child " + c + " to the computation list.");

                String workerID = getFirstAvailableWorker();
                if (workerID == null)
                    queuedComputations.add(c);
                else
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(c); 
                    oos.flush();
                    byte[] taskPathByteArray =  bos.toByteArray();
                    zk.create(worker_path+"/"+workerID+job_path, taskPathByteArray, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        }
        catch (IOException ioe) {System.out.println(ioe);}
        catch (KeeperException ke) {System.out.println(ke);}
        catch (InterruptedException ie) {System.out.println(ie);}
	}
    
    String getFirstAvailableWorker()
    {
        Enumeration enu = workers.keys();
        while (enu.hasMoreElements())
        {
            String elem = (String)enu.nextElement();
            if (workers.get(elem))
                return elem;
        }
        return null;
    }

	// Master fetching task znodes...
	void getTasks()
	{
		zk.getChildren(task_path, this, this, null);  
	}

	void getWorkers()
	{
        try 
        {
		    for (String child : zk.getChildren(worker_path, this))
            {
                workers.put(child, false);
            }
        }
        catch(KeeperException ke){System.out.println(ke);}
        catch(InterruptedException ie){System.out.println(ie);}
	}
}