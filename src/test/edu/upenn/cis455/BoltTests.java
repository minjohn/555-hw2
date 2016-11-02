package test.edu.upenn.cis455;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;

import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.FetchBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

public class BoltTests {

	@Test
	public void fetchBolTtest() {
		
		// some setup stuff
		Topology topology;
		Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
		
		TopologyContext context = new TopologyContext(null, taskQueue);
		
		HashMap<String, String> stormConf = new HashMap<String, String>();
		
		stormConf.put("startUrl", "http://crawltest.cis.upenn.edu/");
		stormConf.put("dbDir", "/home/cis555/workspace/555-hw2/DATABASE");
		stormConf.put("maxPageSize", String.valueOf(10000));
		stormConf.put("UserAgent", "cis455crawler");
		
		OutputCollector collector = new OutputCollector(context);
		
		FetchBolt bolt = new FetchBolt();
		
		Tuple next = new Tuple( new Fields("URL"), new Values("http://crawltest.cis.upenn.edu/"));
		
		bolt.prepare(stormConf, context, collector);
		
		bolt.execute(next);
		
		
	}

}
