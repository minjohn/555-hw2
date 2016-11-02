package edu.upenn.cis.stormlite.spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class URLSpout implements IRichSpout{

	private LinkedBlockingQueue<URLInfo> urlQueue;
	
	/**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	Fields schema = new Fields("URL");
	
	 String executorId = UUID.randomUUID().toString();

	public URLSpout(LinkedBlockingQueue<URLInfo> urlQueue){ 
		this.urlQueue = urlQueue; 
	}
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public void nextTuple() {
		
		URLInfo url = urlQueue.remove();
		this.collector.emit(new Values(url));
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
