package edu.upenn.cis.stormlite.spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.FetchBolt;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class URLSpout implements IRichSpout{

	private static LinkedBlockingQueue<URLInfo> urlQueue;
	
	static Logger log = Logger.getLogger(URLSpout.class);
	
	/**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	Fields schema = new Fields("URL");
	
	 String executorId = UUID.randomUUID().toString();

	public void setQueue(LinkedBlockingQueue<URLInfo> urlQueue){ 
		this.urlQueue = urlQueue; 
	}
	
	public URLSpout(){
		
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
		
		URLInfo url  = urlQueue.poll();
		if(url != null){
			String url_string = url.getUrl();
			this.collector.emit(new Values<Object>(url_string));
			log.info(getExecutorId() + " emitting " + url_string);
		}		
		
		Thread.yield();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
