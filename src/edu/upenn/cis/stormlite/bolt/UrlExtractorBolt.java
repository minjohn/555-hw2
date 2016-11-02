package edu.upenn.cis.stormlite.bolt;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.URLSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.info.URLInfo;

public class UrlExtractorBolt implements IRichBolt {
	
	static Logger log = Logger.getLogger(UrlExtractorBolt.class);
	
	// instance variables
	private static LinkedBlockingQueue<URLInfo> frontier_Q;
	
	String executorId = UUID.randomUUID().toString();
	
	// constructor
	public void setQueue ( LinkedBlockingQueue<URLInfo> queue ){
		this.frontier_Q = queue;
	}
	
	public UrlExtractorBolt(  ){
		
	}
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// do nothing.
	}

	@Override
	public void cleanup() {
		// nothing to do.
	}

	@Override
	public void execute(Tuple input) {
		String documentUri = input.getStringByField("documentUri");
		String documentBody = input.getStringByField("documentBody");
		
		URLInfo url = new URLInfo(documentUri);
		
		Document doc = Jsoup.parse(documentBody);
		Elements links = doc.select("a[href]");

		for( Element link : links  ){

			//String abs_url = url.getUrl() + link.attr("href");
			String abs_url = url.getBaseUrl() + link.attr("href");

			//for(Attribute a : link.attributes()){
			//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
			//}

			//System.out.println("link extracted: " + abs_url);
			//System.out.println("link element: " + link.toString());
			
			//log.info("extracted URL: " + abs_url);
			
			frontier_Q.add(new URLInfo(abs_url));

		}			
		
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		// no need to set routing, we are just enqueuing urls in a shared url object.
	}

	@Override
	public Fields getSchema() {
		return null;
	}
	
}

















