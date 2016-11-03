package edu.upenn.cis.stormlite.bolt;

import java.util.List;
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

public class UrlFilterBolt implements IRichBolt {
	
	static Logger log = Logger.getLogger(UrlFilterBolt.class);
	
	// instance variables
	private static LinkedBlockingQueue<URLInfo> frontier_Q;
	private static WebsiteRecord webrecord;
	Map<String, String> config;
	
	String executorId = UUID.randomUUID().toString();
	

	public void setQueue ( LinkedBlockingQueue<URLInfo> queue ){
		this.frontier_Q = queue;
	}
	
	public void setWebsiteRecord(WebsiteRecord webrecord){
		this.webrecord = webrecord;
	}
	
	public UrlFilterBolt(  ){
		
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
		String documentUri = input.getStringByField("EXTRACTED_URL");
		
		URLInfo url = new URLInfo(documentUri);
		String host = url.getHostName();
		
		String filepath = url.getFilePath();
		
		List<String> disallowed = webrecord.hostRobotsMap.get(host).getDisallowedLinks( config.get("UserAgent"));
		List<String> defaultDisallowed = webrecord.hostRobotsMap.get(host).getDisallowedLinks("*");
		
		
		// add default disallowed links to the set
		if( disallowed == null ){ // nothing for this user agent
			if(defaultDisallowed == null){
				disallowed = null;
			}else{ // just use the default
				disallowed = defaultDisallowed;
			}
		}else{// add to existing
			for( String link  : defaultDisallowed ){
				if( !disallowed.contains(link) ){
					disallowed.add(link);
				}
			}
		}
		
		
		boolean disallowedUrl = false;
		
		for(String path  : disallowed ){

			String disallowedPath;

			if(path.endsWith("/")){
				// get rid of the / at the end
				disallowedPath = path.substring(0, path.length()-1);
			}else{
				disallowedPath = path;
			}

			//System.out.println(" comparing " + filepath + " with " + disallowedPath);

			if(filepath.startsWith(disallowedPath) ==  true  ){
				disallowedUrl = true;
				break;
			}

		}
		
		if(disallowedUrl == false){
			URLInfo newurl = new URLInfo(documentUri);
			newurl.setMethod("HEAD");
			frontier_Q.add(newurl);
		} else{
			System.out.println( url.getUrl() + ": Restricted. Not downloading");
		}
		
		//Document doc = Jsoup.parse(documentBody);
		//Elements links = doc.select("a[href]");

		//log.info("Extracting links from " + documentUri);
//		for( Element link : links  ){
//
//			//String abs_url = url.getUrl() + link.attr("href");
//			String abs_url = url.getBaseUrl() + link.attr("href");
//
//			//for(Attribute a : link.attributes()){
//			//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
//			//}
//
//			//System.out.println("link extracted: " + abs_url);
//			//System.out.println("link element: " + link.toString());
//			
//			//log.info("extracted URL: " + abs_url);
//			URLInfo newurl = new URLInfo(abs_url);
//			newurl.setMethod("HEAD");
//			frontier_Q.add(newurl);
//
//		}			
		
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
		this.config = stormConf;
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

















