package edu.upenn.cis.stormlite.spout;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.FetchBolt;
import edu.upenn.cis.stormlite.bolt.WebsiteRecord;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;

public class URLSpout implements IRichSpout{

	private static LinkedBlockingQueue<URLInfo> urlQueue;
	private static WebsiteRecord webrecord;
	private static Map<String, String> config;

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

	public void setWebsiteRecord(WebsiteRecord webrecord){ 
		this.webrecord = webrecord; 
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
		this.config = config;
		this.collector = collector;
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public void nextTuple() {

		boolean somethingToEmit = false;

		while(somethingToEmit == false){
			
			URLInfo url  = urlQueue.poll();
			if(url != null){
				String url_string = url.getUrl();
				//log.info("checking " + url.getUrl());

				Date last_accessed = webrecord.hostLastAccessed.get(url.getHostName());
				RobotsTxtInfo host_robotsTxt = webrecord.hostRobotsMap.get(url.getHostName()); 
				
				Date now = Calendar.getInstance().getTime();
				
				if( host_robotsTxt == null ){ // no crawl delay for this host, by default, just allow subsequent queries

					this.collector.emit(new Values<Object>(url_string));
					//log.info(" NO HOSTS ROBOTS " + getExecutorId() + " emitting " + url_string);
					
					somethingToEmit = true;
					

				}else{ // check for the crawl delay

					//log.info(url.getUrl() + "Check the crawl delay... ");

					String userAgent = config.get("UserAgent");

					int host_crawl_delay = host_robotsTxt.getCrawlDelay(userAgent);
					
					if(host_crawl_delay != 0){

						Date host_delay = new Date( last_accessed.getTime() + host_crawl_delay * 1000   );
						
						if( host_delay.before(now) == true  ){
							this.collector.emit(new Values<Object>(url_string));
							//log.info(getExecutorId() + " emitting " + url_string);
							
							somethingToEmit = true;

						} else{

							// to respect robots.txt don't query this host now.
							//log.info("reenqueuing " + url.getUrl());
							urlQueue.add(url);

						}

					} 
					else{

						somethingToEmit = true;
						
						this.collector.emit(new Values<Object>(url_string));
						//log.info("NO CRAWL DELAY "+getExecutorId() + " emitting " + url_string);
						
					}
				}

			}
		}

		Thread.yield();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
