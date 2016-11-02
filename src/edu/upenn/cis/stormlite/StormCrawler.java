package edu.upenn.cis.stormlite;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.FetchBolt;
import edu.upenn.cis.stormlite.bolt.DocumentBolt;
import edu.upenn.cis.stormlite.bolt.UrlExtractorBolt;
import edu.upenn.cis.stormlite.bolt.WebsiteRecord;
import edu.upenn.cis.stormlite.example.TestWordCount;
import edu.upenn.cis.stormlite.spout.URLSpout;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;

public class StormCrawler {

	static Logger log = Logger.getLogger(TestWordCount.class);

	private static final String URL_SPOUT = "URL_SPOUT";
	private static final String FETCH_BOLT = "FETCH_BOLT";
	private static final String DOCUMENT_BOLT = "DOCUMENT_BOLT";
	private static final String URLEXTRACTOR_BOLT = "URLEXTRACTOR_BOLT";

	public static void main(String[] args) throws Exception {

		if( args.length < 3  ){

			System.out.println("Invalid number of arguments");
			System.exit(1);

		}else{

			String startUrl = args[0];
			String dbDir = args[1];
			int maxPageSize = Integer.valueOf(args[2]);

			Config config = new Config();
			
			// shared config among the bolts and spouts
			config.put("startUrl", startUrl);
			config.put("dbDir", dbDir);
			config.put("maxPageSize", String.valueOf(maxPageSize));
			config.put("UserAgent", "cis455crawler");
			
			// shared URL Frontier Queue
			LinkedBlockingQueue<URLInfo> frontier_Q = new LinkedBlockingQueue<URLInfo>();
			//WebsiteRecord webrecord = new WebsiteRecord();

			// setup the topology
			TopologyBuilder builder = new TopologyBuilder();
			
			URLSpout urlspout = new URLSpout(frontier_Q);
			//FetchBolt fetch = new FetchBolt(webrecord);
			FetchBolt fetch = new FetchBolt();
			//DocumentBolt documentstore = new DocumentBolt(webrecord);
			DocumentBolt documentstore = new DocumentBolt();
			UrlExtractorBolt urlextractor = new UrlExtractorBolt(frontier_Q);
			
			//spout
			builder.setSpout(URL_SPOUT, urlspout, 1);
			
			// bolts
			builder.setBolt(FETCH_BOLT, fetch, 1);
			builder.setBolt(DOCUMENT_BOLT, documentstore, 1);
			builder.setBolt(URLEXTRACTOR_BOLT, urlextractor, 1);
			
	        Topology topo = builder.createTopology();

	        ObjectMapper mapper = new ObjectMapper();
	        
			try {
				String str = mapper.writeValueAsString(topo);
				
				System.out.println("The StormLite topology is:\n" + str);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/* submit topology to cluster to begin running. */
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology("storm-crawler", config, 
		        		builder.createTopology());
		    Thread.sleep(30000);
		    cluster.killTopology("test");
		    cluster.shutdown();
		    
		    log.debug("Done crawling...");
		    System.exit(0);
			
		}
	}
}

















