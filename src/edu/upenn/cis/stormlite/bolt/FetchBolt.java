package edu.upenn.cis.stormlite.bolt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.example.TestWordCount;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.CrawlerRequester;
import edu.upenn.cis455.crawler.HTTPResponseParser;
import edu.upenn.cis455.crawler.RobotParser;
import edu.upenn.cis455.crawler.RobotParser.RobotTuple;
import edu.upenn.cis455.crawler.info.ResponseTuple;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DBWrapperFactory;

public class FetchBolt implements IRichBolt{

	static Logger log = Logger.getLogger(FetchBolt.class);


	private OutputCollector collector;
	private static DBWrapper dbwrapper;
	private static WebsiteRecord webrecord;
	private static LinkedBlockingQueue<URLInfo> urlQueue;

	// for respecting robots, have threads check last host access one at a time.
	private static 

	Fields schema = new Fields("documentUri", "documentBody");
	Map<String, String> config;
	CrawlerRequester requester = new CrawlerRequester();

	String executorId = UUID.randomUUID().toString();

	public void setWebsiteRecord(WebsiteRecord webrecord){
		this.webrecord = webrecord;
	}

	public void setQueue(LinkedBlockingQueue<URLInfo> urlQueue){
		this.urlQueue = urlQueue;
	}

	// constructor
	public FetchBolt(){

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
	public void cleanup() {

		// cleanup socket stuff?
	}

	@Override
	public void execute(Tuple input) {

		URLInfo url = (URLInfo)input.getObjectByField("URL");
		//URLInfo url = new URLInfo(urltext);

		// the static seenlists that potentially multiple bolts can use to check urls against.
		HashMap<String, Date> seenUrls = webrecord.seenUrls;
		HashMap<String, RobotsTxtInfo> hostRobotsMap = webrecord.hostRobotsMap;

		String userAgent = config.get("UserAgent");

		OutputStream out = null;
		InputStream in = null;

		try {
			if( seenUrls.containsKey(url.getUrl()) == true ){

				boolean disallowedUrl = false;
				String host = url.getHostName();

				// check if this is a disallowed URL
				if(hostRobotsMap.containsKey(url.getHostName())){ // we've seen this host before


					// get crawl delay before sending followup GET request.
					RobotsTxtInfo robotTxt = hostRobotsMap.get(host);

					// get by useragent, hardcoded
					int delay = robotTxt.getCrawlDelay(userAgent);

					// lock the webrecord object for ensuring atmoic checks and updates to the host last accessed times
					synchronized(webrecord){


						if ( delay != 0  ){ // it was set, delay next request by specified time.
							//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
							Date now = Calendar.getInstance().getTime();
							Date last_accessed = webrecord.hostLastAccessed.get(url.getHostName());
							Date host_delayed = new Date( last_accessed.getTime() + delay * 1000   );

							if( host_delayed.before(now) == false  ){
								//re-enqueue
								//log.info("NOT WITHIN CRAWL DELAY RE-ENQUEUE!" + url.getUrl());
								this.urlQueue.add(url);
								return; // no further processing needed.
							} else{
								// otherwise, we are free to requery
							}

						}


						if(url.getUrl().startsWith("https:")){

							// handle in a little bit

						} 
						else { // handle http separately

							// check if its a head or GET request
							if( url.getMethod().compareTo("HEAD") == 0 ){

								// Send a Head request to check if file has been modified.
								InetAddress address = InetAddress.getByName(url.getHostName());

								Socket connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();

								requester.sendHead(out, url, true);

								ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

								String code = response.m_headers.get("status-code").get(0);

								if(code.compareTo("304") == 0){ // not modified 
									//do nothing, no need to put back into the seen urls list
									System.out.println( url.getUrl() + ": Not Modified");

									// dont retrieve it again if its an xml doc, but probably re-extract the links of a html file
									if( !url.getUrl().endsWith(".xml") ){ // html file

										String webpage_content = dbwrapper.getWebPage(url.getUrl());

										if(webpage_content != null){
											// emit the doc
											Values vals = new Values(url.getUrl(), webpage_content);
											collector.emit(vals);

										}else{
											System.out.println("Could not retrieve webpage content from database!");
										}

										synchronized( webrecord ){
											Date now = Calendar.getInstance().getTime();
											webrecord.seenUrls.put(url.getUrl(), now);
											webrecord.hostLastAccessed.put(url.getHostName(), now);
										}
									}

								}
								else if(code.compareTo("200") == 0){ // it has been modified

									if( requester.isURLValid(response.m_headers) == true ){

										String  webpage_content;
										if (  ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null)  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");

											Values vals = new Values(url.getUrl(), webpage_content);
											collector.emit(vals);

											// update the seen list time, since the file was valid (it was modified), so a fresh 
											//   copy of this url must have a modified date after this one.
											synchronized( webrecord ){
												Date now = Calendar.getInstance().getTime();
												webrecord.seenUrls.put(url.getUrl(), now);
												webrecord.hostLastAccessed.put(url.getHostName(), now);
											}

										}else {

											// Enqueue a GET request
											url.setMethod("GET");
											this.urlQueue.add(url);

											synchronized( webrecord ){
												Date now = Calendar.getInstance().getTime();
												// only update host lastaccessed because, we still want to handle a GET request.
												webrecord.hostLastAccessed.put(url.getHostName(), now);
											}

											return; // no further processing needed.
										}

									} else{ // fails a mime-type or size requirement
										System.out.println( url.getUrl() + ": Not Downloading");
									}

								}else{ // error sending HEAD request
									System.out.println("HEAD request failed with code: " + code + "URL: " + url.getUrl());
								}

							}


							// GET REQUEST
							else if( url.getMethod().compareTo("GET") == 0 ){

								// get new connection to send GET?
								InetAddress address = InetAddress.getByName(url.getHostName());
								Socket connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();

								System.out.println( url.getUrl() + ": Downloading");
								requester.sendGet(out, url);

								ResponseTuple response = HTTPResponseParser.parseResponse("GET", in);
								String code = response.m_headers.get("status-code").get(0);
								if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.

									Values vals = new Values(url.getUrl(), response.m_body);
									collector.emit(vals);

								}else {
									System.out.println("GET request failed with code: " + code);
								}

								// update the seen list time, since the file was valid (it was modified), so a fresh 
								//   copy of this url must have a modified date after this one.
								synchronized( webrecord ){
									Date now = Calendar.getInstance().getTime();
									webrecord.seenUrls.put(url.getUrl(), now);
									webrecord.hostLastAccessed.put(url.getHostName(), now);
								}

							}

						}

					} // synchronized call


				} else{
					// host doesn't exist...?
				}
			} else { // We haven't seen this url before

				String host = url.getHostName();

				// check if this is a disallowed URL
				if(hostRobotsMap.containsKey(host)){ // we've seen this host before.

					// Send a Head request to check if file has been modified.
					boolean ssl = false;
					if(url.getUrl().startsWith("https:")){ // https request
						// https related stuff

					} else { // handle http separately


						synchronized(webrecord){
							// check the crawl-delay and wait 
							int delay = hostRobotsMap.get(host).getCrawlDelay(userAgent);

							if ( delay != 0  ){ // it was set, delay next request by specified time.
								//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
								Date now = Calendar.getInstance().getTime();
								Date last_accessed = webrecord.hostLastAccessed.get(url.getHostName());
								Date host_delayed = new Date( last_accessed.getTime() + delay * 1000   );

								if( host_delayed.before(now) == false  ){
									//re-enqueue

									//log.info("NOT WITHIN CRAWL DELAY RE-ENQUEUE!" + url.getUrl());
									this.urlQueue.add(url);
									return; // no further processing needed.
								} else{
									// otherwise, we are free to requery

									//log.info("READY to query HEAD! " + url.getUrl());
								}

							}


							// check if its a head or GET request
							if( url.getMethod().compareTo("HEAD") == 0 ){

								// Send a Head request to check if file has been modified.
								InetAddress address = InetAddress.getByName(url.getHostName());

								Socket connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();

								// haven't seen this before, set flag to false, as we don't have any info of last access time.
								requester.sendHead(out, url, false);

								ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

								String code = response.m_headers.get("status-code").get(0);

								if( code.compareTo("200") == 0 ){

									if( requester.isURLValid(response.m_headers) == true ){

										String webpage_content;

										if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");

											Values vals = new Values(url.getUrl(), webpage_content);
											collector.emit(vals);

											//TODO
											synchronized( webrecord ){
												Date now = Calendar.getInstance().getTime();
												//webrecord.seenUrls.put(url.getUrl(), now);
												webrecord.hostLastAccessed.put(url.getHostName(), now);
											}
										} 
										else{

											// Enqueue a GET request
											url.setMethod("GET");
											this.urlQueue.add(url);

											synchronized( webrecord ){
												Date now = Calendar.getInstance().getTime();
												// only update host lastaccessed because, we still want to handle a GET request.
												webrecord.hostLastAccessed.put(url.getHostName(), now);
											}

											return; // no further processing needed.
										}

									}else{// fails a mime-type or size requirement
										System.out.println( url.getUrl() + ": Not Downloading");
									}

								} else{ // HEAD request failed...
									System.out.println("HEAD request failed with code: " + code + "URL: " + url.getUrl());
								}

							} 

							else if ( url.getMethod().compareTo("GET") == 0   ){
								// get new connection to send GET?
								InetAddress address = InetAddress.getByName(url.getHostName());
								Socket connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();

								System.out.println( url.getUrl() + ": Downloading");
								requester.sendGet(out, url);

								ResponseTuple response = HTTPResponseParser.parseResponse("GET", in);
								String code = response.m_headers.get("status-code").get(0);

								if( code.compareTo("200") == 0 ){

									// emit the document
									Values vals = new Values(url.getUrl(), response.m_body);
									collector.emit(vals);

								}else{
									System.out.println("GET request failed with code: " + code);
								}

								// update the seen list, since this is a url we have not seen before.
								synchronized( webrecord ){
									Date now = Calendar.getInstance().getTime();
									//webrecord.seenUrls.put(url.getUrl(), now);
									webrecord.hostLastAccessed.put(url.getHostName(), now);
								}

							}
						}
					}

				}else{ // have not seen this host before, need to get the robots.txt file.


					RobotsTxtInfo robotTxt = null;
					String robotTxtContent;

					String roboturl = url.getUrl()+"robots.txt";

					//System.out.println("We haven't seen this host before: " + host);
					if ( ( robotTxtContent = dbwrapper.getWebPage(roboturl) ) != null  ){ // check if we have the page in the database, first.

						System.out.println( url.getHostName() + "/robots.txt: Content Seen");

						RobotParser parser = new RobotParser();

						robotTxt = parser.parseRobotString(robotTxtContent);
						hostRobotsMap.put(host, robotTxt);
					}
					else{ // retreive the text file.

						// Send a Head request to check if file has been modified.
						boolean ssl = false;
						if(url.getUrl().startsWith("https:")){


						} else {
							// Send a Head request to check if file has been modified.
							InetAddress address = InetAddress.getByName(url.getHostName());

							Socket connection = new Socket(address, url.getPortNo());

							out = connection.getOutputStream();
							in = connection.getInputStream();

							System.out.println( url.getUrl() + "robots.txt: Downloading");

							requester.sendGetRobots(out, url);

							RobotParser parser = new RobotParser();
							RobotTuple response = parser.parseRobotResponse(in); 

							String code = response.m_headers.get("status-code").get(0);

							if( code.compareTo("200") == 0 ){

								String robot_content = response.getRobotText();
								RobotParser rparser = new RobotParser();

								robotTxt = rparser.parseRobotString(robot_content);

								// insert the robots file for this host.
								hostRobotsMap.put(host, robotTxt);

								// emit the document
								Values vals = new Values(roboturl, robot_content);
								collector.emit(vals);

							}
							else{ // no robots.txt file?? I guess use defaults....?
								System.out.println("Robots.txt GET request failed with code: " + code);
							}
						}
					}

					if(robotTxt != null){

						// - allowed, add actual url back in again and the portion of code that handles "Seen Host" will send the HEAD request.
						// - update the seen list, since this is a url we have not seen before.
						// - also update host's last acessed, because we respect the robots file even after the initial request to robots.txt
						//log.info("Processed RobotsTXT  RE-ENQUEUE!");

						synchronized( webrecord ){
							Date now = Calendar.getInstance().getTime();
							//webrecord.seenUrls.put(roboturl, now);
							webrecord.hostLastAccessed.put(url.getHostName(), now);
						}
						url.setMethod("GET");
						this.urlQueue.add(url);
						return; // no further processing needed.

					}
				}
			}

		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialization, just saves the output stream destination ("collector")
	 */
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.config = stormConf;
		this.collector = collector;

		String dbDir = stormConf.get("dbDir");
		String maxSize = stormConf.get("maxPageSize");

		requester.setMaxFileSize(Integer.valueOf(maxSize));

		try {

			DBWrapperFactory factory = new DBWrapperFactory();
			factory.setDBDir(dbDir);
			// set up db connection.
			dbwrapper = factory.getDBWrapper() ;

		} catch (DatabaseException e) {

			e.printStackTrace();
			System.exit(1);

		} catch (IOException e) {

			e.printStackTrace();
			System.exit(1);

		}

	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}



}
