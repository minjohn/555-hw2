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
	DBWrapper dbwrapper;
	WebsiteRecord webrecord;
	Fields schema = new Fields("documentUri", "documentBody");
	Map<String, String> config;
	CrawlerRequester requester = new CrawlerRequester();

	String executorId = UUID.randomUUID().toString();

	public void setWebsiteRecord(WebsiteRecord webrecord){
		this.webrecord = webrecord;
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

		String urltext = input.getStringByField("URL");
		URLInfo url = new URLInfo(urltext);

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

					String filepath = url.getFilePath();
					List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(userAgent);
					List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");

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

					if(disallowed == null){
						// there was no disallow map specified for this useragent. Default is that it is allowed.
						disallowedUrl = false;
					}
					else{ // check the list of disallowed links 
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
					}

					if(disallowedUrl == false){ // we are allowed to query this url

						// get crawl delay before sending followup GET request.
						RobotsTxtInfo robotTxt = hostRobotsMap.get(host);
						// get by useragent, hardcoded
						int delay = 0;
						delay = robotTxt.getCrawlDelay(userAgent);

						if ( delay != 0  ){ // it was set, delay next request by specified time.
							//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
							Thread.sleep(delay*1000);
						}


						if(url.getUrl().startsWith("https:")){

							// handle in a little bit

						} 
						else { // handle http separately

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
								}

							}else if(code.compareTo("200") == 0){ // it has been modified

								// check if new modified file is within size limits
								if( requester.isURLValid(response.m_headers) == true ){
									if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file
										String webpage_content;

										if (  ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null)  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");
										}else{
											// wait crawl delay before sending followup GET request, if it was set.
											if ( delay != 0  ){ // it was set, delay next request by specified time.

												// Note: to make it more efficient, could just keep track of last queried time and 
												//   just re-enqueue. If the next time we get a URL for the same host, we just check
												//   if the current time is after the delay interval.
												//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
												Thread.sleep(delay*1000);
											}

											// get new connection to send GET?
											address = InetAddress.getByName(url.getHostName());
											connection = new Socket(address, url.getPortNo());

											out = connection.getOutputStream();
											in = connection.getInputStream();

											System.out.println( url.getUrl() + ": Downloading");
											requester.sendGet(out, url);

											response = HTTPResponseParser.parseResponse("GET", in);
											code = response.m_headers.get("status-code").get(0);
											if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.

												Values vals = new Values(url.getUrl(), response.m_body);
												collector.emit(vals);

											}else {
												System.out.println("GET request failed with code: " + code);
											}

											// update the seen list time, since the file was valid (it was modified), so a fresh 
											//   copy of this url must have a modified date after this one.
											synchronized( webrecord.seenUrls ){
												Date now = Calendar.getInstance().getTime();
												webrecord.seenUrls.put(url.getUrl(), now);
											}

										}
									}
									else{ // this is a .html file or just a link to another url, use JSOUP to extract links
										String webpage_content;

										if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");

										}else{ // not in the database, retreive that webpage.

											// wait crawl delay before sending followup GET request, if it was set.
											if ( delay != 0  ){ // it was set, delay next request by specified time.

												// Note: to make it more efficient, could just keep track of last queried time and 
												//   just re-enqueue. If the next time we get a URL for the same host, we just check
												//   if the current time is after the delay interval.
												System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
												Thread.sleep(delay*1000);
											}

											// get new connection to send GET?
											address = InetAddress.getByName(url.getHostName());
											connection = new Socket(address, url.getPortNo());

											out = connection.getOutputStream();
											in = connection.getInputStream();

											System.out.println( url.getUrl() + ": Downloading");
											requester.sendGet(out, url);

											response = HTTPResponseParser.parseResponse("GET", in);

											code = response.m_headers.get("status-code").get(0);

											if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.

												// emit the document
												Values vals = new Values(url.getUrl(), response.m_body);
												collector.emit(vals);

											}else {
												System.out.println("GET request failed with code: " + code);
											}

										}
									}

									// update the seen list time, since the file was valid (it was modified), so a fresh 
								}
								else{ // fails a mime-type or size requirement
									System.out.println( url.getUrl() + ": Not Downloading");
								}

							}else{ // error sending HEAD request
								System.out.println("HEAD request failed with code: " + code);
							}
						}

					}
					else{
						// don't query this URL, its disallowed
						System.out.println( url.getUrl() + ": Restricted. Not downloading");
					}
				}
			} else { // We haven't seen this url before

				boolean disallowedUrl = false;
				String host = url.getHostName();

				// check if this is a disallowed URL
				if(hostRobotsMap.containsKey(host)){ // we've seen this host before.


					String filepath = url.getFilePath();
					List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(userAgent);
					List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");

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


					if(disallowed == null){
						// no disallowed URL list was specified for this userAgent, default is allowed.
						//System.out.println("this is a allowed path");
						disallowedUrl = false;
					}
					else{ 
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

					}

					if (disallowedUrl == false){

						// Send a Head request to check if file has been modified.
						boolean ssl = false;
						if(url.getUrl().startsWith("https:")){ // https request
							// https related stuff

						} else { // handle http separately

							// check the crawl-delay and wait 
							int delay = hostRobotsMap.get(host).getCrawlDelay(userAgent);

							if (delay != 0 ){
								// Note: to make it more efficient, could just keep track of last queried time and 
								//   just re-enqueue. If the next time we get a URL for the same host, we just check
								//   if the current time is after the delay interval.
								//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
								Thread.sleep(delay*1000);
							}

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

									if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file


										String webpage_content;

										if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");
										}
										else{


											delay = hostRobotsMap.get(host).getCrawlDelay(userAgent);
											if ( delay != 0  ){ // it was set, delay next request by specified time.

												// Note: to make it more efficient, could just keep track of last queried time and 
												//   just re-enqueue. If the next time we get a URL for the same host, we just check
												//   if the current time is after the delay interval.
												//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
												Thread.sleep(delay*1000);
											}

											// get new connection to send GET?
											address = InetAddress.getByName(url.getHostName());
											connection = new Socket(address, url.getPortNo());

											out = connection.getOutputStream();
											in = connection.getInputStream();

											System.out.println( url.getUrl() + ": Downloading");
											requester.sendGet(out, url);

											response = HTTPResponseParser.parseResponse("GET", in);
											code = response.m_headers.get("status-code").get(0);

											if( code.compareTo("200") == 0 ){
												// emit the document
												Values vals = new Values(url.getUrl(), response.m_body);
												collector.emit(vals);
											} else{
												System.out.println("GET request failed with code: " + code);
											}

											// update the seen list, since this is a url we have not seen before.
											synchronized( webrecord.seenUrls ){
												Date now = Calendar.getInstance().getTime();
												webrecord.seenUrls.put(url.getUrl(), now);
											}

										}

									} 
									else{ // this is a .html file or just a link to another url, use JSOUP to extract links

										String webpage_content;

										if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
											System.out.println( url.getUrl() + ": Content Seen");
										}
										else{


											delay = hostRobotsMap.get(host).getCrawlDelay(userAgent);
											if ( delay != 0  ){ // it was set, delay next request by specified time.

												// Note: to make it more efficient, could just keep track of last queried time and 
												//   just re-enqueue. If the next time we get a URL for the same host, we just check
												//   if the current time is after the delay interval.
												//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
												Thread.sleep(delay*1000);
											}


											// get new connection to send GET?
											address = InetAddress.getByName(url.getHostName());
											connection = new Socket(address, url.getPortNo());

											out = connection.getOutputStream();
											in = connection.getInputStream();

											System.out.println( url.getUrl() + ": Downloading");
											requester.sendGet(out, url);

											response = HTTPResponseParser.parseResponse("GET", in);
											code = response.m_headers.get("status-code").get(0);

											if( code.compareTo("200") == 0 ){

												// emit the document
												Values vals = new Values(url.getUrl(), response.m_body);
												collector.emit(vals);

											}else{
												System.out.println("GET request failed with code: " + code);
											}
										}

										// update the seen list, since this is a url we have not seen before.
										synchronized( webrecord.seenUrls ){
											Date now = Calendar.getInstance().getTime();
											webrecord.seenUrls.put(url.getUrl(), now);
										}

									}
								}
								else{// fails a mime-type or size requirement
									System.out.println( url.getUrl() + ": Not Downloading");
								}

							} else{ // HEAD request failed...
								System.out.println("HEAD request failed with code: "+ code);
							}
						}

					} else{
						// disallowed, do nothing. but add to seen list.
						System.out.println( url.getUrl() + ": Restricted. Not downloading");
						// disallowed, do nothing. but add to seen list.
						synchronized( webrecord.seenUrls ){
							Date now = Calendar.getInstance().getTime();
							webrecord.seenUrls.put(url.getUrl(), now);
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
								Values vals = new Values(url.getUrl(), robot_content);
								collector.emit(vals);

							}
							else{ // no robots.txt file?? I guess use defaults....?
								System.out.println("Robots.txt GET request failed with code: " + code);
							}
						}
					}

					if(robotTxt != null){

						List<String> disallowedLinks = robotTxt.getDisallowedLinks(userAgent);
						List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");

						// add default disallowed links to the set
						if( disallowedLinks == null ){ // nothing for this user agent
							if(defaultDisallowed == null){
								disallowedLinks = null;
							}else{ // just use the default
								//System.out.println("Using default links");
								disallowedLinks = defaultDisallowed;
							}
						}else{// add to existing

							for( String link  : defaultDisallowed ){
								if( !disallowedLinks.contains(link) ){
									disallowedLinks.add(link);
								}
							}
						}

						if( disallowedLinks != null){

							String disallowedPath;
							String filepath = url.getFilePath();

							for(String path  : disallowedLinks ){

								if(path.endsWith("/")){
									// get rid of the / at the end
									disallowedPath = path.substring(0, path.length()-1);
								}else{
									disallowedPath = path;
								}

								if(filepath.startsWith(disallowedPath) ==  true  ){
									disallowedUrl = true;
									break;
								}

							}

							if(disallowedUrl == true ){
								// not allowed

							}else{
								// allowed, add it again and the portion of code that handles "Seen Host" will send the HEAD request.

								// probably want to do a second Get request to get actual url content
								int delay = hostRobotsMap.get(host).getCrawlDelay(userAgent);
								if ( delay != 0  ){ // it was set, delay next request by specified time.

									// Note: to make it more efficient, could just keep track of last queried time and 
									//   just re-enqueue. If the next time we get a URL for the same host, we just check
									//   if the current time is after the delay interval.
									//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
									Thread.sleep(delay*1000);
								}


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

								//   copy of this url must have a modified date after this one.
								synchronized( webrecord.seenUrls ){
									Date now = Calendar.getInstance().getTime();
									webrecord.seenUrls.put(url.getUrl(), now);
								}

							}
						} else{
							// also allowed? this useragent is not specified in the robots.txt file.

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

							//   copy of this url must have a modified date after this one.
							synchronized( webrecord.seenUrls ){
								Date now = Calendar.getInstance().getTime();
								webrecord.seenUrls.put(url.getUrl(), now);
							}

						}
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
