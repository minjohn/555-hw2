package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;

import edu.upenn.cis455.crawler.RobotParser.RobotTuple;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class XPathCrawler {


	public String getStartUrl() {
		return startUrl;
	}

	public void setStartUrl(String startUrl) {
		this.startUrl = startUrl;
	}

	public String getDbDir() {
		return dbDir;
	}

	public void setDbDir(String dbDir) {
		this.dbDir = dbDir;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public void enqueueURL( URLInfo url ){
		frontier_Q.add(url);
	}
	
	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	//Instance variables
	String startUrl;
	String dbDir;
	String userAgent;
	int maxSize;
	Queue<URLInfo> frontier_Q = new LinkedList<URLInfo>();
	HashMap<String, Date> seenUrls = new HashMap<String, Date>(); // url and last time we requested it.
	HashMap<String, RobotsTxtInfo> hostRobotsMap = new HashMap<String, RobotsTxtInfo>();
	String[] fileTypes = { "text/html", "text/plain", "application/html", "+xml" };
	Set<String> acceptableFileTypes = new HashSet<String>(Arrays.asList(fileTypes));
	RobotParser robotParser = new RobotParser();
	

	private void sendHead(OutputStream out, URLInfo info, boolean seen){

		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);

		PrintWriter writer = new PrintWriter(out,true);

		if(seen == true){ // seen this url, checking if it was modified.

			writer.println("HEAD " + info.getUrl() +" HTTP/1.1");
			writer.println("Host: " + info.getHostName());
			writer.println("User-agent: cis455crawler");

			String url = info.getUrl();
			String date = dateFormat.format(seenUrls.get(url));
			writer.println("If-Modified-Since:" + date);
			writer.println("\n");

		}else{

			writer.println("HEAD " + info.getUrl() +" HTTP/1.1");
			writer.println("Host: " + info.getHostName());
			writer.println("User-agent: cis455crawler");
			writer.println("\n");

		}

	}

	// We assume that the socket is already connected to the host?
	private void sendGet(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET " + info.getUrl() +" HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-agent: cis455crawler");
		writer.println("\n");


	}

	// We assume that the socket is already connected to the host?
	private void sendGetRobots(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET /robots.txt HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-agent: cis455crawler");
		writer.println("\n");

	}


	private boolean isURLValid(HashMap<String, List<String>> headers ){

		boolean valid = false;
		int length = 0;
		String contentType = "";

		if( headers.containsKey("content-length")  ){

			length = Integer.getInteger(headers.get("content-length").get(0));

		}

		if (headers.containsKey("content-type")){
			contentType = headers.get("content-type").get(0);
		}

		if( length != 0 && length < maxSize ) {
			valid = true;
		}

		if( contentType.compareTo("") != 0 && acceptableFileTypes.contains(contentType) ){
			valid = true;
		}

		return valid;

	}




	private void run() throws IOException, InterruptedException{

		OutputStream out = null;
		InputStream in = null;

		
		
		// main loop
		while( frontier_Q.size() > 0 ){
			
//			URLInfo urls[] = new URLInfo[frontier_Q.size()];
//			frontier_Q.toArray(urls);
//			System.out.println("URLs in Queue ======");
//			for(URLInfo text  : urls  ){
//				System.out.println(text.getUrl());
//			}
//			System.out.println("END URLs in Queue ======");
			
			
			URLInfo url = frontier_Q.remove();
			
			System.out.println("dequeued URL:" + url.getUrl());

			// we've seen the url before
			if( seenUrls.containsKey(url.getUrl()) == true ){

				boolean disallowedUrl = false;
				String host = url.getHostName();

				// check if this is a disallowed URL
				if(hostRobotsMap.containsKey(host)){ // we've seen this host before
					
					String filepath = url.getFilePath();
					List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(getUserAgent());
					List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");
					
					// add default disallowed links to the set
					for( String link  : defaultDisallowed ){
						if( !disallowed.contains(link) ){
							disallowed.add(link);
						}
					}
					
					//for (String link : disallowed ){
					//	System.out.println(" disallowed link:" + link);
					//}
					
					System.out.println("Checking file path: " + filepath);
					
					if(disallowed == null){
						
						// there was no disallow map specified for this useragent. Default is that it is allowed.
						System.out.println("this is a allowed path");
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

				}

				if(disallowedUrl == false){ // we are allowed to query this url
					
					System.out.println("This url is allowed, getting ready to send a HEAD request");
					
					// Send a Head request to check if file has been modified.
					InetAddress address = InetAddress.getByName(url.getHostName());

					Socket connection = new Socket(address, url.getPortNo());

					out = connection.getOutputStream();
					in = connection.getInputStream();

					// get crawl delay before sending followup GET request.
					RobotsTxtInfo robotTxt = hostRobotsMap.get(host);

					// get by useragent, hardcoded
					int delay = 0;
					delay = robotTxt.getCrawlDelay(getUserAgent());

					if ( delay != 0  ){ // it was set, delay next request by specified time.
						System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
						Thread.sleep(delay*1000);
					}
					
					System.out.println("Sending HEAD...");
					// set a seen flag to indicate that we want to check if the seen url has been modified.
					sendHead(out, url, true);

					ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

					String code = response.m_headers.get("status-code").get(0);

					if(code.compareTo("304") == 0){ // not modified 
						//do nothing, no need to put back into the seen urls list
						System.out.println("HEAD request failed with code: " + code + "File has not been modified");

					}else if(code.compareTo("200") == 0){ // it has been modified

						// check if new modified file is within size limits

						if( isURLValid(response.m_headers) == true ){

							// wait crawl delay before sending followup GET request, if it was set.
							if ( delay != 0  ){ // it was set, delay next request by specified time.

								// Note: to make it more efficient, could just keep track of last queried time and 
								//   just re-enqueue. If the next time we get a URL for the same host, we just check
								//   if the current time is after the delay interval.
								System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
								Thread.sleep(delay*1000);
							}
							
							if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file
								
								
								// get new connection to send GET?
								connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();


								System.out.println("Sending GET...");
								sendGet(out, url);

								response = HTTPResponseParser.parseResponse("GET", in);

								code = response.m_headers.get("status-code").get(0);

								if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.


									//System.out.println("BODY of file for url: " + url.getUrl());
									//System.out.println("Body number of bytes: " + response.m_body.length());
									
									
									// update the data in the database.
									// DBWrapper;


								}else {
									System.out.println("GET request failed with code: " + code);
								}


								// update the seen list time, since the file was valid (it was modified), so a fresh 
								//   copy of this url must have a modified date after this one.
								Date now = Calendar.getInstance().getTime();
								seenUrls.put(url.getUrl(), now);
							}
							else{ // this is a .html file or just a link to another url, use JSOUP to extract links
								
								// get new connection to send GET?
								connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();


								System.out.println("Sending GET...");
								sendGet(out, url);

								response = HTTPResponseParser.parseResponse("GET", in);

								code = response.m_headers.get("status-code").get(0);

								if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.


									//System.out.println("BODY of file for url: " + url.getUrl());
									//System.out.println("Body number of bytes: " + response.m_body.length());
									
									//System.out.println("Extracting links from a html file!");
									
									
									//Document doc = Jsoup.connect(url.getUrl()).get();
									//System.out.println("BODY: " + response.m_body);
									Document doc = Jsoup.parse(response.m_body);
									Elements links = doc.select("a[href]");
									
									for( Element link : links  ){
										
										String abs_url = url.getUrl() + link.attr("href");
										
										
										//for(Attribute a : link.attributes()){
										//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
										//}
										
										System.out.println("link extracted: " + abs_url);
										//System.out.println("link element: " + link.toString());
										frontier_Q.add(new URLInfo(abs_url));
										
									}
									
									// update the data in the database.
									// DBWrapper;


								}else {
									System.out.println("GET request failed with code: " + code);
								}

								// update the seen list time, since the file was valid (it was modified), so a fresh 
								//   copy of this url must have a modified date after this one.
								Date now = Calendar.getInstance().getTime();
								seenUrls.put(url.getUrl(), now);
								
							}
						}


					}else{ // error sending HEAD request
						System.out.println("HEAD request failed with code: " + code);
						
					}
				}
				else{
					// don't query this URL, its disallowed
					System.out.println("This url is disallowed, Doing nothing");
					// disallowed, do nothing. but add to seen list.
					//Date now = Calendar.getInstance().getTime();
					//seenUrls.put(url.getUrl(), now);
				}


			} else { // We haven't seen this url before

				boolean disallowedUrl = false;
				String host = url.getHostName();
				
				System.out.println("We haven't seen this url before: " + url.getUrl());
				
				
				// check if this is a disallowed URL
				if(hostRobotsMap.containsKey(host)){ // we've seen this host before.
					
					System.out.println("We have seen this host before: " + host);
					
					String filepath = url.getFilePath();
					List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(getUserAgent());
					List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");
					
					// add default disallowed links to the set
					for( String link  : defaultDisallowed ){
						if( !disallowed.contains(link) ){
							disallowed.add(link);
						}
					}
					
					//for (String link : disallowed ){
					//	System.out.println(" disallowed link:" + link);
					//}
					
					
					System.out.println("Checking file path: " + filepath);
					
					if(disallowed == null){
						// no disallowed URL list was specified for this userAgent, default is allowed.
						System.out.println("this is a allowed path");
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
						InetAddress address = InetAddress.getByName(url.getHostName());

						Socket connection = new Socket(address, url.getPortNo());

						out = connection.getOutputStream();
						in = connection.getInputStream();

						// check the crawl-delay and wait 
						int delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());

						if (delay != 0 ){
							// Note: to make it more efficient, could just keep track of last queried time and 
							//   just re-enqueue. If the next time we get a URL for the same host, we just check
							//   if the current time is after the delay interval.
							System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
							Thread.sleep(delay*1000);
						}
						
						System.out.println("Sending HEAD...");
						// set a seen flag to indicate that we haven't seen url, so we dont want to check if it has been modified.
						sendHead(out, url, false);

						ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

						String code = response.m_headers.get("status-code").get(0);

						if( code.compareTo("200") == 0 ){

							if ( delay != 0  ){ // it was set, delay next request by specified time.

								// Note: to make it more efficient, could just keep track of last queried time and 
								//   just re-enqueue. If the next time we get a URL for the same host, we just check
								//   if the current time is after the delay interval.
								System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
								Thread.sleep(delay*1000);
							}
							
							if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file
								
								System.out.println("Downloading an .xml file!");
								
								// get new connection to send GET?
								connection = new Socket(address, url.getPortNo());
	
								out = connection.getOutputStream();
								in = connection.getInputStream();
								
								
								System.out.println("Sending GET...");
								sendGet(out, url);
	
								response = HTTPResponseParser.parseResponse("GET", in);
	
								code = response.m_headers.get("status-code").get(0);
	
								if( code.compareTo("200") == 0 ){
									
									//System.out.println("BODY of file for url: " + url.getUrl());
									//System.out.println("Body number of bytes: " + response.m_body.length());
								
									// TODO put this in the database
									// update the data in the database.
									// DBWrapper;
								} else{
									System.out.println("GET request failed with code: " + code);
								}
								
								// update the seen list, since this is a url we have not seen before.
								Date now = Calendar.getInstance().getTime();
								seenUrls.put(url.getUrl(), now);
								
							} 
							else{ // this is a .html file or just a link to another url, use JSOUP to extract links
								
								
								// get new connection to send GET?
								connection = new Socket(address, url.getPortNo());
	
								out = connection.getOutputStream();
								in = connection.getInputStream();
								
								
								System.out.println("Sending GET...");
								sendGet(out, url);
	
								response = HTTPResponseParser.parseResponse("GET", in);
	
								code = response.m_headers.get("status-code").get(0);
	
								if( code.compareTo("200") == 0 ){
									
									//System.out.println("BODY of file for url: " + url.getUrl());
									//System.out.println("Body number of bytes: " + response.m_body.length());
									
									System.out.println("Extracting links from a html file!");
									
									//Document doc = Jsoup.connect(url.getUrl()).get();
									//System.out.println("BODY: " + response.m_body);
									
									Document doc = Jsoup.parse(response.m_body);
									Elements links = doc.select("a[href]");
									
									for( Element link : links  ){
										
										String abs_url = url.getUrl() + link.attr("href");
										
										//for(Attribute a : link.attributes()     ){
										//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
										//}
										 
										System.out.println("link extracted: " + abs_url);
										URLInfo info = new URLInfo(abs_url);
										frontier_Q.add(info);
										
									}
									
									
									// TODO put this in the database
									// update the data in the database.
									// DBWrapper;
								} else{
									System.out.println("GET request failed with code: " + code);
								}
								
								//System.out.println(" Whole doc body?: \n"+ doc.toString());
								
								Date now = Calendar.getInstance().getTime();
								seenUrls.put(url.getUrl(), now);
								
							}

						} else{ // HEAD request failed...
							System.out.println("HEAD request failed with code: "+ code);
						}


					} else{
						System.out.println("This url is disallowed, Doing nothing");
						// disallowed, do nothing. but add to seen list.
						Date now = Calendar.getInstance().getTime();
						seenUrls.put(url.getUrl(), now);
					}


				}else{ // have not seen this host before, need to get the robots.txt file.
					
					System.out.println("We haven't seen this host before: " + host);
					
					// Send a Head request to check if file has been modified.
					InetAddress address = InetAddress.getByName(url.getHostName());

					Socket connection = new Socket(address, url.getPortNo());

					out = connection.getOutputStream();
					in = connection.getInputStream();
					
					System.out.println("Sending a request for Robots: ");
					sendGetRobots(out, url);

					RobotTuple response = robotParser.parseRobotResponse(in); 

					String code = response.m_headers.get("status-code").get(0);

					if( code.compareTo("200") == 0 ){

						RobotsTxtInfo rob = response.getRobot();
						//System.out.println("PRINT");

						// insert the robots file for this host.
						hostRobotsMap.put(host, rob);
						
						//System.out.println("GOT ROBOTS file\n Disallowed links for host: " + host +"\n");
						
						List<String> disallowedLinks = rob.getDisallowedLinks(getUserAgent());
						List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");
						
						// add default disallowed links to the set
						for( String link  : defaultDisallowed ){
							if( !disallowedLinks.contains(link) ){
								disallowedLinks.add(link);
							}
						}
						
						//for (String link : disallowedLinks ){
						//	System.out.println(" disallowed link:" + link);
						//}
						
						System.out.println("checking if the requested url is accessible: " + url.getFilePath());
						
						if( disallowedLinks != null){
							
							String filepath = url.getFilePath();
							
							for(String path  : disallowedLinks ){
								
								String disallowedPath;
								
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
								
								System.out.println("This url is allowed, adding it into the QUEUE");
								frontier_Q.add(url);
							}
						} else{
							// also allowed? this useragent is not specified in the robots.txt file.
							System.out.println("This url is allowed, adding it into the QUEUE");
							frontier_Q.add(url);
						}

					}else{ // no robots.txt file?? I guess use defaults....?

						System.out.println("Robots.txt GET request failed with code: " + code);

					}


				}


			}


		}


	}

	

	
	public static void main(String[] argv) throws IOException, InterruptedException{

		if( argv.length < 3  ){

			System.out.println("Invalid number of arguments");
			System.exit(1);
			
		}else{

			String startUrl = argv[0];
			String dbDir = argv[1];
			int maxSize = Integer.valueOf(argv[2]);
			
			
			XPathCrawlerFactory crawlerFactory = new XPathCrawlerFactory();

			XPathCrawler crawler =  crawlerFactory.getCrawler();

			crawler.setStartUrl(startUrl);
			crawler.setDbDir(dbDir);
			crawler.setMaxSize(maxSize);
			crawler.setUserAgent("cis455crawler");
			crawler.enqueueURL(new URLInfo(startUrl));

			crawler.run();
			
			System.out.println("Done Crawling");
			
			
		}


	}

}
