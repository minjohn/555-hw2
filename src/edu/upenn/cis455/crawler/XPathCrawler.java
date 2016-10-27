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
import java.util.TimeZone;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;

import edu.upenn.cis455.crawler.RobotParser.RobotTuple;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DBWrapper.WebPage;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.sleepycat.je.DatabaseException;


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

	public void setDbDir(String dbDir) throws DatabaseException, IOException {

		this.dbwrapper = new DBWrapper(dbDir);

		this.dbDir = dbDir;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public int getMaxNumPages(){
		return maxNumPages;
	}

	public void setMaxPages(int pages){
		this.maxNumPages = pages;
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
	int maxNumPages = -1;
	DBWrapper dbwrapper;

	Queue<URLInfo> frontier_Q = new LinkedList<URLInfo>();
	HashMap<String, Date> seenUrls = new HashMap<String, Date>(); // url and last time we requested it.
	HashMap<String, RobotsTxtInfo> hostRobotsMap = new HashMap<String, RobotsTxtInfo>();

	String[] fileTypes = { "text/html", "text/xml", "application/html"};
	Set<String> acceptableFileTypes = new HashSet<String>(Arrays.asList(fileTypes));

	RobotParser robotParser = new RobotParser();


	private void sendHead(OutputStream out, URLInfo info, boolean seen){

		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		PrintWriter writer = new PrintWriter(out,true);

		if(seen == true){ // seen this url, checking if it was modified.

			writer.println("HEAD " + info.getUrl() +" HTTP/1.1");
			writer.println("Host: " + info.getHostName());
			writer.println("User-Agent: cis455crawler");

			String url = info.getUrl();
			String date = dateFormat.format(dbwrapper.getWebPageLastAccessed(url));

			//System.out.println("lastAccessed date: " + date);

			writer.println("If-Modified-Since: " + date);
			writer.println("Connection: close");
			writer.println("\n");

		}else{

			writer.println("HEAD " + info.getUrl() +" HTTP/1.1");
			writer.println("Host: " + info.getHostName());
			writer.println("User-Agent: cis455crawler");
			writer.println("Connection: close");
			writer.println("\n");

		}

	}

	// We assume that the socket is already connected to the host?
	private void sendGet(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET " + info.getUrl() +" HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-Agent: cis455crawler");
		writer.println("Connection: close");
		writer.println("\n");


	}

	// We assume that the socket is already connected to the host?
	private void sendGetRobots(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET /robots.txt HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-Agent: cis455crawler");
		writer.println("Connection: close");
		writer.println("\n");

	}


	private boolean isURLValidSSL(HttpsURLConnection con ){

		boolean valid = false;
		int length = 0;
		String contentType = "";

		if( con.getContentLength() != -1  ){

			length = con.getContentLength();

		}else{
			System.out.println("content-length is null");
		}

		if (con.getContentType()  != null){
			contentType = con.getContentType();
		}else{
			System.out.println("content-type is not known");
		}

		if( length != 0 && length < maxSize ) {

			//System.out.println("length condition met! " + String.valueOf(length) + " " + String.valueOf(maxSize));

			valid = true;
		} else{

			return false;
		}

		if( contentType.compareTo("") != 0 && (acceptableFileTypes.contains(contentType) || contentType.contains("+xml") )){
			valid = true;
		}else{
			return false;
		}

		return valid;

	}


	private boolean isURLValid(HashMap<String, List<String>> headers ){

		boolean valid = false;
		int length = 0;
		String contentType = "";

		if( headers.containsKey("content-length")  ){

			if(headers.get("content-length") == null){
				System.out.println("content-length is null");
			}

			String length_str = headers.get("content-length").get(0);

			//System.out.println(length_str);
			length = Integer.valueOf(length_str);

		}

		if (headers.containsKey("content-type")){
			contentType = headers.get("content-type").get(0);
		}

		if( length != 0 && length < maxSize ) {

			//System.out.println("length condition met! " + String.valueOf(length) + " " + String.valueOf(maxSize));

			valid = true;
		} else{

			return false;
		}

		if( contentType.compareTo("") != 0 && (acceptableFileTypes.contains(contentType) || contentType.contains("+xml") )){
			valid = true;
		}else{
			return false;
		}

		return valid;

	}


	public void run() throws IOException, InterruptedException{

		OutputStream out = null;
		InputStream in = null;
		int counter = 0;

		try {

			// main loop
			while( frontier_Q.size() > 0 ){

				//			URLInfo urls[] = new URLInfo[frontier_Q.size()];
				//			frontier_Q.toArray(urls);
				//			System.out.println("URLs in Queue ======");
				//			for(URLInfo text  : urls  ){
				//				System.out.println(text.getUrl());
				//			}
				//			System.out.println("END URLs in Queue ======");

				// for testing purposes, limit the number of pages we attempt to crawl.
				if( getMaxNumPages() != -1 && counter == getMaxNumPages() ){
					break;
				}

				URLInfo url = frontier_Q.remove();
				if( getMaxNumPages() != -1 ){
					counter ++;
				}

				//System.out.println("dequeued URL:" + url.getUrl());

				// we've seen the url before
				if( seenUrls.containsKey(url.getUrl()) == true ){

					//System.out.println("We've seen this url before:" + url.getUrl());

					boolean disallowedUrl = false;
					String host = url.getHostName();

					// check if this is a disallowed URL
					if(hostRobotsMap.containsKey(host)){ // we've seen this host before

						String filepath = url.getFilePath();
						List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(getUserAgent());
						List<String> defaultDisallowed = hostRobotsMap.get(host).getDisallowedLinks("*");

						// add default disallowed links to the set
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

						//for (String link : disallowed ){
						//	System.out.println(" disallowed link:" + link);
						//}

						//System.out.println("Checking file path: " + filepath);

						if(disallowed == null){

							// there was no disallow map specified for this useragent. Default is that it is allowed.
							//System.out.println("this is a allowed path");
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

						// get crawl delay before sending followup GET request.
						RobotsTxtInfo robotTxt = hostRobotsMap.get(host);
						// get by useragent, hardcoded
						int delay = 0;
						delay = robotTxt.getCrawlDelay(getUserAgent());

						if ( delay != 0  ){ // it was set, delay next request by specified time.
							//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
							Thread.sleep(delay*1000);
						}

						//System.out.println("This url is allowed, getting ready to send a HEAD request");

						//boolean ssl = false;
						if(url.getUrl().startsWith("https:")){

							//ssl = true;

							try {
								URL url_ssl = new URL(url.getUrl()); 

								HttpsURLConnection con = (HttpsURLConnection)url_ssl.openConnection();
								con.setDoOutput (true);
								con.setRequestProperty("Host", url.getHostName());
								con.addRequestProperty("User-Agent", "cis455crawler");
								con.setRequestMethod("HEAD");

								SimpleDateFormat dateFormat = new SimpleDateFormat(
										"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);
								dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
								String date = dateFormat.format(dbwrapper.getWebPageLastAccessed(url.getUrl()));

								//System.out.println("lastAccessed date: " + date);

								con.addRequestProperty("If-Modified-Since", date);
								con.addRequestProperty("Connection", "close");

								con.connect();

								//in  = con.getInputStream();
								//TODO add the processing stuff here

								//ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

								String code =  String.valueOf(con.getResponseCode());


								if(code.compareTo("304") == 0){ // not modified 
									//do nothing, no need to put back into the seen urls list
									//System.out.println("HEAD request failed with code: " + code + "File has not been modified");
									System.out.println( url.getUrl() + ": Not Modified");

									// dont retrieve it again if its an xml doc, but probably re-extract the links of a html file
									if( !url.getUrl().endsWith(".xml") ){

										String webpage_content = dbwrapper.getWebPage(url.getUrl());

										if(webpage_content != null){
											Document doc = Jsoup.parse(webpage_content);
											Elements links = doc.select("a[href]");

											for( Element link : links  ){

												//String abs_url = url.getUrl() + link.attr("href");
												String abs_url = url.getBaseUrl() + link.attr("href");

												//for(Attribute a : link.attributes()){
												//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
												//}

												//System.out.println("link extracted: " + abs_url);
												//System.out.println("link element: " + link.toString());
												frontier_Q.add(new URLInfo(abs_url));

											}
										}
										else{
											System.out.println("Could not retrieve webpage content from database!");
										}
									}


								}else if(code.compareTo("200") == 0){ // it has been modified

									// check if new modified file is within size limits

									if( isURLValidSSL(con) == true ){

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


												try {
													url_ssl = new URL(url.getUrl()); 

													con = (HttpsURLConnection)url_ssl.openConnection();
													con.setDoOutput (true);
													con.setRequestProperty("Host", url.getHostName());
													con.setRequestMethod("GET");
													con.addRequestProperty("User-Agent", "cis455crawler");
													con.addRequestProperty("Connection", "close");

													System.out.println( url.getUrl() + ": Downloading");
													con.connect();

													in  = con.getInputStream();

												} catch (MalformedURLException e) {

													e.printStackTrace();
													continue;

												} catch (IOException e) {

													e.printStackTrace();
													continue;

												}
												
												int content_length = con.getContentLength();

												if(content_length != -1){
													String body = HTTPResponseParser.parseResponseSSL(in, content_length);

													code = String.valueOf(con.getResponseCode());

													if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.

														//System.out.println("BODY of file for url: " + url.getUrl());
														//System.out.println("Body number of bytes: " + response.m_body.length());

														// store the page in database.
														if( dbwrapper.containsWebPage(body) == true){
															//System.out.println( url.getUrl() + ": Content Seen");
														}else{
															dbwrapper.putWebPage(url.getUrl(), body);
														}

													}else {
														System.out.println("GET request failed with code: " + code);
													}
												}												

												// update the seen list time, since the file was valid (it was modified), so a fresh 
												//   copy of this url must have a modified date after this one.
												Date now = Calendar.getInstance().getTime();
												seenUrls.put(url.getUrl(), now);

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

												try {
													url_ssl = new URL(url.getUrl()); 

													con = (HttpsURLConnection)url_ssl.openConnection();
													con.setDoOutput (true);
													con.setRequestProperty("Host", url.getHostName());
													con.setRequestMethod("GET");
													con.addRequestProperty("User-Agent", "cis455crawler");
													con.addRequestProperty("Connection", "close");

													System.out.println( url.getUrl() + ": Downloading");
													con.connect();

													in  = con.getInputStream();
													//out = con.getOutputStream();

												} catch (MalformedURLException e) {

													e.printStackTrace();
													continue;

												} catch (IOException e) {

													e.printStackTrace();
													continue;

												}


												int content_length = con.getContentLength();

												if(content_length != -1){

													String body = HTTPResponseParser.parseResponseSSL(in, content_length);

													code = String.valueOf(con.getResponseCode());

													if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.
														webpage_content = body;

														// store the page in database.
														if( dbwrapper.containsWebPage(body) == true){
															//System.out.println( url.getUrl() + ": Content Seen");
															webpage_content = null;
														}else{
															dbwrapper.putWebPage(url.getUrl(), webpage_content);
														}
													}else {
														System.out.println("GET request failed with code: " + code);
														webpage_content = null;
													}
												}
												else{
													webpage_content = null;
												}
											}

											if( webpage_content != null ){ // got a modified version of the seen url.


												//System.out.println("BODY of file for url: " + url.getUrl());
												//System.out.println("Body number of bytes: " + response.m_body.length());

												//System.out.println("Extracting links from a html file!");


												//Document doc = Jsoup.connect(url.getUrl()).get();
												//System.out.println("BODY: " + response.m_body);
												Document doc = Jsoup.parse(webpage_content);
												Elements links = doc.select("a[href]");

												for( Element link : links  ){

													//String abs_url = url.getUrl() + link.attr("href");
													String abs_url = url.getBaseUrl() + link.attr("href");

													//for(Attribute a : link.attributes()){
													//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
													//}

													//System.out.println("link extracted: " + abs_url);
													//System.out.println("link element: " + link.toString());
													frontier_Q.add(new URLInfo(abs_url));

												}						

											}else{
												//System.out.println("Could not extract links. webpage content is null... " );
											}

											// update the seen list time, since the file was valid (it was modified), so a fresh 
											//   copy of this url must have a modified date after this one.
											Date now = Calendar.getInstance().getTime();
											seenUrls.put(url.getUrl(), now);

										}
									}
									else{ // fails a mime-type or size requirement
										System.out.println( url.getUrl() + ": Not Downloading");
									}

								}else{ // error sending HEAD request
									System.out.println("HEAD request failed with code: " + code);

								}

								//TODO MARKER end -------------------------------------------

							} catch (MalformedURLException e) {

								e.printStackTrace();
								continue;

							} catch (IOException e) {

								e.printStackTrace();
								continue;

							}

						} else { // handle http separately

							// Send a Head request to check if file has been modified.
							InetAddress address = InetAddress.getByName(url.getHostName());

							Socket connection = new Socket(address, url.getPortNo());

							out = connection.getOutputStream();
							in = connection.getInputStream();

							sendHead(out, url, true);

							ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

							String code = response.m_headers.get("status-code").get(0);

							if(code.compareTo("304") == 0){ // not modified 
								//do nothing, no need to put back into the seen urls list
								//System.out.println("HEAD request failed with code: " + code + "File has not been modified");
								System.out.println( url.getUrl() + ": Not Modified");

								// dont retrieve it again if its an xml doc, but probably re-extract the links of a html file
								if( !url.getUrl().endsWith(".xml") ){

									String webpage_content = dbwrapper.getWebPage(url.getUrl());

									if(webpage_content != null){
										Document doc = Jsoup.parse(webpage_content);
										Elements links = doc.select("a[href]");

										for( Element link : links  ){

											//String abs_url = url.getUrl() + link.attr("href");
											String abs_url = url.getBaseUrl() + link.attr("href");

											//for(Attribute a : link.attributes()){
											//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
											//}

											//System.out.println("link extracted: " + abs_url);
											//System.out.println("link element: " + link.toString());
											frontier_Q.add(new URLInfo(abs_url));

										}
									}
									else{
										System.out.println("Could not retrieve webpage content from database!");
									}
								}


							}else if(code.compareTo("200") == 0){ // it has been modified

								// check if new modified file is within size limits

								if( isURLValid(response.m_headers) == true ){

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
											sendGet(out, url);




											response = HTTPResponseParser.parseResponse("GET", in);

											code = response.m_headers.get("status-code").get(0);

											if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.


												//System.out.println("BODY of file for url: " + url.getUrl());
												//System.out.println("Body number of bytes: " + response.m_body.length());

												// store the page in database.
												if( dbwrapper.containsWebPage(response.m_body) == true){
													//System.out.println( url.getUrl() + ": Content Seen");
												}else{
													dbwrapper.putWebPage(url.getUrl(), response.m_body);
												}

											}else {
												System.out.println("GET request failed with code: " + code);
											}

											// update the seen list time, since the file was valid (it was modified), so a fresh 
											//   copy of this url must have a modified date after this one.
											Date now = Calendar.getInstance().getTime();
											seenUrls.put(url.getUrl(), now);

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


											// get new connection to send GET?
											address = InetAddress.getByName(url.getHostName());
											connection = new Socket(address, url.getPortNo());

											out = connection.getOutputStream();
											in = connection.getInputStream();

											System.out.println( url.getUrl() + ": Downloading");
											sendGet(out, url);


											response = HTTPResponseParser.parseResponse("GET", in);

											code = response.m_headers.get("status-code").get(0);

											if( code.compareTo("200") == 0 ){ // got a modified version of the seen url.
												webpage_content = response.m_body;

												// store the page in database.
												if( dbwrapper.containsWebPage(response.m_body) == true){
													//System.out.println( url.getUrl() + ": Content Seen");
													webpage_content = null;
												}else{
													dbwrapper.putWebPage(url.getUrl(), webpage_content);
												}
											}else {
												System.out.println("GET request failed with code: " + code);
												webpage_content = null;
											}
										}

										if( webpage_content != null ){ // got a modified version of the seen url.


											//System.out.println("BODY of file for url: " + url.getUrl());
											//System.out.println("Body number of bytes: " + response.m_body.length());

											//System.out.println("Extracting links from a html file!");


											//Document doc = Jsoup.connect(url.getUrl()).get();
											//System.out.println("BODY: " + response.m_body);
											Document doc = Jsoup.parse(webpage_content);
											Elements links = doc.select("a[href]");

											for( Element link : links  ){

												//String abs_url = url.getUrl() + link.attr("href");
												String abs_url = url.getBaseUrl() + link.attr("href");

												//for(Attribute a : link.attributes()){
												//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
												//}

												//System.out.println("link extracted: " + abs_url);
												//System.out.println("link element: " + link.toString());
												frontier_Q.add(new URLInfo(abs_url));

											}						

										}else{
											//System.out.println("Could not extract links. webpage content is null... " );
										}

										// update the seen list time, since the file was valid (it was modified), so a fresh 
										//   copy of this url must have a modified date after this one.
										Date now = Calendar.getInstance().getTime();
										seenUrls.put(url.getUrl(), now);

									}
								}
								else{ // fails a mime-type or size requirement
									System.out.println( url.getUrl() + ": Not Downloading");
								}

							}else{ // error sending HEAD request
								System.out.println("HEAD request failed with code: " + code);

							}

						}

						//System.out.println("Sending HEAD...");
						// set a seen flag to indicate that we want to check if the seen url has been modified.

					}
					else{
						// don't query this URL, its disallowed
						System.out.println( url.getUrl() + ": Restricted. Not downloading");
						// disallowed, do nothing. but add to seen list.
						//Date now = Calendar.getInstance().getTime();
						//seenUrls.put(url.getUrl(), now);
					}


				} else { // We haven't seen this url before

					// TODO haven't seen this url before...


					boolean disallowedUrl = false;
					String host = url.getHostName();

					//System.out.println("We haven't seen this url before: " + url.getUrl());


					// check if this is a disallowed URL
					if(hostRobotsMap.containsKey(host)){ // we've seen this host before.

						//System.out.println("We have seen this host before: " + host);

						String filepath = url.getFilePath();
						List<String> disallowed = hostRobotsMap.get(host).getDisallowedLinks(getUserAgent());
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

						//for (String link : disallowed ){
						//	System.out.println(" disallowed link:" + link);
						//}


						//System.out.println("Checking file path: " + filepath);

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

								ssl = true;

								try {
									URL url_ssl = new URL(url.getUrl()); 

									HttpsURLConnection con = (HttpsURLConnection)url_ssl.openConnection();
									con.setDoOutput (true);
									con.setRequestProperty("Host", url.getHostName());
									con.setRequestMethod("HEAD");
									con.addRequestProperty("User-Agent", "cis455crawler");
									con.addRequestProperty("Connection", "close");

									// check the crawl-delay and wait 
									int delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());

									if (delay != 0 ){
										// Note: to make it more efficient, could just keep track of last queried time and 
										//   just re-enqueue. If the next time we get a URL for the same host, we just check
										//   if the current time is after the delay interval.
										//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
										Thread.sleep(delay*1000);
									}

									con.connect();

									in  = con.getInputStream();


									//TODO add the processing stuff here

									String code =  String.valueOf(con.getResponseCode());

									if( code.compareTo("200") == 0 ){

										if( isURLValidSSL(con) == true ){

											if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file

												String webpage_content;

												if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
													System.out.println( url.getUrl() + ": Content Seen");
												}
												else{

													delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());
													if ( delay != 0  ){ // it was set, delay next request by specified time.

														// Note: to make it more efficient, could just keep track of last queried time and 
														//   just re-enqueue. If the next time we get a URL for the same host, we just check
														//   if the current time is after the delay interval.
														//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
														Thread.sleep(delay*1000);
													}


													//System.out.println("Downloading an .xml file!");

													// get new connection to send GET?

													try {
														url_ssl = new URL(url.getUrl()); 

														con = (HttpsURLConnection)url_ssl.openConnection();
														con.setDoOutput (true);
														con.setRequestProperty("Host", url.getHostName());
														con.setRequestMethod("GET");
														con.addRequestProperty("User-Agent", "cis455crawler");
														con.addRequestProperty("Connection", "close");

														System.out.println( url.getUrl() + ": Downloading");
														con.connect();

														in  = con.getInputStream();
														//out = con.getOutputStream();

													} catch (MalformedURLException e) {

														e.printStackTrace();
														continue;

													} catch (IOException e) {

														e.printStackTrace();
														continue;

													}
													

													int content_length = con.getContentLength();

													if(content_length != -1){

														String body = HTTPResponseParser.parseResponseSSL(in, content_length);

														code =  String.valueOf(con.getResponseCode());

														if( code.compareTo("200") == 0 ){

															//System.out.println("BODY of file for url: " + url.getUrl());
															//System.out.println("Body number of bytes: " + response.m_body.length());
															// update the data in the database.
															if( dbwrapper.containsWebPage(body) == true){
																//System.out.println( url.getUrl() + ": Content Seen");
															}
															else{
																dbwrapper.putWebPage(url.getUrl(), body);
															}

														} else{
															System.out.println("GET request failed with code: " + code);
														}
													}

													// update the seen list, since this is a url we have not seen before.
													Date now = Calendar.getInstance().getTime();
													seenUrls.put(url.getUrl(), now);
												}

											} 
											else{ // this is a .html file or just a link to another url, use JSOUP to extract links

												String webpage_content;

												if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
													System.out.println( url.getUrl() + ": Content Seen");

												}
												else{
													delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());
													if ( delay != 0  ){ // it was set, delay next request by specified time.

														// Note: to make it more efficient, could just keep track of last queried time and 
														//   just re-enqueue. If the next time we get a URL for the same host, we just check
														//   if the current time is after the delay interval.
														//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
														Thread.sleep(delay*1000);
													}


													// get new connection to send GET?
													
														try {
															 url_ssl = new URL(url.getUrl()); 

															con = (HttpsURLConnection)url_ssl.openConnection();
															con.setDoOutput (true);
															con.setRequestProperty("Host", url.getHostName());
															con.setRequestMethod("GET");
															con.addRequestProperty("User-Agent", "cis455crawler");
															con.addRequestProperty("Connection", "close");

															System.out.println( url.getUrl() + ": Downloading");
															con.connect();

															in  = con.getInputStream();
															//out = con.getOutputStream();

														} catch (MalformedURLException e) {

															e.printStackTrace();
															continue;

														} catch (IOException e) {

															e.printStackTrace();
															continue;

														}
													
														
														
														
														int content_length = con.getContentLength();

														if(content_length != -1){

															String body = HTTPResponseParser.parseResponseSSL(in, content_length);

															code = String.valueOf(con.getResponseCode());


															if( code.compareTo("200") == 0 ){
																webpage_content = body;

																// store the page in database.
																if( dbwrapper.containsWebPage(body) == true){
																	//System.out.println( url.getUrl() + ": Content Seen");

																	// skip the extracting
																	webpage_content = null;
																}
																else{
																	dbwrapper.putWebPage(url.getUrl(), webpage_content);
																}
															}else{
																System.out.println("GET request failed with code: " + code);
																webpage_content = null;
															}
														}else{
															
															webpage_content = null;
															
														}
												}

												if( webpage_content != null ){

													//System.out.println("BODY of file for url: " + url.getUrl());
													//System.out.println("Body number of bytes: " + response.m_body.length());

													//System.out.println("Extracting links from a html file!");

													//Document doc = Jsoup.connect(url.getUrl()).get();
													//System.out.println("BODY: " + response.m_body);

													Document doc = Jsoup.parse(webpage_content);
													Elements links = doc.select("a[href]");

													for( Element link : links  ){

														//String abs_url = url.getUrl() + link.attr("href");
														String abs_url = url.getBaseUrl() + link.attr("href");

														//for(Attribute a : link.attributes()     ){
														//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
														//}

														//System.out.println("link extracted: " + abs_url);
														URLInfo info = new URLInfo(abs_url);
														frontier_Q.add(info);

													}

												} else{
													//System.out.println("Could not extract links. webpage content is null... " );
												}

												//System.out.println(" Whole doc body?: \n"+ doc.toString());

												Date now = Calendar.getInstance().getTime();
												seenUrls.put(url.getUrl(), now);

											}
										}
										else{// fails a mime-type or size requirement
											System.out.println( url.getUrl() + ": Not Downloading");
										}

									} else{ // HEAD request failed...
										System.out.println("HEAD request failed with code: "+ code);
									}


								} catch (MalformedURLException e) {

									e.printStackTrace();
									continue;

								} catch (IOException e) {

									e.printStackTrace();
									continue;

								}

							} else { // handle http separately


								// check the crawl-delay and wait 
								int delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());

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
								sendHead(out, url, false);


								ResponseTuple response = HTTPResponseParser.parseResponse("HEAD", in);

								String code = response.m_headers.get("status-code").get(0);

								if(code.compareTo("304") == 0){ // not modified 
									//do nothing, no need to put back into the seen urls list
									//System.out.println("HEAD request failed with code: " + code + "File has not been modified");
									System.out.println( url.getUrl() + ": Not Modified");

									// dont retrieve it again if its an xml doc, but probably re-extract the links of a html file
									if( !url.getUrl().endsWith(".xml") ){

										String webpage_content = dbwrapper.getWebPage(url.getUrl());

										if(webpage_content != null){
											Document doc = Jsoup.parse(webpage_content);
											Elements links = doc.select("a[href]");

											for( Element link : links  ){

												//String abs_url = url.getUrl() + link.attr("href");
												String abs_url = url.getBaseUrl() + link.attr("href");

												//for(Attribute a : link.attributes()){
												//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
												//}

												//System.out.println("link extracted: " + abs_url);
												//System.out.println("link element: " + link.toString());
												frontier_Q.add(new URLInfo(abs_url));

											}
										}
										else{
											System.out.println("Could not retrieve webpage content from database!");
										}
									}
								}
								else if( code.compareTo("200") == 0 ){

									if( isURLValid(response.m_headers) == true ){

										if( url.getUrl().endsWith(".xml") ){ // just get the contents of an .xml file

											String webpage_content;

											if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
												System.out.println( url.getUrl() + ": Content Seen");
											}
											else{

												delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());
												if ( delay != 0  ){ // it was set, delay next request by specified time.

													// Note: to make it more efficient, could just keep track of last queried time and 
													//   just re-enqueue. If the next time we get a URL for the same host, we just check
													//   if the current time is after the delay interval.
													//System.out.println("Crawler delay was specified for this UserAgent, sleeping...");
													Thread.sleep(delay*1000);
												}


												//System.out.println("Downloading an .xml file!");

												

												// get new connection to send GET?
												address = InetAddress.getByName(url.getHostName());
												connection = new Socket(address, url.getPortNo());

												out = connection.getOutputStream();
												in = connection.getInputStream();

												System.out.println( url.getUrl() + ": Downloading");
												sendGet(out, url);
												


												response = HTTPResponseParser.parseResponse("GET", in);

												code = response.m_headers.get("status-code").get(0);

												if( code.compareTo("200") == 0 ){

													//System.out.println("BODY of file for url: " + url.getUrl());
													//System.out.println("Body number of bytes: " + response.m_body.length());
													// update the data in the database.
													if( dbwrapper.containsWebPage(response.m_body) == true){
														//System.out.println( url.getUrl() + ": Content Seen");
													}
													else{
														dbwrapper.putWebPage(url.getUrl(), response.m_body);
													}

												} else{
													System.out.println("GET request failed with code: " + code);
												}

												// update the seen list, since this is a url we have not seen before.
												Date now = Calendar.getInstance().getTime();
												seenUrls.put(url.getUrl(), now);
											}

										} 
										else{ // this is a .html file or just a link to another url, use JSOUP to extract links

											String webpage_content;

											if ( ( webpage_content = dbwrapper.getWebPage(url.getUrl()) ) != null  ){ // check if we have the page in the database, first.
												System.out.println( url.getUrl() + ": Content Seen");

											}
											else{
												delay = hostRobotsMap.get(host).getCrawlDelay(getUserAgent());
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
												sendGet(out, url);


												response = HTTPResponseParser.parseResponse("GET", in);

												code = response.m_headers.get("status-code").get(0);

												if( code.compareTo("200") == 0 ){
													webpage_content = response.m_body;

													// store the page in database.
													if( dbwrapper.containsWebPage(response.m_body) == true){
														//System.out.println( url.getUrl() + ": Content Seen");

														// skip the extracting
														webpage_content = null;
													}
													else{
														dbwrapper.putWebPage(url.getUrl(), webpage_content);
													}
												}else{
													System.out.println("GET request failed with code: " + code);
													webpage_content = null;
												}
											}

											if( webpage_content != null ){

												//System.out.println("BODY of file for url: " + url.getUrl());
												//System.out.println("Body number of bytes: " + response.m_body.length());

												//System.out.println("Extracting links from a html file!");

												//Document doc = Jsoup.connect(url.getUrl()).get();
												//System.out.println("BODY: " + response.m_body);

												Document doc = Jsoup.parse(webpage_content);
												Elements links = doc.select("a[href]");

												for( Element link : links  ){

													//String abs_url = url.getUrl() + link.attr("href");
													String abs_url = url.getBaseUrl() + link.attr("href");

													//for(Attribute a : link.attributes()     ){
													//	System.out.println("key: " + a.getKey() + " value: " + a.getValue());
													//}

													//System.out.println("link extracted: " + abs_url);
													URLInfo info = new URLInfo(abs_url);
													frontier_Q.add(info);

												}

											} else{
												//System.out.println("Could not extract links. webpage content is null... " );
											}

											//System.out.println(" Whole doc body?: \n"+ doc.toString());

											Date now = Calendar.getInstance().getTime();
											seenUrls.put(url.getUrl(), now);

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
							//System.out.println("This url is disallowed, Doing nothing");
							System.out.println( url.getUrl() + ": Restricted. Not downloading");
							// disallowed, do nothing. but add to seen list.
							Date now = Calendar.getInstance().getTime();
							seenUrls.put(url.getUrl(), now);
						}


					}else{ // have not seen this host before, need to get the robots.txt file.

						RobotsTxtInfo robotTxt = null;
						String robotTxtContent;

						String roboturl = url.getUrl()+"robots.txt";
						//System.out.println("robot: " + roboturl);

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

								ssl = true;

								try {
									URL url_ssl = new URL(roboturl); 

									HttpsURLConnection con = (HttpsURLConnection)url_ssl.openConnection();
									con.setDoOutput (true);
									con.setRequestProperty("Host", url.getHostName());
									con.setRequestMethod("GET");
									con.addRequestProperty("User-Agent", "cis455crawler");
									con.addRequestProperty("Connection", "close");

									System.out.println( url.getUrl() + "robots.txt: Downloading");
									con.connect();

									in  = con.getInputStream();

									String robotBody = robotParser.parseRobotResponseSSL(in);
									
									System.out.println("robot.txt body: " + robotBody);
									
									RobotParser parser = new RobotParser();
									robotTxt = parser.parseRobotString(robotBody);
									
									String code = String.valueOf(con.getResponseCode());
									
									//System.out.println("robot.txt response code: " + code);
									
									if( code.compareTo("200") == 0 ){

										// insert the robots file for this host.
										hostRobotsMap.put(host, robotTxt);

										//store file in database
										dbwrapper.putWebPage(roboturl, robotBody);

									}
									else{ // no robots.txt file?? I guess use defaults....?

										System.out.println("Robots.txt GET request failed with code: " + code);

									}
									

								} catch (MalformedURLException e) {

									e.printStackTrace();
									continue;

								} catch (IOException e) {

									e.printStackTrace();
									continue;

								}

							} else {
								// Send a Head request to check if file has been modified.
								InetAddress address = InetAddress.getByName(url.getHostName());

								Socket connection = new Socket(address, url.getPortNo());

								out = connection.getOutputStream();
								in = connection.getInputStream();

								System.out.println( url.getUrl() + "robots.txt: Downloading");
								sendGetRobots(out, url);

								RobotTuple response = robotParser.parseRobotResponse(in); 

								String code = response.m_headers.get("status-code").get(0);

								if( code.compareTo("200") == 0 ){

									String robot_content = response.getRobotText();
									RobotParser rparser = new RobotParser();

									robotTxt = rparser.parseRobotString(robot_content);

									// insert the robots file for this host.
									hostRobotsMap.put(host, robotTxt);

									//store file in database
									dbwrapper.putWebPage(roboturl, robot_content);

								}
								else{ // no robots.txt file?? I guess use defaults....?

									System.out.println("Robots.txt GET request failed with code: " + code);

								}

							}

						}

						if(robotTxt != null){

							List<String> disallowedLinks = robotTxt.getDisallowedLinks(getUserAgent());
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

							//for (String link : disallowedLinks ){
							//	System.out.println(" disallowed link:" + link);
							//}

							//System.out.println("checking if the requested url is accessible: " + url.getFilePath());

							if( disallowedLinks != null){

								String filepath = url.getFilePath();
								//System.out.println("url: " + url.getUrl() + " filepath: " + filepath);

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

									//System.out.println("This url is allowed, adding it into the QUEUE");
									frontier_Q.add(url);
								}
							} else{
								// also allowed? this useragent is not specified in the robots.txt file.
								//System.out.println("This url is allowed, adding it into the QUEUE");
								frontier_Q.add(url);
							}

						}

					}


				}


			}
		}finally{
			dbwrapper.close();
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

			// optional number of pages
			if( argv.length == 4 ){
				int numPages = Integer.valueOf(argv[3]);
				crawler.setMaxPages(numPages);
			}


			crawler.run();

			System.out.println("Done Crawling");


		}


	}

}
