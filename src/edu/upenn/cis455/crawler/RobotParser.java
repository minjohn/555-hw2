package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;

public class RobotParser {

	class RobotTuple { 
		  public final HashMap<String, List<String>> m_headers; 
		  public final RobotsTxtInfo m_robot; 
		  public RobotTuple(HashMap<String, List<String>> headers, RobotsTxtInfo robot) { 
		    this.m_robot = robot; 
		    this.m_headers = headers; 
		  } 
		  
		  public RobotsTxtInfo getRobot(){
			  return m_robot;
		  }
		  
	} 
	
	public RobotParser(){
		
	}

	public RobotTuple parseRobotResponse(InputStream in) throws IOException {

		HashMap<String, List<String>>m_headers = new HashMap<String,List<String>>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String filename = null;
		String httpVersion = null;

		System.out.println("attempting to readline");

		String responseLine = reader.readLine();

		System.out.println("requestline Text: " + responseLine);

		String[] responseParts = responseLine.split("\\s+");

		if(responseParts.length != 3){
			System.out.println(" Response does not have all of the parts!");
			return null;
		}

		// add the status code to the "headers"
		String status_code = responseParts[1];

		List<String> status = new ArrayList<String>();
		status.add(status_code);
		m_headers.put("status-code", status);

		if( status_code.compareTo("200") == 0 ){
			String nextLine;
			nextLine = reader.readLine();


			boolean run = true;
			boolean malformedHeader = false;
			boolean multiLine = false;
			String lastHeader = null; // for multiLine headers

			while( nextLine != null && nextLine.compareTo("\n") != 0 && nextLine.compareTo("\r\n") != 0  ){

				// to skip the last new line character in the HTTPRequest ( Last line of HttpRequests
				//   is a newline character, but BufferedReader will return the contents of the line without
				//   the line-terminating characters (\n ,\r\n). Hence it will return an empty string?
				if(!nextLine.isEmpty()){ 


					//find the whitespace between header and value, if there is none, then its a malformed request

					int colon = nextLine.indexOf(':');	
					if( colon == -1 &&  multiLine == false ){

						//ignore this header, malformed header
					}
					else if(colon == -1 &&  multiLine == true ){ // possible multiple line header 

						// add to the value array of header key
						//System.out.println(threadMessage("last header:" + lastHeader));

						String v = nextLine.trim();

						//System.out.println(threadMessage("v: " + v));

						if(v.endsWith(",") == true){

							v = v.substring(0, v.length()-1);
							//System.out.println(threadMessage("without comma v: " + v));
							multiLine = true;
						}else{
							multiLine = false;
						}

						//						headers.updateHeader(lastHeader, v ); 

						if(m_headers.containsKey(lastHeader) == true){
							m_headers.get(lastHeader).add(v);
						}else{
							List<String> list = new ArrayList<String>();
							list.add(v);
							m_headers.put(lastHeader, list);	
						}
					} 
					else{ // this is a Single Line Header

						String header, value;


						header = nextLine.substring(0, colon).toLowerCase().trim(); // exclude the colon
						value = nextLine.substring(colon+1, nextLine.length()).trim(); 

						List<String> val = new ArrayList<String>();

						if(value.endsWith(",")){ // this is a possible multiline multivalue header, handle the headless values above
							multiLine = true;
						}

						if(header.compareTo("user-agent") != 0){ // one line comma separated multivalue header or just regular header

							// cookies usually have name=value pair and a session token
							if(header.compareTo("cookie") == 0 ){

								System.out.println("got a cookie header ");

								String [] cookieattrs = value.split(";");
								String sessionId = null;
								String name = null;
								String cookieval = null;

								for(String attr  : Arrays.asList(cookieattrs) ){

									String[] nameval =  attr.trim().split("=");
									// special cookie is the session id.
									if( nameval[0].trim().toLowerCase().compareTo("session-id") == 0 ){

										sessionId = nameval[1].trim();
										System.out.println("session id:" + sessionId);

									}else{

										name = nameval[0];
										cookieval = nameval[1];
										System.out.println("COOKIE name:" + name + " value:"+cookieval);

										//Cookie cookie = new Cookie(name, cookieval);

										System.out.println("Adding COOKIE HEADER: name:"+header+" value:"+value);

										//headers.addCookie(cookie);

										if(m_headers.containsKey(name) == true){
											m_headers.get(name).add(cookieval);
										}else{
											List<String> list = new ArrayList<String>();
											list.add(cookieval);
											m_headers.put(name, list);	
										}

									}

								}

								// put into list object for saving text into headers
								val.add(value);

							}
							else{
								// single value header
								for( String v : Arrays.asList(value.split(","))){
									val.add(v.trim());
								}
							}

						}
						else{ // parse user-agent values specially because they can have comments in brackets (...)

							int pos = 0;

							//System.out.println("parsing user-agent");

							while( value != null ){

								//System.out.println("value: " + value);

								// its a comment, add it to the previous user-agent info
								if(value.startsWith("(")){

									int endparen = value.indexOf(')');
									if(endparen > 0){
										System.out.println("pos: " + pos);
										int prev = pos-1;
										System.out.println("prev: " + prev);
										if( prev >= 0){ //there is a valid user agent to apply comment to
											val.set(prev, val.get(prev) + " " + value.substring(0, endparen+1) );
										}
									}

									value = ( value.substring(endparen+1).trim() );
									pos = pos-1; // since we move the potential element to the previous element 

								}
								else {
									int nextSpace = value.indexOf(' ');
									if(nextSpace < 0 ){ // last agent thing
										val.add(value.trim());
										value = null;
									}
									else{
										val.add(value.substring(0, nextSpace));
										value = value.substring(nextSpace).trim();
									}

								}
								pos ++;
							}

						}


						//headers.addHeader(header, val);

						if(m_headers.containsKey(header) == true){
							m_headers.get(header).add(val.get(0));
						}else{
							m_headers.put(header,val);	
						}

						//System.out.println(threadMessage("Header: " + header + 
						//		"  |  Value: " + val.toString()));

						lastHeader = header;
					}

					nextLine = reader.readLine();
					//					System.out.println("Next line: " + nextLine);
				}
				else{
					nextLine = null; // trigger the termination condition
				}
			}


			String userAgent = "";
			RobotsTxtInfo robots = new RobotsTxtInfo();

			//			System.out.println("Body???");

			while ( (nextLine = reader.readLine()) != null ){

//				System.out.println(nextLine);

				String[] parts = nextLine.split(":");

				if (parts.length < 2){ // no value!, ignore this

				}else{

					String header = parts[0].toLowerCase().trim();
					String value = parts[1].trim();

					if( header.compareTo("user-agent") == 0  ){ // the current user-agent. 
						userAgent = value.trim();
//						System.out.println("Got User agent");
						robots.addUserAgent(userAgent);
					} else if ( header.compareTo("disallow")  == 0 ){

						if( userAgent.compareTo("") == 0 ){
							// no userAgent to associate this header to.
							
						}else{
//							System.out.println("Adding Disallow " + value);
							robots.addDisallowedLink(userAgent, value);
						}

					} else if ( header.compareTo("allow") == 0 ){

						if( userAgent.compareTo("") == 0 ){
							// no userAgent to associate this header to.

						}else{
//							System.out.println("Adding Allow " + value);
							robots.addAllowedLink(userAgent, value);
						}

					} else if ( header.compareTo("crawl-delay")  == 0){

						if( userAgent.compareTo("") == 0 ){
							// no userAgent to associate this header to.

						}else{
//							System.out.println("Adding crawl delay " + value);
							robots.addCrawlDelay(userAgent, Integer.valueOf(value));
						}
					}
				}
			}
			
//			System.out.println("printing...");
//			robots.print();
//			System.out.println("delay: " + String.valueOf(robots.getCrawlDelay("crawltest.cis.upenn.edu")));
//			System.out.println("Disallowed: ");
//			assert robots != null;

			RobotTuple tuple = new RobotTuple(m_headers, robots);
			
			
			return tuple;


		}else{
			// error status code is only added into the tuple and returned.
			RobotTuple tuple = new RobotTuple(m_headers, null);
			return tuple;
		}

	}

}
