package edu.upenn.cis455.crawler;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;

public class CrawlerRequester {
	
	int maxSize;
	String[] fileTypes = { "text/html", "text/xml", "application/html"};
	Set<String> acceptableFileTypes = new HashSet<String>(Arrays.asList(fileTypes));
	
	DBWrapper dbwrapper;
	
	public void setMaxFileSize(int max){
		this.maxSize = max;
	}
	
	
	public void sendHead(OutputStream out, URLInfo info, boolean seen){

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
	public void sendGet(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET " + info.getUrl() +" HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-Agent: cis455crawler");
		writer.println("Connection: close");
		writer.println("\n");


	}

	// We assume that the socket is already connected to the host?
	public void sendGetRobots(OutputStream out, URLInfo info){

		PrintWriter writer = new PrintWriter(out,true);

		writer.println("GET /robots.txt HTTP/1.1");
		writer.println("Host: " + info.getHostName());
		writer.println("User-Agent: cis455crawler");
		writer.println("Connection: close");
		writer.println("\n");

	}


	public boolean isURLValidSSL(HttpsURLConnection con ){

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


	public boolean isURLValid(HashMap<String, List<String>> headers ){

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
	
	
	
}
