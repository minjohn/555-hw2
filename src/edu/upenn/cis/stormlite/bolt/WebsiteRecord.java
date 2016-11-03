package edu.upenn.cis.stormlite.bolt;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;

public class WebsiteRecord {
	
	public static HashMap<String, Date> seenUrls = new HashMap<String, Date>(); // url and last time we requested it.
	public static HashMap<String, Date> hostLastAccessed = new HashMap<String, Date>(); // url and last time we requested it.
	public static HashMap<String, RobotsTxtInfo> hostRobotsMap = new HashMap<String, RobotsTxtInfo>();
	
	// for acceptable file types
	String[] fileTypes = { "text/html", "text/xml", "application/html"};
	Set<String> acceptableFileTypes = new HashSet<String>(Arrays.asList(fileTypes));
	
	
	static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);
			//.setTimeZone(TimeZone.getTimeZone("GMT"));
	
	
	public WebsiteRecord(){
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public static SimpleDateFormat getDateFormat(){
		return dateFormat;
	}
	
	
	
}
