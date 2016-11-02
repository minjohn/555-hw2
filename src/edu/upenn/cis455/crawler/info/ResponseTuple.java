package edu.upenn.cis455.crawler.info;

import java.util.HashMap;
import java.util.List;

public class ResponseTuple { 
	  public final HashMap<String, List<String>> m_headers; 
	  public final String m_body; 
	  public ResponseTuple(HashMap<String, List<String>> headers, String body) { 
	    this.m_body = body; 
	    this.m_headers = headers; 
	  } 
} 