package edu.upenn.cis455.xpathengine;

import java.io.InputStream;

import org.w3c.dom.Document;
import org.xml.sax.helpers.DefaultHandler;

public class XPathEngineImpl implements XPathEngine {

	String [] xpaths;
	boolean [] validXPath;
	//String axis = "/";
	
	public XPathEngineImpl() {
		// Do NOT add arguments to the constructor!!
	}

	public void setXPaths(String[] s) {
		/* TODO: Store the XPath expressions that are given to this method */

		xpaths = s;

	}
	
	

	// we detected a predicate, "next" is a string with the first bracket stripped.
	public boolean checkTest(String next){
		
//		int bracket = 0;
//		String test = next.trim();
//		
//		if( test.startsWith("text()")  ){
//			
//			 String [] parts = test.split("=");
//			 
//			 if(parts.length == 2){ // must be exactly two, else its malformed.
//				 
//				 String value = parts[1].trim(); // get rid or ignore whitespace.
//				 
//				 if( value.matches("\".*\"")   ){ // must be a quoted string
//					 return true;
//				 }else{
//					 return false;
//				 }
//				 
//			 }else{
//				 return false;
//			 }
//			
//		} else if( test.startsWith("contains(") ){
//			
//			int index = test.indexOf("contains(");
//			String inner = test.substring(index).trim(); // get rid of any whitespace inside the contains test.
//			
//			if( inner.startsWith("text()") ){ // must start with text()
//				String [] parts = test.split(",");
//				
//				 if(parts.length != 2){ // must be exactly two, else its malformed.
//					
//					 String value = parts[1].trim(); 
//					 
//					 if( value.matches("\".*\"")   ){ // must be a quoted string
//						 
//						 
//						 
//						 return true;
//					 }else{
//						 return false;
//					 }
//					 
//					 
//				 }else{
//					 return false;
//				 }
//				
//			}else{
//				 // didn't start with text() invalid...
//				 return false;
//				 
//			}
//			
//		} else if( test.startsWith("@") ){
//			
//		} else if( ( bracket = test.indexOf("[") ) != -1   ){
//			
//			// check everything before bracket is alphabetical.
//			if( test.substring(0, bracket).matches("[a-zA-Z]+") && checkStep(test.substring(bracket)) == true ){
//				return true;
//			}else{
//				return false;
//			}
//		}	
	}
	
	
	public boolean checkStep(String next){
		
		// step is nodename { test }
//		int bracket = 0;
//		if ( ( bracket = next.indexOf("[") ) != -1  ){ // is there a predicate?
//			
//			String nodename = next.substring(0, bracket);
//			
//			if ( nodename.length() == 0 ){ // possible invalid, there was no node name.
//				return false;
//			}
//			
//			// strip the first bracket, and recursively check the contents of the predicate, the ending bracket will be 
//			//   checked recursively in the checkTest function.
//			if( checkTest( next.substring(bracket)) == true ){ 
//				return true;
//			}
//			
//		} 
//		
//		else if (next.indexOf("/") != -1 ) { // is there a next step?
//			
//		} 
//		
//		// last step in path? ; nodename should only have characters, no symbols. 
//		else if ( next.matches("[a-zA-Z]+") ) { 
//			return true;
//		} 
//		
//		else{
//			return false;
//		}
		
		
		
		
		
		
	}
	
	
	public boolean isValid(int i) {
		/* TODO: Check which of the XPath expressions are valid */
		
//		if( xpaths[i].compareTo("/") == 0 || xpaths[i].compareTo("//") == 0 ){ // invalid
//			
//			validXPath[i] = false;
//			
//		} else if( xpaths[i].charAt(0) == '/' ){
//			
//			// valid so far.
//			
//			String nextStep = xpaths[i].substring(1);
//			
//			if(checkStep(nextStep) == true){
//				validXPath[i] = true;
//			}else{
//				validXPath[i] = false;
//			}
//
//		} 
//		else{
//			// invalid 
//			validXPath[i] = false;
//		}
		
		checkStep(xpaths[i]);
		
		
		
		

		return false;
	}

	public boolean[] evaluate(Document d) { 
		/* TODO: Check whether the document matches the XPath expressions */
		return null; 
	}

	@Override
	public boolean isSAX() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

}
