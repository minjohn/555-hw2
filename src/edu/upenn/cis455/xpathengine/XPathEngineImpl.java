package edu.upenn.cis455.xpathengine;

import java.io.InputStream;

import org.w3c.dom.Document;
import org.xml.sax.helpers.DefaultHandler;

public class XPathEngineImpl implements XPathEngine {

	String [] xpaths;
	boolean [] validXPath;
	
	String cur_symbol;
	String expression;
	int pos;
	//String axis = "/";
	
	public XPathEngineImpl() {
		// Do NOT add arguments to the constructor!!
	}

	public void setXPaths(String[] s) {
		/* TODO: Store the XPath expressions that are given to this method */

		xpaths = s;

	}
	
	
	public boolean isValidNodeCharacter(String c){
		
		if( Character.isLetterOrDigit(c.charAt(0)) 
				|| c.compareTo("-") == 0 
				|| c.compareTo("_") == 0 ){
			return true;
		}
		return false;
		
	}
	
	public boolean isValidEnclosingCharacter(String c){
		
		if(  c.compareTo("]") == 0 	|| c.compareTo(")") == 0 ){
			return true;
		}
		return false;
		
	}
	
	
	// just shifts the pointer forward to skip whitespace.
	public void moduloWhitespace(){
		while ( pos < expression.length() && Character.isWhitespace(expression.substring(pos, pos+1).charAt(0)) ){
			pos++;
		}
	}
	
	public boolean checkTextEquals(){
		
		// initialized to false, to test the condition that there is a closing quote character later.
		boolean isValid = false;
		
		moduloWhitespace();
				
		if(expression.substring(pos,pos+1).compareTo("=") != 0 ){
			return false;
		} 
		
		pos++;
		
		moduloWhitespace();
		
		if(expression.substring(pos,pos+1).compareTo("\"") != 0){
			return false;
		}
		
		pos++;
		
		while(pos < expression.length()  ){
			
			String c = expression.substring(pos,pos+1);
			
			if( c.compareTo("\"") == 0  ){ // got the terminating quote, don't need to continue.
				pos++;
				isValid = true;
				break;
			}
			
			pos++;
		}
		
		
		return isValid;
		
	}
	
	public boolean checkContains(){
		
		// initialized to false, to test the condition that there is a closing quote character later.
		boolean isValid = false; 
		
		// ignore any leading whitespace prior to the "text" element
		moduloWhitespace();
		
		if( expression.substring(pos).startsWith("text()") == false  ){
			return false;
		}
		
		pos = pos + "text()".length();
		moduloWhitespace();
		
		if( expression.substring(pos, pos+1).compareTo(",") != 0 ){
			return false;
		}
		
		pos++;
		
		moduloWhitespace();
		
		if( expression.substring(pos, pos+1).compareTo("\"") != 0 ){
			return false;
		}
		
		pos++;
		
		while(pos < expression.length()  ){

			String c = expression.substring(pos,pos+1);

			if( c.compareTo("\"") == 0  ){ // got the terminating quote, don't need to continue.
				pos++;
				isValid = true;
				break;
			}

			pos++;
		}
		
		moduloWhitespace();
		
		// get the ending parentheses
		if( expression.substring(pos, pos+1).compareTo(")") != 0 ){
			return false;
		}
		
		pos++;
		
		return isValid;
		
	}
	
	public boolean checkAttribute(){
		
		// initialized to false, to test the condition that there is a closing quote character later.
		boolean isValid = false;
		boolean equals = false;
		
		String next = expression.substring(pos,pos+1);
		
		
		if( isValidNodeCharacter(next) == false ){ // must be a valid character for attribute name.
			return false;
		}
		
		pos++;
		
		while( pos < expression.length() ){

			String c = expression.substring(pos,pos+1);

			if( expression.substring(pos, pos+1).compareTo("=") == 0 ){ // got the terminating quote, don't need to continue.
				pos++;
				equals = true;
				break;
			} 
			else if( Character.isWhitespace(c.charAt(0)) ){
				// iterate and ignore whitespace before equals
				moduloWhitespace();
				break; // pos is now pointing first character that is not a space.
			}
			else if( isValidNodeCharacter(c) == false ){ //only these characters allowed in the step element name, cant have a second @ either
				return false;
			} 

			pos++;
		}
		
		// check if we did not catch the '=' symbol in the string, we must have at least an equals...
		if( pos == expression.length()){
			return false;
		}
		
		if(equals == false){
			
			if(expression.substring(pos, pos+1).compareTo("=") != 0){
				return false;
			}
			
		}
		
		// after the equals ignore any whitespace
		moduloWhitespace();
		
		if( expression.substring(pos, pos+1).compareTo("\"") != 0 ){
			return false;
		}
		
		pos++;
		
		while( pos < expression.length()  ){

			String c = expression.substring(pos,pos+1);

			if( c.compareTo("\"") == 0  ){ // got the terminating quote, don't need to continue.
				pos++;
				isValid = true;
				break;
			}

			pos++;
		}
		
		return isValid;
		
		
	}
	

	// we detected a predicate, "next" is a string with the first bracket stripped.
	public boolean checkTest(boolean insideTest){
		
		boolean isValid = true;
		
		// there is an asumption that opening bracket in this function is always true, since that is the only way we call it...
		boolean openingBracket = true;
		// ignore any leading whitespace.
		moduloWhitespace();
		
		// after modulo whitespace, first thing that must follow is a alphabetical, underscore, numerical character or @ for attributes
		String first = expression.substring(pos, pos+1);
		
		if( isValidNodeCharacter(first) == false && first.compareTo("@") != 0  ){ // there must be an invalid symbol or bracket start without element name?
			return false;
		}
		
		String element = expression.substring(pos);
		
		// shortcut? check if the following valid "tests" are included?
		if( element.startsWith("text()")   ){
			pos = pos + "text()".length();
			if(checkTextEquals() == false){
				return false;
			} else {
				return true;
			}
		}
		else if (element.startsWith("contains(") ){
			pos = pos + "contains(".length();
			if(checkContains() == false){
				return false;
			} else {
				return true;
			}
		} 
		else if (element.startsWith("@")){ // attribute
			pos++;
			if(checkAttribute() == false){
				return false;
			} else {
				return true;
			}
		}
		
		
		// after tests finish, ""'s can have  white space afterwards.
		moduloWhitespace();
		
		
		while( pos < expression.length() ) { // rest of the characters
			
			String c = expression.substring(pos,pos+1);
			
			if(c.compareTo("[") == 0){
				pos++;
				if( (isValid = checkTest(true)) == false ){ // something failed.. trickle up failure and stop processing
					break;
				}
				
				openingBracket = true;
				moduloWhitespace(); // ignore any whitespace after the test predicate body.
				continue;
			}
			else if( c.compareTo("/") == 0 ){	
				pos++;
				if( (isValid = checkStep(true)) == false ){ // something failed.. trickle up failure and stop processing
					break;
				}
				continue;
			}
//			else if( isValidEnclosingCharacter(c) ){ // there can be whitespace after a closing bracket
//				
//				
//				// this test predicate is directly on a step.
//				if(insideTest == false){
//					return true;
//				} else if ( insideTest == true ){
//					
//					//pos++;
//					//// must have had a previous opening bracket at this level, otherwise, this is an unmatched opening bracket.
//					//if( openingBracket == false ){
//					//	return false;
//					//}
//					
//					
//					//if(pos <= expression.length()){
//					//	moduloWhitespace();
//					//	openingBracket = false;
//					//	continue;
//					//}
//						
//				}	
//			}
			else if( isValidNodeCharacter(c) == false ){ //only these characters allowed in the step element name, cant have a second @ either
				return false;
			}
			
			pos++;
		}
		
		return isValid;

	}
	
	// because of the way we iterate one character at a time through the xpath expression, steps defined within the brackets of 
	//   a test will eventually read the bracket. Because a closing bracket is only relevant to the level where the opening
	//   bracket was, we will ignore closing brackets when a step is inside a test bracket and just return.
	/*
	 * Rules:  - if not in a test predicate bracket and we run across a closing bracket without an openig bracket, this is a stray
	 *         - if we are in a test predicate bracket and we run across a closing bracket, just return true, we were valid until the bracket. 
	 */	
	public boolean checkStep(boolean insideTest){
		 
		boolean isValid = true;
		boolean openingBracket = false;
		
		// after slash the first thing that must follow is a alphabetical, underscore, numerical character for nodename
		String first = expression.substring(pos, pos+1);

		if( isValidNodeCharacter(first) == false){ 
			// there must be an invalid symbol or bracket start without element name.
			return false;
		}

		
		while( pos < expression.length() ){
			
			String c = expression.substring(pos, pos+1);
			
			if( c.compareTo("[") == 0 ){ // a test predicate
				pos++;
				if( (isValid = checkTest(false)) == false ){ // something failed.. trickle up failure and stop processing
					//break;
					
				}
				
				moduloWhitespace(); // ignore any whitespace after the test predicate body.
				
				//expect closing bracket here
				
				//openingBracket = true;
				
				continue;
			}
			else if ( c.compareTo("/") == 0){ // another step
				pos++;
				if( (isValid = checkStep(false) ) == false){ // something failed.. trickle up failure and stop processing
					break;
				}
				continue;
			}
//			else if( isValidEnclosingCharacter(c) ){ // there can be whitespace after a closing bracketpos++;
//				
//				
//				// must have had a previous opening bracket at this level, otherwise, this is an unmatched opening bracket.
//				if( insideTest == false && openingBracket == false ){
//					return false;
//				}
//				else if(insideTest == true) { // flow back up to previous level
//					return true;
//				}
//				
//				// insideTest == false and openingBracket == true, its relevant at this level?
//				pos++;
//				
//				if(pos <= expression.length()){
//					moduloWhitespace();
//					
//					openingBracket = false;
//					
//					continue;
//				}
//				
//			}
			//else if( isValidNodeCharacter(c) == false  && isValidEnclosingCharacter(c) == false ){
			else if( isValidNodeCharacter(c) == false ){ //check for invalid step element name characters
				
				//isValid = false;
				//break;
				return false;
			}
			
			
			pos++; // move position pointer by 1
		}
		
		return isValid; // for things that are just characters (case 1): ... /abc
		
	}
	
	public boolean checkAxis(){
		
		if( expression.charAt(pos) == '/' ){
			
			pos++;
			//return checkStep();
			return checkStep(false);
		} else {
			return false;
		}
		
	}
	
	
	public boolean isValid(int i) {
		/* TODO: Check which of the XPath expressions are valid */
		
		pos = 0;
		expression = xpaths[i].trim();
		boolean valid = checkAxis();
		
		return valid;
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
