package edu.upenn.cis455.xpathengine;

import java.io.InputStream;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;

public class XPathEngineImpl implements XPathEngine {

	String [] xpaths;
	boolean [] validXPath;
	
	String cur_symbol;
	String expression;
	int pos;
	
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
	
	public boolean expectCloseBracket(){
		String c = expression.substring(pos, pos+1);
		if( c.compareTo("]") == 0 ){
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
		
		if( expression.substring(pos).startsWith("text") == true  ){

			pos = pos + "text".length();

			moduloWhitespace();
			// check for first parentheses
			int temp = pos;
			String paren = expression.substring(pos,pos+1);
			moduloWhitespace();

			if(temp == pos){ // there was no space, need to increment ourselves.
				pos++;
			}

			String paren2 = expression.substring(pos,pos+1);

			if( paren.compareTo("(") == 0 && paren2.compareTo(")") == 0 ){
				pos++;
				
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
		}

		
		return false;

		
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
	public boolean checkTest(){
		
		// there is an asumption that opening bracket in this function is always true, since that is the only way we call it...
		
		boolean closingBracket = false;
		// ignore any leading whitespace.
		moduloWhitespace();
		
		// after modulo whitespace, first thing that must follow is a alphabetical, underscore, numerical character or @ for attributes
		String first = expression.substring(pos, pos+1);
		
		if( isValidNodeCharacter(first) == false && first.compareTo("@") != 0  ){ // there must be an invalid symbol or bracket start without element name?
			return false;
		}
		
		String element = expression.substring(pos);


		if( element.startsWith("text") == true  ){

			pos = pos + "text".length();

			moduloWhitespace();
			// check for first parentheses
			int temp = pos;
			String paren = expression.substring(pos,pos+1);
			moduloWhitespace();

			if(temp == pos){ // there was no space, need to increment ourselves.
				pos++;
			}
			
			moduloWhitespace();
			
			String paren2 = expression.substring(pos,pos+1);

			if( paren.compareTo("(") == 0 && paren2.compareTo(")") == 0 ){
				pos++;

				moduloWhitespace();

				if(checkTextEquals() == false){
					return false;
				} else {
					// to be a complete valid test, must have closing bracket
					moduloWhitespace();

				}
			} else if(isValidNodeCharacter(paren) == false ||  isValidNodeCharacter(paren2) == false ) { // check if they are invalid characters
				return false;
			}

		}
		else if (element.startsWith("contains") == true ){
			pos = pos + "contains".length();

			moduloWhitespace();
			// check for first parentheses
			int temp = pos;
			String paren = expression.substring(pos,pos+1);
			moduloWhitespace();

			if(temp == pos){ // there was no space, need to increment ourselves.
				pos++;
			}

			if( paren.compareTo("(") == 0 ){

				moduloWhitespace();

				if(checkContains() == false){
					return false;
				} else {
					// to be a complete valid test, must have closing bracket
					moduloWhitespace();
				}

			} else if(isValidNodeCharacter(paren) == false ) { // check if they are invalid characters
				return false;
			}


		} 
		else if (element.startsWith("@") == true){ // attribute
			pos++;
			if(checkAttribute() == false){
				return false;
			} else {
				// to be a complete valid test, must have closing bracket
				moduloWhitespace();
			}
		}

		
		while( pos < expression.length() ) { // rest of the characters
			
			String c = expression.substring(pos,pos+1);
			
			if(c.compareTo("[") == 0){
				pos++;
				if( (closingBracket = checkTest()) == false ){ // something failed.. trickle up failure and stop processing
					break;
				}
				
				int temp = pos;
				moduloWhitespace(); // ignore any whitespace after the test predicate body.
				continue;

			}
			else if( c.compareTo("/") == 0 ){	
				
				// peek at next element to see if it is a valid character without incrementing position
				String next = expression.substring(pos+1,pos+2);
				if( isValidNodeCharacter(next) == false){ // something failed.. trickle up failure and stop processing
					return false;
				}
				
			}
			else if( isValidEnclosingCharacter(c) ){ // there can be whitespace after a closing bracket
				
				closingBracket = true;
				
				if(pos < expression.length()-1){ // increment if this bracket is not the last character in the expression.
					pos++;
				}
				
				break;
				
			}
			else if( isValidNodeCharacter(c) == false){ //only these characters allowed in the step element name, cant have a second @ either
				return false;
			}
			
			pos++;
		}
		
		return closingBracket;

	}
	
	// because of the way we iterate one character at a time through the xpath expression, steps defined within the brackets of 
	//   a test will eventually read the bracket. Because a closing bracket is only relevant to the level where the opening
	//   bracket was, we will ignore closing brackets when a step is inside a test bracket and just return.
	public boolean checkStep( ){
		 
		boolean isValid = true;
		boolean openingBracket = false;
		
		// after slash the first thing that must follow is a alphabetical, underscore, numerical character for nodename
		String first = expression.substring(pos, pos+1);

		if( isValidNodeCharacter(first) == false){ 
			// there must be an invalid symbol or bracket start without element name.
			return false;
		}

		
		while( pos < expression.length()){
			
			String c = expression.substring(pos, pos+1);
			
			if( c.compareTo("[") == 0 ){ // a test predicate
				
				
				openingBracket = true;
				pos++;
				
				moduloWhitespace();
				
				if( checkTest() == false ){
					return false;
				}
			
				moduloWhitespace(); // ignore any whitespace after the test predicate body.
				continue;

				
			}
			else if ( c.compareTo("/") == 0){ // another step
				
				// peek at next element to see if it is a valid character without incrementing position
				String next = expression.substring(pos+1,pos+2);
				if( isValidNodeCharacter(next) == false){ // something failed.. trickle up failure and stop processing
					return false;
				}
				
			}
			else if( isValidEnclosingCharacter(c) ){ // there can be whitespace after a closing bracketpos++;
				
			
				// must have had a previous opening bracket at this level, otherwise, this is an unmatched opening bracket.
				if( openingBracket == false ){
					return false;
				}
				
				openingBracket = false;
		
			}
			
			else if( isValidNodeCharacter(c) == false ){ //check for invalid step element name characters
				
				return false;
			}
			
			
			pos++; // move position pointer by 1
		}
		

		
		
		return isValid; // for things that are just characters (case 1): ... /abc
		
	}
	
	public boolean checkAxis(){
		
		if( expression.charAt(pos) == '/' ){
			
			pos++;
			
			return checkStep();
		} else {
			return false;
		}
		
	}
	
	
	public boolean isValid(int i) {
		/* TODO: Check which of the XPath expressions are valid */
		
		//reset the position pointer
		pos = 0;
		
		expression = xpaths[i].trim();
		boolean valid = checkAxis();
		
		return valid;
	}

	public boolean matchStepTest(String name, NodeList list){
		
		boolean text = false;
		boolean contains = false;
		boolean attribute = false;
		boolean step = false;
		int temp_pos = pos; // get a snapshot of pos, so all nodes can start at same position.
		
		// get the test type
		//  moduloWhitespace,
		//  starts with text(), contains(...), @, step
		// for each of the nodes that match 'name'
		//     get children 
		//       iterate through children and only traverse children who match the test
		// 		   - Note: for step tests, just call the matchStepNodeName() 
		
		
		return false;
	}
	
	public boolean matchStepNodeName(String name, NodeList list){
		
		boolean test = false;
		String step = "";
		int temp_pos = pos; // get a snapshot of pos, so all nodes can start at same position.
		
		for(int i = 0; i < list.getLength(); i++ ){
			
			Node node = list.item(i);
			
			if( temp_pos == expression.length() ){
				
				if(node.getNodeName().compareTo(name) == 0){
					return true;
				}
				
			} else if( node.getNodeName().compareTo(name) == 0 ){ // match traverse down its children.
				
				NodeList children  = node.getChildNodes();
				
				StringBuffer buffer = new StringBuffer();
				
				while(temp_pos < expression.length()){
					
					char c = expression.charAt(temp_pos);
					
					if( c == '/'){
						break;
					}		
					else if(c == '[' ){
						test = true;
						break;
					} else{
						temp_pos++;
						buffer.append(c);
					}
				}
				
				
				step = buffer.toString();
				// just the naive case for matching node names
				pos = temp_pos + step.length();
				
				if( test == true){
					
					if(matchStepTest( step , children) == true){
						return true;
					}
					
				}else if(matchStepNodeName( step , children) == true ){
					return true;
				}
				
			}
			
		}
		
		return false;
		
	}
	
	
	public boolean matchXPath(Document d, String xpath){
		
		// Get children and attempt to match the first step of the xpath.
		NodeList children = d.getChildNodes();
		boolean test = false;
		
		pos = 1; // skip root slash
		expression = xpath;
		
		String step;
			
		StringBuffer buffer = new StringBuffer();

		while(pos < expression.length()){

			char c = expression.charAt(pos);

			if( c == '/'){
				break;
			}		
			else if(c == '[' ){
				test = true;
				break;
			} else{
				pos++;
				buffer.append(c);
			}
		}

		step = buffer.toString();
		pos = pos + step.length();

		if( test == true){
			
			if(matchStepTest( step , children) == true){
				return true;
			}
			
		}else if(matchStepNodeName( step , children) == true ){
			return true;
		}
						
		return false;

	}
	
	
	
	public boolean[] evaluate(Document d) { 
		/* TODO: Check whether the document matches the XPath expressions */
		
		boolean [] matches = new boolean[ xpaths.length ];
		
		for(int i = 0; i< xpaths.length; i++){
			
			if(isValid(i) == true){ // only try to match valid xpaths 
				matches[i] = matchXPath(d, xpaths[i]);
			}
			
		}
		
		
		
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




















