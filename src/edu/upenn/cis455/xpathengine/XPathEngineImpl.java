package edu.upenn.cis455.xpathengine;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Element;
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
	int temp_pos; // used for xpath matching

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

	public int moduloWhitespace(int curr_position){

		while ( curr_position < expression.length() && Character.isWhitespace(expression.substring(curr_position, curr_position+1).charAt(0)) ){
			curr_position++;
		}
		return curr_position;
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


	public int testTextEquals(Element childElement, String expression, int curr_position  ){

		String first = expression.substring(curr_position);
		int first_quote = first.indexOf("\"");

		String second = expression.substring(first_quote);
		int second_quote = second.indexOf("\"");

		String value = expression.substring(first_quote+1, second_quote);

		curr_position = second_quote;

		NodeList children = childElement.getChildNodes();

		for(int i = 0; i < children.getLength(); i++){

			Node child = children.item(i);

			if( child.getNodeType() == Node.TEXT_NODE ){
				if( child.getNodeValue().equals(value)  ){

					// get the next position to check the xpath with

					int closingbracket_index = expression.substring(curr_position).indexOf("]");

					return closingbracket_index;
				}
			}
		}

		return -1;


	}

	public int testContains(Element childElement, String expression, int curr_position ){

		String first = expression.substring(curr_position);
		int first_quote = first.indexOf("\"");

		String second = expression.substring(first_quote);
		int second_quote = second.indexOf("\"");

		String value = expression.substring(first_quote+1, second_quote);

		curr_position = second_quote;

		NodeList children = childElement.getChildNodes();

		for(int i = 0; i < children.getLength(); i++){

			Node child = children.item(i);

			if( child.getNodeType() == Node.TEXT_NODE ){
				if( child.getNodeValue().equals(value)  ){
					int closingbracket_index = expression.substring(curr_position).indexOf("]");

					return closingbracket_index;
				}
			}
		}

		return -1;

	}

	public int testAttribute(Element childElement, String expression, int curr_position ){
		
		String first = expression.substring(curr_position);
		int first_quote = first.indexOf("\"");

		String second = expression.substring(first_quote);
		int second_quote = second.indexOf("\"");
		
		
		int closingbracket_index = expression.substring(second_quote).indexOf("]");

		String attribute_test = expression.substring(curr_position, closingbracket_index);

		String pattern ="@(.*?)\\s*=\\s*'(.*?)'";

		Pattern attribute_pattern = Pattern.compile(pattern);
		Matcher matcher = attribute_pattern.matcher( attribute_test );

		matcher.find();

		String attribute_name = matcher.group(1);
		String attribute_value = matcher.group(2);

		NodeList children = childElement.getChildNodes();

		for(int i = 0; i < children.getLength(); i++){

			Node child = children.item(i);

			if( child.getNodeType() == Node.ATTRIBUTE_NODE ){

				if( child.getNodeName().equals( attribute_name ) && child.getNodeValue().equals(attribute_value)  ){

					return closingbracket_index;
				}
			}
		}

		return -1;

	}

	public int testExistence(Element childElement, String expression, int curr_position){
		
		
		boolean children_exist = false;
		boolean test = false;
		boolean end_test = false;
		// just a list of children 
		
		// child(ren) with tests 
		int curr_pos = curr_position;

		String step;

		StringBuffer buffer = new StringBuffer();

		while(curr_pos < expression.length()){

			char c = expression.charAt(curr_pos);
			curr_pos++;

			if( c == '/'){
				break;
			}		
			else if(c == '[' ){
				test = true;
				break;
			}
			else if(c == ']' ){
				end_test = true;
				break;
			}
			else{

				buffer.append(c);
			}
		}
		
		step = buffer.toString();
		//curr_pos = curr_pos + step.length();
		
		NodeList children = childElement.getChildNodes();

		for(int i = 0; i< children.getLength(); i++){

			Node child = children.item(i);
			if( child.getNodeName().compareTo(step) == 0 ){

				if( curr_pos == expression.length() ){ // terminating condition, we are at the end of the xpath and there was a node match
					return curr_pos;
				}
				
				if( child.getNodeType() == Node.ELEMENT_NODE ){
					
					if(end_test == true){
						return curr_pos;
					}
					
					if(test == true){ // there is a test associated with this node
					// check if it passes test
					// if passes test, continue; otherwise do not continue with this node.
	
						String test_type = expression.substring(curr_pos);  
						int next_pos = 0;
						if(test_type.startsWith("text")) {
	
							next_pos = testTextEquals( childElement, expression, curr_pos );
	
						} else if(test_type.startsWith("contains")) {
	
							next_pos = testContains( childElement, expression, curr_pos );
	
						} else if(test_type.startsWith("@")) {
	
							next_pos = testAttribute( childElement, expression, curr_pos );
	
						} else { // step existence
							next_pos = testExistence( childElement, expression, curr_pos);
							
							if( next_pos != -1 ){ // increment the position to any closing brackets at a higher level
								next_pos = moduloWhitespace(next_pos);	
							}	
						}
	
						// if test passes, then continue going down xpath
						if(next_pos != -1){
							return next_pos;
						}
	
					} else{ // there isn't test continue to check xpath
						int next_pos = 0;
						next_pos = testExistence( childElement, expression, curr_pos);
						
						if( next_pos != -1 ){ // increment the position to any closing brackets at a higher level
							int temp = moduloWhitespace(next_pos);
							
							if(temp > next_pos){ // we incremented the pointer position because we ignored a space(s)
								next_pos = temp;
							}
						}
						
						if( next_pos != -1 ){
							return next_pos;
						}
	
					}
				}
				
			}
		}
		
		
		return -1;
		
	}

	public boolean matchXPath(Element root, String xpath, int curr_position){

		// Get children and attempt to match the first step of the xpath.
		NodeList children = root.getChildNodes();
		boolean test = false;
		boolean match = false;

		
		int curr_pos = curr_position;

		String step;

		StringBuffer buffer = new StringBuffer();

		while(curr_pos < xpath.length()){

			char c = xpath.charAt(curr_pos);
			curr_pos++;

			if( c == '/'){
				break;
			}		
			else if(c == '[' ){
				test = true;
				break;
			}
			else{
				buffer.append(c);
			}
		}

		step = buffer.toString();
		//curr_pos = curr_pos + step.length();

		// go through the children nodes and see if any of these match the first step

		for(int i = 0; i< children.getLength(); i++){

			Node child = children.item(i);

			// node name matched
			if( child.getNodeName().compareTo(step) == 0 ){

				if( curr_pos == xpath.length() ){ // terminating condition, we are at the end of the xpath and there was a node match
					return true;
				}

				// otherwise continue checking xpaths
				if( child.getNodeType() == Node.ELEMENT_NODE ){

					Element childElement = (Element) child;

					if(test == true){ // there is a test associated with this node
						// check if it passes test
						// if passes test, continue; otherwise do not continue with this node.

						String test_type = xpath.substring(curr_pos);  
						int next_pos = 0;
						if(test_type.startsWith("text")) {

							next_pos = testTextEquals( childElement, xpath, curr_pos );

						} else if(test_type.startsWith("contains")) {

							next_pos = testContains( childElement, xpath, curr_pos );

						} else if(test_type.startsWith("@")) {

							next_pos = testAttribute( childElement, xpath, curr_pos );

						} else { // step existence
							next_pos = testExistence( childElement, xpath, curr_pos);
						}

						// if test passes, then continue going down xpath
						if(next_pos != -1){
							
							if( next_pos < xpath.length() ){ // we are not at the end of the xpath but test passed, so continue
								match = matchXPath( childElement, xpath, next_pos);
								if( match == true ){
									return true;
								}
							} else if (next_pos == xpath.length()){
								return true;
							}							
						}

					} else{ // there isn't test continue to check xpath

						match = matchXPath( childElement, xpath, curr_pos);
						if( match == true ){
							return true;
						}

					}

				} 

			}

		}


		return false;

	}



	public boolean[] evaluate(Document document) { 
		/* TODO: Check whether the document matches the XPath expressions */

		// array of xpaths
		boolean [] matches = new boolean[ xpaths.length ];

		Element root = document.getDocumentElement();
		
		System.out.println("root name:" + root.getNodeName());
		
		for(int i = 0; i< xpaths.length; i++){

			if(isValid(i) == true){ // only try to match valid xpaths 
				matches[i] = matchXPath(root, xpaths[i], 1);
			} else{
				matches[i] = false;
			}

		}

		return matches; 
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




















