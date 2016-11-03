package test.edu.upenn.cis455;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class RegexTest {

	@Test
	public void test() {
		
		String test = "\"this \"";
		//System.out.println(test);
		
		assertEquals(test.matches("\"[A-Za-z0-9_\\s\\S]*\"")   , true );
		
		String test2 = "contains( blah() )";
		
		assertEquals(test2.matches("contains[(].*[)]")   , true );
		
//		String test3 = " /bear/cat//dog/cow";
//		
//		String[] stuff = test3.split("[(//)(/)]");
//		
//		for( String s  : stuff ){
//			System.out.println(s);
//		}
		
//		String valid = "f";
//		
//		assertEquals(valid.matches("[^a-zA-Z0-9_-]"), false  );
//		
//		String invalid = "a";
//		
//		assertEquals(invalid.matches("[^@a-zA-Z0-9_-]"), true );
//		
//		//assertEquals(test2.matches("contains[(].*[)]")   , true );
		
		
		String attribute_test = "@id = '123331'";
		//System.out.println(attribute_test);
		
		//String pattern = "@([a-zA-]+?)\\s+=\\s+'([a-zA-z]+?)'";
		String pattern ="@(.*?)\\s*=\\s*'(.*?)'";
		
		Pattern attribute_pattern = Pattern.compile(pattern);
		Matcher matcher = attribute_pattern.matcher( attribute_test );
		
		matcher.find();
		
		String attribute_name = matcher.group(1);
		String attribute_value = matcher.group(2);
		
		System.out.println(" Attribute name: " + attribute_name + " value: " + attribute_value);
		
		
	}

}
