package test.edu.upenn.cis455;

import static org.junit.Assert.*;

import org.junit.Test;

public class RegexTest {

	@Test
	public void test() {
		
		String test = "\"this \"";
		System.out.println(test);
		
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
		
		String valid = "f";
		
		assertEquals(valid.matches("[^a-zA-Z0-9_-]"), false  );
		
		String invalid = "a";
		
		assertEquals(invalid.matches("[^@a-zA-Z0-9_-]"), true );
		
		//assertEquals(test2.matches("contains[(].*[)]")   , true );
		
		
		
	}

}
