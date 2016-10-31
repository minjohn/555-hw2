package test.edu.upenn.cis455;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.upenn.cis455.xpathengine.XPathEngine;
import edu.upenn.cis455.xpathengine.XPathEngineFactory;

public class XPathEngineTest {

	@Test
	public void test() {
		
		XPathEngine xEngine = XPathEngineFactory.getXPathEngine();
		
		String[] expressions =  { "/foo/bar/cat" };
		
		xEngine.setXPaths(expressions);
		
		assertEquals(xEngine.isValid(0), true);
		
		String[] expressions2 =  { "/foo/b@r/cat" };
		
		xEngine.setXPaths(expressions2);
		
		assertEquals(xEngine.isValid(0), false);
		
		String[] expressions3 =  { "/foo/bar[@att=\"123\"]" };
		
		xEngine.setXPaths(expressions3);
		
		assertEquals(xEngine.isValid(0), true);
		
		String[] expressions4 =  { "/xyz/abc[contains(text(),\"someSubstring\")]" };
		
		xEngine.setXPaths(expressions4);
		
		assertEquals(xEngine.isValid(0), true);		
				
		
		String[] expressions5 =  { "/a/b/c[text()=\"theEntireText\"]" };
		
		xEngine.setXPaths(expressions5);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions6 =  { "/blah[anotherElement]" };
		
		xEngine.setXPaths(expressions6);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions7 =  { "/this/that[something/else]" };
		
		xEngine.setXPaths(expressions7);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions8 =  { "/d/e/f[foo[text()=\"something\"]][bar]" };
		
		xEngine.setXPaths(expressions8);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions9 =  { "/a/b/c[text() = \"whiteSpacesShouldNotMatter\"]" };
		
		xEngine.setXPaths(expressions9);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions10 =  { "/foo/bar[  @att=\"123\"]" };
		
		xEngine.setXPaths(expressions10);
		
		assertEquals(xEngine.isValid(0), true);
		
		
		// invalid case, bracket without nodename.
		String[] expressions11 =  { "/foo/[@att=\"123\"]" };
		
		xEngine.setXPaths(expressions11);
		
		assertEquals(xEngine.isValid(0), false);
		
		
		// invalid case, missing equals sign
		String[] expressions12 =  { "/foo/bar[@att  \"123\"]" };

		xEngine.setXPaths(expressions12);

		assertEquals(xEngine.isValid(0), false);
		
		
		// invalid case, missing equals sign
		String[] expressions13 =  { "/foo/bar[@att\"123\"]" };

		xEngine.setXPaths(expressions13);

		assertEquals(xEngine.isValid(0), false);
		
		// invalid case, missing equals sign
		String[] expressions14 =  { "/d/e/f[foo[text()     \"something\"]][bar]" };

		xEngine.setXPaths(expressions14);

		assertEquals(xEngine.isValid(0), false);
		
		
		// valid case, spaces in all places for contains
		String[] expressions15 =  { "/xyz/abc[  contains(  text()   ,   \"someSubstring\")]" };

		xEngine.setXPaths(expressions15);

		assertEquals(xEngine.isValid(0), true);
		
		// valid case, spaces in all places for contains
		String[] expressions16 =  { "/xyz/abc[  contains(  text()   ,   \"someSubstring\" )  ])" };

		xEngine.setXPaths(expressions16);

		assertEquals(xEngine.isValid(0), true);
		
		
		// invalid case, missing comma for contains
		String[] expressions17 =  { "/xyz/abc[  contains(  text()      \"someSubstring\" )  ])" };

		xEngine.setXPaths(expressions17);

		assertEquals(xEngine.isValid(0), false);
		
		// valid case, spaces in all places for contains
		String[] expressions18 =  { "/xyz/abc[  contains(  text()   ,   \"someSubstring\" )  ]  )" };

		xEngine.setXPaths(expressions18);

		assertEquals(xEngine.isValid(0), true);
		
		
		// valid case, spaces in all places for contains
		String[] expressions19 =  { "/xyz/abc[  contains(  text() ,     \"someSubstring\" )  ]  )   " };

		xEngine.setXPaths(expressions19);

		assertEquals(xEngine.isValid(0), true);
		
		
		// invalid case, duplicate equals sign attribute
		String[] expressions20 =  {  "/foo/bar[@att==\"123\"]" };

		xEngine.setXPaths(expressions20);

		assertEquals(xEngine.isValid(0), false);
		
		// invalid case, duplicate equals sign contains
		String[] expressions21 =  { "/xyz/abc[  contains(  text()  ,, \"someSubstring\" )  ])" };

		xEngine.setXPaths(expressions21);

		assertEquals(xEngine.isValid(0), false);

		// invalid case, duplicate equals sign text()
		String[] expressions22 =  { "/d/e/f[foo[text()==\"something\"]][bar]" };

		xEngine.setXPaths(expressions22);

		assertEquals(xEngine.isValid(0), false);
		
		
		// invalid case, space before test, hence invalid node name
		String[] expressions23 =  { "/d/e/f [foo[text()==\"something\"]][bar]" };

		xEngine.setXPaths(expressions23);

		assertEquals(xEngine.isValid(0), false);
		
		
		// invalid case, space after slash hence invalid node name
		String[] expressions24 =  { "/d/e/ f [foo[text()==\"something\"]][bar]" };

		xEngine.setXPaths(expressions24);

		assertEquals(xEngine.isValid(0), false);
		

		// invalid case, invalid characters
		String[] expressions25 =  { "/d/e/$[foo[text()=\"something\"]][bar]" };

		xEngine.setXPaths(expressions25);

		assertEquals(xEngine.isValid(0), false);


		// invalid case, invalid characters in node name
		String[] expressions26 =  { "/d/e/f[f%o[text()=\"something\"]][bar]" };

		xEngine.setXPaths(expressions26);

		assertEquals(xEngine.isValid(0), false);
		
		// invalid case, invalid characters in node name
		String[] expressions27 =  { "/d/e/f[foo[text()=\"something\"] ^ ][bar]" };

		xEngine.setXPaths(expressions27);

		assertEquals(xEngine.isValid(0), false);

		
		// invalid case, invalid characters in node name
		String[] expressions28 =  { "/d/e/f[foo[text()=\"something\"]][b@r]" };

		xEngine.setXPaths(expressions28);

		assertEquals(xEngine.isValid(0), false);
		
		// valid case, invalid characters in string literal
		String[] expressions29 =  { "/d/e/f[foo[text()=\"$omet4i^g\"]][bar]" };

		xEngine.setXPaths(expressions29);

		assertEquals(xEngine.isValid(0), true);
		
		
		String[] expressions30 =  { "/d/e/ffoo]" };

		xEngine.setXPaths(expressions30);

		assertEquals(xEngine.isValid(0), false);
		
		
		
	}

}
