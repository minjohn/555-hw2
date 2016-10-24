package test.edu.upenn.cis455;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis455.storage.DBWrapper;
import junit.framework.TestCase;

public class DBWrapperTest extends TestCase {
	

	public void testUsers() throws DatabaseException, IOException{
		DBWrapper db = new DBWrapper("/home/cis555/workspace/555-hw2/DATABASE/");

		db.putUser("STEVEN", "ABC123");
		db.putUser("TOM", "PASSWORD123");
		db.putUser("TAMMY", "HELLOW");
		//System.out.println(db.seeAllUsers());


		assertEquals( db.getUser("STEVEN"), "ABC123");

		db.deleteUser("TAMMY");

		assertEquals( db.getUser("TAMMY"), null );
		
		System.out.println(db.seeAllUsers());
		
		//db.deleteAllUsers();

	}
	
	
	public void testURLs() throws DatabaseException, IOException{
		
		DBWrapper db = new DBWrapper("/home/cis555/workspace/555-hw2/DATABASE/");
		db.deleteAllWebPages();
		System.out.println(" current pages " + db.seeAllWebPageURIs());
		
		db.putWebPage("testpage1", "content1");
		db.putWebPage("testpage2", "content2");
		db.putWebPage("testpage3", "content3\n");
		db.putWebPage("testpage4", "content4");
		
		//System.out.println(" current pages " + db.seeAllWebPageURIs());
		
		boolean contains = db.containsWebPage("content1");
		System.out.println("contains: " + String.valueOf(contains));
		
		assertEquals( contains, true );
		
		db.deleteWebPage("testpage2");
		
		assertEquals( db.containsWebPage("content2"), false );
		
		assertEquals( db.getWebPage("testpage3"), "content3\n");
		
		
		db.deleteAllWebPages();
		
		
	}
	
	
}
