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
//		System.out.println(db.seeAllUsers());


		assertEquals( db.getUser("STEVEN"), "ABC123");

		db.deleteUser("TAMMY");

		assertEquals( db.getUser("TAMMY"), null );
		
		System.out.println(db.seeAllUsers());

	}
	
	
	public void testURLs() throws DatabaseException, IOException{
		
		DBWrapper db = new DBWrapper("/home/cis555/workspace/555-hw2/DATABASE/");
		
		
		
		
	}
	
	
}
