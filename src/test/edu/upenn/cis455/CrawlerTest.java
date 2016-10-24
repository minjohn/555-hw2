package test.edu.upenn.cis455;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis455.crawler.XPathCrawler;
import edu.upenn.cis455.crawler.XPathCrawlerFactory;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;
import junit.framework.TestCase;

public class CrawlerTest extends TestCase {
	
	public void testPrivate() throws DatabaseException, IOException, InterruptedException{
		
		DBWrapper db = new DBWrapper("/home/cis555/workspace/555-hw2/DATABASEs/Database2");
		db.deleteAllWebPages();
		
		XPathCrawlerFactory crawlerFactory = new XPathCrawlerFactory();

		XPathCrawler crawler =  crawlerFactory.getCrawler();
		   
		crawler.setStartUrl("http://crawltest.cis.upenn.edu/");
		crawler.setDbDir("/home/cis555/workspace/555-hw2/DATABASE");
		crawler.setMaxSize(100000);
		crawler.setUserAgent("cis455crawler");
		crawler.enqueueURL(new URLInfo("http://crawltest.cis.upenn.edu/"));
			
		crawler.run();
		
		// check that /marie/private is not in the database.
		assertEquals(db.containsWebPage("http://crawltest.cis.upenn.edu/marie/"), false);
		
		
	}
	
	public void testMaxSize() throws DatabaseException, IOException, InterruptedException{
		
		DBWrapper db = new DBWrapper("/home/cis555/workspace/555-hw2/DATABASE/");
		db.deleteAllWebPages();
		
		XPathCrawlerFactory crawlerFactory = new XPathCrawlerFactory();

		XPathCrawler crawler =  crawlerFactory.getCrawler();
		   
		crawler.setStartUrl("http://crawltest.cis.upenn.edu/");
		crawler.setDbDir("/home/cis555/workspace/555-hw2/DATABASE");
		crawler.setMaxSize(500);
		crawler.setUserAgent("cis455crawler");
		crawler.enqueueURL(new URLInfo("http://crawltest.cis.upenn.edu/"));
		
		crawler.run();
		
		assertEquals(db.containsWebPage("http://crawltest.cis.upenn.edu/misc/weather.xml"), false);
	}
	
	
}
