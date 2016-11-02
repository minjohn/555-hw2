package edu.upenn.cis.stormlite.bolt;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DBWrapperFactory;

public class XPathMatcherBolt implements IRichBolt{
	
	DBWrapper db;
	OutputCollector collector;
	Fields schema = new Fields("documentUri", "documentBody");
	
	String executorId = UUID.randomUUID().toString();
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		db.close();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		String documentUri = input.getStringByField("documentUri");
		String documentBody = input.getStringByField("documentBody");
		
		Values vals = new Values(documentUri, documentBody);
		
		collector.emit(vals);
		
		
		// Xpath matcher stuff.
		
		// check all the valid xpaths on the document, for each xpath, that matches, 
		//   insert into the db with a channel name and document uri.
		//   probably need a channel secondary index for get channel by xpath. ( what about channels with duplicate
		//    xpaths? we can assume they are global??) 
		
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		String dbDir = stormConf.get("dbDir");
		
		try {
			
			DBWrapperFactory factory = new DBWrapperFactory();
			factory.setDBDir(dbDir);
			// set up db connection.
			db = factory.getDBWrapper() ;
			
		} catch (DatabaseException e) {
			
			e.printStackTrace();
			System.exit(1);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			System.exit(1);
			
		}
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

}
