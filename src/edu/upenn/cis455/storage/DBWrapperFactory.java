package edu.upenn.cis455.storage;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;

public class DBWrapperFactory {

	static DBWrapper db;
	static String dbDir = "";

	public void setDBDir(String dir){
		this.dbDir = dir;
	}

	public DBWrapper getDBWrapper() throws DatabaseException, IOException{

		if(dbDir.compareTo("") == 0){
			return null;
		}

		if(db == null){

			// set up db connection.
			db = new DBWrapper(dbDir);
			return db;

		} else{
			return db;
		}

	}
}
