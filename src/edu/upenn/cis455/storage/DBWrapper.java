package edu.upenn.cis455.storage;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;



public class DBWrapper {
	
	
	@Entity 
	public static class Channel {
		
		@PrimaryKey
		String channelName;
		private String xpathExpression;
		
		Channel( String name, String expr){
			channelName = name;
			xpathExpression = expr;
		}
		
		private Channel(){}
		
		public String getXPath(){
			return xpathExpression;
		}
		
		public String getName(){
			return channelName;
		}
		
	}
	
	/* The data accessor class for the entity model. */
	static class ChannelAccessor{
		
		// webpage accessor
		PrimaryIndex<String, Channel> channelByName;
		
		public ChannelAccessor(EntityStore store) throws DatabaseException {
			channelByName = store.getPrimaryIndex(String.class, Channel.class);
		}
		
	}
	
	
	@Entity
	public static class WebPage {
		
		@PrimaryKey
		String uri;
		private String content;
		private Date lastAccessed;
		
		
		WebPage(String identifier, String body){
			
			uri = identifier;
			content = body;
			
			lastAccessed = Calendar.getInstance().getTime();
			
		}
		
		// deserialization and serialization needs a empty constructor. During
		//   deserialization a empty constructor is called and the data is filled in 
		//   afterwards.
		private WebPage(){}
		
		public Date getLastAccessed(){
			return lastAccessed;
		}
		
		public String getContent(){
			return content;
		}
		
		public void updateLastAccessedToNow(){
			lastAccessed = Calendar.getInstance().getTime();
		}
		
	}
	
	/* The data accessor class for the entity model. */
	static class WebPageAccessor{
		
		// webpage accessor
		PrimaryIndex<String, WebPage> pageByURI;
		
		public WebPageAccessor(EntityStore store) throws DatabaseException {
			pageByURI = store.getPrimaryIndex(String.class, WebPage.class);
		}
		
	}
	

	
	// Represents a User Entity that I store in my database currently
	@Entity
	public static class User {
		
		@PrimaryKey
		String username;
		private String password;
		
		
		// Constructor
		User(String uname, String pass){
			username = uname;
			password = pass;
		}
		
		// deserialization and serialization needs a empty constructor. During
		//   deserialization a empty constructor is called and the data is filled in 
		//   afterwards.
		private User() {}


		public String getPassword() {
			return password;
		}


	}
	
	/* The data accessor class for the entity model. */
	static class UserAccessor{
		
		// user accessor
		PrimaryIndex<String, Channel> channelByName;
		PrimaryIndex<String, User> userByUsername;
		
		public UserAccessor(EntityStore store) throws DatabaseException {
			userByUsername = store.getPrimaryIndex(String.class, User.class);
			channelByName = store.getPrimaryIndex(String.class, Channel.class);
		}
		
	}
	
	
	// constructor and Berkeley related objects.
	private static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	private static EntityStore webpage_store;
	//private static EntityStore robot_store;
	private static EntityStore channel_store;
	private UserAccessor user_access_object;
	private WebPageAccessor webpage_access_object;
	private ChannelAccessor channel_access_object;
	//private RobotsInfoAccessor robot_access_object;
	
	public DBWrapper( String envDir ) throws DatabaseException, IOException{
		
		//System.out.println(envDir);
		
		File home = new File(envDir);
		
		synchronized (this){
			if(!home.exists()){
				home.mkdirs();
			}
		}
		home.createNewFile();
		
		envDirectory = envDir;
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		myEnv = new Environment(home, envConfig);
		
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		store= new EntityStore(myEnv, "UserStore", storeConfig);
		webpage_store= new EntityStore(myEnv, "WebPageStore", storeConfig);
		channel_store= new EntityStore(myEnv, "ChannelStore", storeConfig);
		//robot_store= new EntityStore(myEnv, "RobotStore", storeConfig);
		
		user_access_object = new UserAccessor(store);
		webpage_access_object = new WebPageAccessor(webpage_store);
		channel_access_object = new ChannelAccessor(channel_store);
		//robot_access_object = new RobotsInfoAccessor(robot_store);
	}
	
//	// combine new robots with existing robots object
//	public void putRobotsTextInfo(String host, RobotsTxtInfo robotInfo ){
//		RobotsInfo db_robot = new RobotsInfo(host, robotInfo);
//		robot_access_object.robotIndex.put(db_robot);
//		
//	}
//	
//	public RobotsTxtInfo getRobotsTextInfo(String host){
//		RobotsInfo db_robot = robot_access_object.robotIndex.get(host);
//		if(db_robot !=  null){
//			
//			RobotsTxtInfo robots = db_robot.getRobotsTxtInfo();
//			
//			return robots;
//		}
//		return null;
//	}
	
	public void putChannel( String name, String xpath ) throws DatabaseException {
		Channel ch = new Channel(name, xpath);
		channel_access_object.channelByName.put(ch);
	}
	
	public String getChannelXpath(String name){
		Channel ch = channel_access_object.channelByName.get(name);
		if(ch != null){
			return ch.xpathExpression;
		}
		return null;
	}
	
	public void deleteChannel(String name){
		channel_access_object.channelByName.delete(name);
	}
	
	
	public void putWebPage( String uri, String content) throws DatabaseException {
		
		WebPage page = new WebPage(uri,content);
		webpage_access_object.pageByURI.put(page);
		
	}
	
	
	public String getWebPage(String uri){
		WebPage w = webpage_access_object.pageByURI.get(uri);
		if(w !=  null){
			return w.content;
		}
		return null;
	}
	
	public Date getWebPageLastAccessed(String uri){
		WebPage w = webpage_access_object.pageByURI.get(uri);
		if(w !=  null){
			return w.getLastAccessed();
		}
		return null;
	}
	
	public boolean containsWebPage( String content ){
		
		EntityCursor<WebPage> pages = webpage_access_object.pageByURI.entities();
		try{
			for(WebPage w : pages){
				if( w.content.compareTo(content) == 0 ){
					return true;
				}
			}
		}finally {
			pages.close();
		}
		
		return false;
		
	}
	
	public String seeAllWebPageURIs(){
		StringBuilder listOfURIs = new StringBuilder();
		
		EntityCursor<WebPage> pages = webpage_access_object.pageByURI.entities();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);
		
		try{
			for(WebPage w : pages){
				String dateString = dateFormat.format(w.getLastAccessed());
				
				listOfURIs.append("Uri: ").append(w.uri).append(" ").append("content size: ")
				.append(String.valueOf(w.content.length())).append(" Last Accessed: ").append(dateString).append("\n");
			}
		} finally {
			pages.close();
		}
		
		return listOfURIs.toString();
	}
	
	public void deleteWebPage(String uri) throws DatabaseException{
		webpage_access_object.pageByURI.delete(uri);
	}
	
	public void deleteAllWebPages(){
		
		boolean success = false;
		Transaction txn = null;
		
		try{
			
			txn = myEnv.beginTransaction(null, null);
			
			EntityCursor<WebPage> cursor = webpage_access_object.pageByURI.entities(txn, null);
			
			try{
				
				for ( WebPage web : cursor){
					cursor.delete();
				}
				
			}finally{
				cursor.close();
			}
			txn.commit();
			success = true;
			return;
			
		}finally {
			if (!success) {
				if (txn != null) {
					txn.abort();
				}
			}
		}
		
	}
	
	
	
	// creates a new user, but also generates a unique id for it.
	public void putUser( String username, String password ) throws DatabaseException {

		// initialize User entity
		User u = new User(username, password);
		
		// TODO probably should check for its existence before inserting, but the random UUID
		//   is hacky way to fix this.
		user_access_object.userByUsername.put(u);
		
		//assert user_access_object.userByUsername.get(username) != null;
		
	}
	
	// get find user and return their password. If the user doesn't exist, return null
	public String getUser(String username){
		User u = user_access_object.userByUsername.get(username);
		if( u != null){
			return u.password; 
		}
		return null;
	}
	
	// method to see all users in persistent database.
	public String seeAllUsers(){
		
		StringBuilder listOfUsers = new StringBuilder();
		
		EntityCursor<User> users = user_access_object.userByUsername.entities();
		
		try{
			for(User u : users){
				listOfUsers.append("User: ").append(u.username).append(" ")
				           .append("Password: ").append(u.getPassword()).append("\n");
			}
		} finally{
			users.close();
		}
		
		return listOfUsers.toString();
		
	}
	
	// delete the user
	public void deleteUser(String username){
		user_access_object.userByUsername.delete(username);
		
		//assert user_access_object.userByUsername.get(username) == null;
	}
	
	public void deleteAllUsers(){
		
		boolean success = false;
		Transaction txn = null;
		
		try{
			
			txn = myEnv.beginTransaction(null, null);
			
			EntityCursor<User> cursor = user_access_object.userByUsername.entities(txn, null);
			
			try{
				
				for ( User web : cursor){
					cursor.delete();
				}
				
			}finally{
				cursor.close();
			}
			txn.commit();
			success = true;
			return;
			
		}finally {
			if (!success) {
				if (txn != null) {
					txn.abort();
				}
			}
		}
		
	}
	
	
	// close the environment and store.
	public void close() throws DatabaseException {
		store.close();
		webpage_store.close();
		myEnv.close();
		
		
	}
	
}
