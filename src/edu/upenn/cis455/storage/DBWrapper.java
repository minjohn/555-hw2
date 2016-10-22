package edu.upenn.cis455.storage;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

public class DBWrapper {
	
	
	// Represents a User Entity that I store in my database currently
	@Entity
	public static class User {
		
//		@PrimaryKey
//		String uid;
		
		@PrimaryKey
		String username;
		private String password;
		
		
		// Constructor
		User(String uname, String pass){
//			uid = id;
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

		PrimaryIndex<String, User> userByUsername;
		
		public UserAccessor(EntityStore store) throws DatabaseException {
			userByUsername = store.getPrimaryIndex(String.class, User.class);
		}
		
	}
	
	
	// constructor and Berkeley related objects.
	private static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	private UserAccessor user_access_object;
	
	public DBWrapper( String envDir ) throws DatabaseException, IOException{
		
		System.out.println(envDir);
		
		File home = new File(envDir);
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
		
		user_access_object = new UserAccessor(store);
		
	}
	
	// creates a new user, but also generates a unique id for it.
	public void putUser( String username, String password ) throws DatabaseException {
		
		// generate a random uid for it
//		UUID id = UUID.randomUUID();
		
		// initialize User entity
//		User u = new User(id.toString(), username, password);
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
	
	
	// close the environment and store.
	public void close() throws DatabaseException {
		store.close();
		myEnv.close();
	}
	
}
