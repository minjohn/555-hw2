package edu.upenn.cis455.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DBWrapper.User;

@SuppressWarnings("serial")
public class XPathServlet extends HttpServlet {
	
	DBWrapper db;
	
	
	
	@Override
	public void init(ServletConfig config) throws ServletException{
		
		super.init(config);
		String envHome = getServletContext().getInitParameter("BDBstore");
		try {
			db = new DBWrapper(envHome);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/* You may want to override one or both of the following methods */

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		/* TODO: Implement user interface for XPath engine here */
		
		
		// check for the user in the database
		
		String username = request.getParameter("username");
		String password = request.getParameter("password");
		String dbpass = null;
		if( ( dbpass = db.getUser(username)) != null ) { // user exists
			
			if( dbpass.compareTo(password) ==0 ){ // matches
				
				System.out.println("Password is correct");
				
//				PrintWriter out = response.getWriter();
//				
//				out.println("<html>\n");
//				out.println("<head>\n");
//				out.println("<title> UserPage </title>\n");
//				out.println("</head>\n");
//				out.println("<body> " + "Welcome " + username+ "</body>\n");
//				out.println("<a href=\"http://localhost:8080/logout\"><button> logout </button> </a> ");
//				out.println("</html>\n");
				
				// create a session for the user.
				HttpSession session;
				session = request.getSession();
				
				session.setAttribute("user", username);
				session.setAttribute("password", password);
				
				//redirect to main page.
				response.sendRedirect("/");
				
			}else{ // wrong password, redirect to the login page.
				System.out.println("Wrong Password");
				response.sendRedirect("/");
				
			}
			
		}else{ // create a new user
			
			System.out.println("Created new user");
			
			db.putUser(username, password);
			
			HttpSession session;
			session = request.getSession();
			session.setAttribute("user", username);
			session.setAttribute("password", password);
			
			// redirect to main login page.
			response.sendRedirect("/");
			
		}
		
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		/* TODO: Implement user interface for XPath engine here */
		
		HttpSession session;
		
		session = request.getSession(false);
		PrintWriter out = response.getWriter();
		
		if(session == null){ // there is no session, user is not logged in
			
			System.out.println("No session, send log in page");
			
			//File f = new File("resources/postform.html");
			File f = new File("resources/index.html");
			
			StringBuffer loginPage = new StringBuffer();
			
			String line;
			try {
				BufferedReader reader = new BufferedReader(new FileReader(f));
				
				while( (line = reader.readLine()) != null ){
					loginPage.append(line).append("\n");
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			out.println(loginPage.toString());
			
		}else{ // there is a session
			
			System.out.println("Session exists");
			
			// check the servlet path to check whether the user sent a logged out request
			
			String servletPath = request.getServletPath();
			System.out.println("ServletPath: " + servletPath);
			
			if( servletPath.compareTo("/logout") == 0 ){
				
				//invalidate the session and send back the main page
				session.invalidate();
				
				File f = new File("resources/index.html");
				
				StringBuffer loginPage = new StringBuffer();
				
				String line;
				try {
					BufferedReader reader = new BufferedReader(new FileReader(f));
					
					while( (line = reader.readLine()) != null ){
						loginPage.append(line).append("\n");
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				out.println(loginPage.toString());
			}
			else if( servletPath.compareTo("/lookup") == 0 ){
				
				String lookupurl = request.getParameter("url");
				
				System.out.println("lookup url: " + lookupurl);
				
				if(lookupurl != null){
					
					String content = db.getWebPage(lookupurl);
					
					if(content == null){
						out.println("Requested URL does not exist...");
					}else{
						out.println(content);
					}
					
				}else{
					out.println("url specified is empty...");
				}
				
				
				
			}
			else{ // not a log out request
				
				// display user page.
				String username = (String) session.getAttribute("user");
				
				out.println("<html>\n");
				out.println("<head>\n");
				out.println("<title> UserPage </title>\n");
				out.println("</head>\n");
				out.println("<body> " + "Welcome " + username+ "</body>\n");
				out.println("<a href=\"http://localhost:8080/logout\"><button> logout </button> </a>\n");
				out.println("</html>\n");
			}
			
		}		
		
	}

}









