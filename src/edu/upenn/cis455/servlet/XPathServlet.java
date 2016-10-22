package edu.upenn.cis455.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.*;

import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DBWrapper.User;

@SuppressWarnings("serial")
public class XPathServlet extends HttpServlet {
	
	/* TODO: Implement user interface for XPath engine here */
	
	/* You may want to override one or both of the following methods */

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		/* TODO: Implement user interface for XPath engine here */
		
		
		String envHome = getServletContext().getInitParameter("BDBstore");
		
		DBWrapper db = new DBWrapper(envHome);
		
		// check for the user in the database
		
		String username = request.getParameter("username");
		String password = request.getParameter("password");
		String dbpass = null;
		if( ( dbpass = db.getUser(username)) != null ) { // user exists
			
			if( dbpass.compareTo(password) ==0 ){ // matches
				PrintWriter out = response.getWriter();
				
				out.println("<html>\n");
				out.println("<head>\n");
				out.println("<title> UserPage </title>\n");
				out.println("</head>\n");
				out.println("<body> " + "Welcome " + username+ "</body>\n");
				out.println("<a href=\"http://localhost:8080/logout\"><button> logout </button> </a> ");
				out.println("</html>\n");
				
				// create a session for the user.
				HttpSession session;
				session = request.getSession();
				
				session.setAttribute("user", username);
				session.setAttribute("password", password);
				
				
			}else{ // wrong password, redirect to the login page.
				
				response.sendRedirect("xpath");
				
			}
			
		}else{ // create a new user
			
			db.putUser(username, password);
			
			HttpSession session;
			session = request.getSession();
			session.setAttribute("user", username);
			session.setAttribute("password", password);
			
			response.sendRedirect("xpath");
			
		}
		
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		/* TODO: Implement user interface for XPath engine here */
		
		HttpSession session;
		
		session = request.getSession(false);
		PrintWriter out = response.getWriter();
		
		if(session == null){ // there is no session, user is not logged in
			
			//File f = new File("resources/postform.html");
			File f = new File("target/register.jsp");
			
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
			
			// check the servlet path to check whether the user sent a logged out request
			
			String servletPath = request.getServletPath();
			System.out.println("ServletPath: " + servletPath);
			
			if( servletPath.compareTo("/logout") == 0 ){
				
				//invalidate the session and send back the main page
				session.invalidate();
				
				File f = new File("target/register.jsp");
				
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
			else{ // not a log out request
				
				// display user page.
				String username = (String) session.getAttribute("user");
				
				out.println("<html>\n");
				out.println("<head>\n");
				out.println("<title> UserPage </title>\n");
				out.println("</head>\n");
				out.println("<body> " + "Welcome " + username+ "</body>\n");
				out.println("<a href=\"http://localhost:8080/logout\"><button> logout </button> </a> ");
				out.println("</html>\n");
			}
			
		}		
		
	}

}









