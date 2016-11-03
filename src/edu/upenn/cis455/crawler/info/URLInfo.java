package edu.upenn.cis455.crawler.info;

public class URLInfo {
	private String method;
	private String hostName;
	private int portNo;
	private String filePath;
	private String url;
	private String baseurl;
	
	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name and file path
	 */
	public URLInfo(String docURL){
		
		url = docURL;
		
		if(docURL == null || docURL.equals(""))
			return;
		docURL = docURL.trim();
		
		if( !( docURL.startsWith("http://") || docURL.startsWith("https://") ) || docURL.length() < 8){
			//System.out.println("Failed");
			return;
		}
		// Stripping off 'http://'
		if(docURL.startsWith("http://")){
			docURL = docURL.substring(7);
		}
		else if(docURL.startsWith("https://")){
			docURL = docURL.substring(8);
		}
		/*If starting with 'www.' , stripping that off too
		if(docURL.startsWith("www."))
			docURL = docURL.substring(4);*/
		
		//System.out.println("docURL: " + docURL);
		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0,i);
		if(i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); //starts with '/'
		if(address.equals("/") || address.equals(""))
			return;
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				portNo = 80;
			}
		}else{
			hostName = address;
			portNo = 80;
		}
	}
	
	
	public URLInfo(String docURL, String method){
		
		this.method = method;
		url = docURL;
		
		if(docURL == null || docURL.equals(""))
			return;
		docURL = docURL.trim();
		
		if( !( docURL.startsWith("http://") || docURL.startsWith("https://") ) || docURL.length() < 8){
			//System.out.println("Failed");
			return;
		}
		// Stripping off 'http://'
		if(docURL.startsWith("http://")){
			docURL = docURL.substring(7);
		}
		else if(docURL.startsWith("https://")){
			docURL = docURL.substring(8);
		}
		/*If starting with 'www.' , stripping that off too
		if(docURL.startsWith("www."))
			docURL = docURL.substring(4);*/
		
		//System.out.println("docURL: " + docURL);
		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0,i);
		if(i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); //starts with '/'
		if(address.equals("/") || address.equals(""))
			return;
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				portNo = 80;
			}
		}else{
			hostName = address;
			portNo = 80;
		}
	}
	
	
//
//	public URLInfo(String hostName, String filePath){
//		this.hostName = hostName;
//		this.filePath = filePath;
//		this.portNo = 80;
//	}
	
//	public URLInfo(String hostName,int portNo,String filePath){
//		this.hostName = hostName;
//		this.portNo = portNo;
//		this.filePath = filePath;
//	}
	
	public String getMethod(){
		return method;
	}
	
	public void setMethod(String method){
		this.method = method;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public void setHostName(String s){
		hostName = s;
	}
	
	public int getPortNo(){
		return portNo;
	}
	
	public void setPortNo(int p){
		portNo = p;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public void setFilePath(String fp){
		filePath = fp;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setBaseUrl(String base){
		baseurl = base;
	}
	
	public String getBaseUrl() {
		
		if( filePath.endsWith(".html") == true ){ // this url was pointing to an .html file not a servlet?
			
			int slash = filePath.lastIndexOf("/");
			String filename = filePath.substring(slash+1);
			return url.replaceAll(filename, "");
			
		}
		
		return url;
	}
	
	
}
