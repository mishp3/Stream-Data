package com.stream.streamingdata;

/**
 * @author mishp3
 */
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

public class StreamData {

	private Connection conn = null;
	private PreparedStatement psmnt= null;
	
	public static void main(String args[])throws IOException{
		StreamData stream = new StreamData();
		stream.sendStream();
	}
	
	public StreamData(){
		conn = JDBCConnection.getConnection();
	}
	
	/**
	 * Fetches customer transaction information from fsf_sample_cctran table and sends it to CreditCardTransactionStream data set
	 */
	public void sendStream(){
		try {
			psmnt = conn.prepareStatement("select * from fsf_sample_cctran");
			ResultSet rs = psmnt.executeQuery();
			double CCT_TRANAMOUNT=0.00, CCT_ACCTNBR = 0.00;
			while(rs.next()){
				CCT_TRANAMOUNT = rs.getDouble("CCT_TRANAMOUNT");
				CCT_ACCTNBR = rs.getDouble("CCT_ACCTNBR");
				JSONObject json = new JSONObject();
				json.put("CCT_TRANAMOUNT", CCT_TRANAMOUNT);
				json.put("CCT_ACCTNBR",CCT_ACCTNBR);
				System.out.println("Sending CCT_ACCTNBR: "+ CCT_ACCTNBR);
				sendData(json);
				//System.out.println("Sent data...");
			}	
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Sends json data to REST service of CreditCardTransactionStream data set.
	 * 
	 * @param json contains the transaction data: Transaction amount and account number. 
	 */
	private void sendData(JSONObject json) {
		String url = "http://10.61.9.168:7003/stream/CreditCardTransactionStream";
		URL obj;
		try {
			obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			String userPassword = "MarketingAdministratorFS" + ":" + "install";
			String encoding = new String(new Base64().encode(userPassword.getBytes()));
			con.setRequestMethod("POST");
			con.setRequestProperty("Authorization", "Basic " + encoding);
			con.setDoOutput(true);
			OutputStream os = con.getOutputStream();
			//System.out.println("Before response code..");
			os.write(json.toString().getBytes("UTF-8"));
			con.connect();
			int status = con.getResponseCode();
			System.out.println("Status: "+ status);
			os.flush();
			os.close();
		} catch (MalformedURLException e) {
			System.out.println("MalformedURLException: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
