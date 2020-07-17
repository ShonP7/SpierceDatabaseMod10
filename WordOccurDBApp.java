import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class WordOccurDBApp {
	

	public static void main(String[] args) throws SQLException, FileNotFoundException {
			
			String wordOccurArrayPath="WordOccurArray.csv";
			int batchSize = 469;
			
			Statement statement = connect().createStatement();
			PreparedStatement importStatement = connect().prepareStatement("insert into word_occurrences.word_tbl(term,count) values (?,?)");
			statement.executeUpdate("insert into word_occurrences.word_tbl(term,count) values ('from_java', 00)");
			
			try (BufferedReader lineReader = new BufferedReader(new FileReader(wordOccurArrayPath));){
				String lineText = null;
				
				int index = 0;
				lineReader.readLine();
				
				while((lineText = lineReader.readLine()) != null) {
					String[] data = lineText.split(",");
					String term = data[0];
					String count = data[1];
					
					importStatement.setString(1, term);	
					importStatement.setString(2, count);
					
					Float floatCount = Float.parseFloat(count);
	                importStatement.setFloat(2, floatCount);
					
					importStatement.addBatch();
					
					 if (index % batchSize == 0) {
		                    importStatement.executeBatch();
		                }
				}
					
				importStatement.executeUpdate();	
				
			} catch (IOException e) {
				e.printStackTrace();
			}
				
			
			ResultSet wordTblResult = statement.executeQuery("select * from word_occurrences.word_tbl");	
				while(wordTblResult.next()) {
					System.out.println(wordTblResult.getString("term"));
					System.out.println("count");
				}	
			
				} 
	
	
		private static Connection connect(){
			
			Connection conn = null;
			String url = "jdbc:mysql://localhost:3306/word_occurrences";
			
			
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(url, "root", "T@S$10nsdp1008");
				System.out.println("Connected!");
				
			} catch (ClassNotFoundException | SQLException e) { 
				
				System.err.println(e.getClass().getName() + ": " + e.getMessage() );
				System.out.println(e+"");
				System.exit(0);
				
			}
			return conn;		
			
		}
	}
	
	
	



//String term = null;
//
//Statement statement = connect().createStatement();
//
//try(Scanner theRaven = new Scanner(new File ("theRaven.txt"));){
//	
//	while(theRaven.hasNextLine()) {
//		term = "";
//		String line; 	
//		
//		line = theRaven.nextLine();
//	}
//	
//} catch (FileNotFoundException e) {
//	e.printStackTrace();
//}
//
////execute the statement object
//ResultSet wordTblResult = statement.executeQuery("select * from word_occurrences.word_tbl");	
//	
//	//process the result
//	while(wordTblResult.next()) {
//		System.out.println(wordTblResult.getString("term"));
//	}	
//	
//	//statement.executeUpdate("insert into word_occurrences.word_tbl(count) values (09)");
//	//statement.executeUpdate("insert into word_occurrences.word_tbl(term) values ('thank you')");
//
//	try(Connection conn = connect();
//			PreparedStatement termImport = conn.prepareStatement("insert into word_occurrences.word_tbl(term) values(?)")){
//		
//		termImport.setString(1, term);
//		
//		termImport.executeUpdate();
//		
//	}catch(SQLException s) {
//		System.out.println(s);
//		
//	}

//try(Scanner theRaven = new Scanner(new File ("theRaven.txt"));){
//	
//	while(theRaven.hasNextLine()) {
//		term = "";
//		String line; 	
//		
//		line = theRaven.nextLine();
//	}
//	
//} catch (FileNotFoundException e) {
//	e.printStackTrace();
//}

