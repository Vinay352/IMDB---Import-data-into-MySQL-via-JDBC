package edu.rit.ibd.a1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;

public class IMDBToSQL {

	public static void main(String[] args) throws Exception {

		final String a = args[0];
		final String b = args[1];
		final String c = args[2];
		final String folderToIMDBGZipFiles = args[3];

		Connection con = DriverManager.getConnection(a,b,c);
		con.setAutoCommit(false);

		String sql_delete = "DROP TABLE IF EXISTS Movie, Genre, MovieGenre, Person, Actor, Director, KnownFor, Producer, Writer ";
		con.prepareStatement(sql_delete).execute();

		// CREATE query for Movie table
		String sql_0 = "CREATE TABLE Movie" +
				"( id INT, " +
				" ptitle VARCHAR(450), " +
				" otitle VARCHAR(450), " +
				" adult INT, " +
				" year INT, " +
				" runtime INT, " +
				" rating FLOAT(3,2), " +
				" totalvotes INT, " +
				" PRIMARY KEY (id) " +
				" )";

		// CREATE query for Genre table
		String sql_1 = "CREATE TABLE Genre" +
				"( id INT, " +
				" name VARCHAR(30), " +
				" PRIMARY KEY (id)" +
				" )";

		con.prepareStatement(sql_0).execute();
		con.prepareStatement(sql_1).execute();



		BufferedReader br_ratings = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"))));

		int cnt = 0;
		HashMap<Integer, List> ratingsDict = new HashMap<>();
		String ratings_line;

		// ratings dictionary created with tconst (after preprocessing) as keys and ratings and votes in array as values
		while( ( ratings_line = br_ratings.readLine() ) != null ){
			if( cnt != 0 ){
				String[] temp = ratings_line.split("\t");

				String temp1 = temp[0].strip().substring(2);
				int t = Integer.parseInt(temp1);

				List<Object> ratingVotesArr = new ArrayList<>(2);

				ratingVotesArr.add(Float.parseFloat(temp[1]));
				ratingVotesArr.add(Integer.parseInt(temp[2]));

				// System.out.println(ratingVotesArr);

				ratingsDict.put(t, ratingVotesArr);
			}
			cnt = 1;
		}

		br_ratings.close();

		BufferedReader br = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"))));

		String line;
		int movieCounter = 0;
		int counter = 0;
		Hashtable<Integer, String> movieDict = new Hashtable();

		Set<String> genreSet = new HashSet<String>();

		PreparedStatement st_movie = con.prepareStatement("INSERT IGNORE INTO Movie(id, ptitle, otitle, adult, year, runtime, rating, totalvotes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

		while ( ( line = br.readLine() ) != null ) {

			if(counter != 0){
				String[] temp = line.split("\t");

				// removing tt or nn from beginning of IDs and removing leading zeroes
				if ( temp[1].equals("movie") ){

					String temp1 = temp[0].strip().substring(2);
					int t = Integer.parseInt(temp1);

					movieCounter++;

					movieDict.put(t, temp[8]);

					// System.out.println(line);

					// id
					st_movie.setInt(1, t);

					//ptitle
					if( !temp[2].strip().equals("\\N") )
						st_movie.setString(2, temp[2].strip());
					else
						st_movie.setNull(2, Types.VARCHAR);

					//otitle
					if( !temp[3].strip().equals("\\N") )
						st_movie.setString(3, temp[3].strip());
					else
						st_movie.setNull(3, Types.VARCHAR);

					//adult
					if( !temp[4].strip().equals("\\N") )
						st_movie.setInt(4, Integer.parseInt(temp[4].strip()));
					else
						st_movie.setNull(4, Types.INTEGER);

					//year
					if( !temp[5].strip().equals("\\N") )
						st_movie.setInt(5, Integer.parseInt(temp[5].strip()));
					else
						st_movie.setNull(5, Types.INTEGER);

					//runtime
					if( !temp[7].strip().equals("\\N") )
						st_movie.setInt(6, Integer.parseInt(temp[7].strip()));
					else
						st_movie.setNull(6, Types.INTEGER);

					List<Object> tempArrayRatings = ratingsDict.get(t);
					//System.out.println( t + " ----- " + tempArrayRatings );

					//System.out.println(t + " ------ " + "rating " + tempArrayRatings.get(0) + tempArrayRatings.get);

					if( tempArrayRatings == null ){
						//rating
						st_movie.setNull(7, Types.FLOAT);

						//totalvotes
						st_movie.setNull(8, Types.INTEGER);
					}
					else {
						//rating
						st_movie.setFloat(7, (Float) tempArrayRatings.get(0));

						//totalvotes
						st_movie.setInt(8, (Integer) tempArrayRatings.get(1)); ;
					}

					st_movie.addBatch();

					if(counter == 50000){
						counter = 1;
						st_movie.executeBatch();
						con.commit();
					}

					// System.out.println(t + " :ID ------ " + line);
				}

				// creating set of unique genres of the movies with movie type = movie
				if ( temp[1].equals("movie") ){
					if(!temp[temp.length - 1].equals("\\N")){
						String[] temp2 = temp[temp.length - 1].strip().split(",");
						for(String genreType : temp2){
							// System.out.print(qwe + " ");
							genreSet.add(genreType);
						}
						// System.out.println();
					}
				}
			}
			counter++;
		}
		st_movie.executeBatch();
		con.commit();

		br.close();
		st_movie.close();

		//System.out.println(genreSet);

		int genreCounter = 0;
		counter = 0;
		PreparedStatement st = con.prepareStatement("INSERT IGNORE INTO Genre(id, name) VALUES (?,?)");

		Hashtable<String, Integer> genreDict = new Hashtable();

		for(String temp_genreSet : genreSet){
			genreCounter++;
			counter++;

			genreDict.put(temp_genreSet, genreCounter);

			st.setInt(1,genreCounter);
			st.setString(2, temp_genreSet);
			st.addBatch();
			if(counter == 50000){
				counter = 1;
				st.executeBatch();
				con.commit();
			}

		}

		st.executeBatch();
		con.commit();
		st.close();


		// CREATE query for MovieGenre table
		String sql_2 = "CREATE TABLE MovieGenre" +
				"( mid INT, " +
				" gid INT, " +
				" PRIMARY KEY (mid, gid) " +
				" )";

		con.prepareStatement(sql_2).execute();

		PreparedStatement st_movieGenre = con.prepareStatement("INSERT IGNORE INTO MovieGenre(mid, gid) VALUES (?,?)");

		//con.setAutoCommit(true);

		int movieGenreCounter = 0;
		//System.out.println("-----------"+movieDict.size());
		for( int item : movieDict.keySet()){
			//System.out.println(item);

			String temp_hold = movieDict.get(item);

			if( !temp_hold.equals("\\N")) {
				movieGenreCounter++;
				//System.out.println(movieGenreCounter);
				String[] tempArr = temp_hold.split(",");
				for (String tempStringMovieDict : tempArr) {

					st_movieGenre.setInt(1, item);
					st_movieGenre.setInt(2, genreDict.get(tempStringMovieDict));

					//System.out.println(item+ " : item------" + " " + genreDict.get(tempStringMovieDict));

					st_movieGenre.addBatch();

				}

				if( movieGenreCounter == 100000 ){
					movieGenreCounter = 1;
					st_movieGenre.executeBatch();
					con.commit();
				}
			}

		}
		// System.out.println(movieGenreCounter);
		st_movieGenre.executeBatch();
		con.commit();
		st_movieGenre.close();




		// CREATE QUERY for Person table
		String sql_3 = "CREATE TABLE Person" +
				"( id INT, " +
				" name VARCHAR(150), " +
				" byear INT, " +
				" dyear INT, " +
				" PRIMARY KEY (id) " +
				" )";

		con.prepareStatement(sql_3).execute();

		PreparedStatement st_person = con.prepareStatement("INSERT IGNORE INTO Person(id, name, byear, dyear) VALUES (?, ?, ?, ?)");

		BufferedReader br_person = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"))));

		int personCounter = 0;
		counter = 0;

		while( ( line = br_person.readLine() ) != null ){
			if(counter != 0){
				String[] temp = line.split("\t");
				personCounter++;

				String temp1 = temp[0].strip().substring(2);
				int t = Integer.parseInt(temp1);

				// id
				st_person.setInt(1, t);

				// name
				if( !temp[1].strip().equals("\\N") )
					st_person.setString(2, temp[1].strip());
				else
					st_person.setNull(2, Types.VARCHAR);

				// byear
				if( !temp[2].strip().equals("\\N") )
					st_person.setInt(3, Integer.parseInt(temp[2].strip()));
				else
					st_person.setNull(3, Types.INTEGER);

				// dyear
				if( !temp[3].strip().equals("\\N") )
					st_person.setInt(4, Integer.parseInt(temp[3].strip()));
				else
					st_person.setNull(4, Types.INTEGER);

				st_person.addBatch();

				if(personCounter == 350000){
					personCounter = 1;
					st_person.executeBatch();
					con.commit();
				}
			}
			counter = 1;
		}
		st_person.executeBatch();
		con.commit();

		br_person.close();
		st_person.close();






		// CREATE query for KnownFor table
		String sql_6 = "CREATE TABLE KnownFor" +
				"( mid INT, " +
				" pid INT, " +
				" PRIMARY KEY (mid, pid) " +
				" )";

		con.prepareStatement(sql_6).execute();

		PreparedStatement st_knownFor = con.prepareStatement("INSERT IGNORE INTO KnownFor(mid, pid) VALUES (?, ?)");

		BufferedReader br_knownFor = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"))));

		int knownForCounter = 0;
		counter = 0;

		while( ( line = br_knownFor.readLine() ) != null ){
			if(counter != 0){
				String[] temp = line.split("\t");

				String temp1 = temp[0].strip().substring(2);
				int t = Integer.parseInt(temp1);

				if( !temp[5].equals("\\N") ) {
					knownForCounter++;

					String[] tempArr = temp[5].split(",");

					for (String tempStringKnownFor : tempArr) {

						String temp2 = tempStringKnownFor.strip().substring(2);
						int t2 = Integer.parseInt(temp2);

						if (movieDict.containsKey(t2)){
							// pid
							st_knownFor.setInt(2, t);

							// mid
							st_knownFor.setInt(1, t2);

							st_knownFor.addBatch();
						}

					}

					if( knownForCounter == 150000 ){
						knownForCounter = 1;
						st_knownFor.executeBatch();
						con.commit();
					}
				}

			}
			counter = 1;
		}
		st_knownFor.executeBatch();
		con.commit();

		br_knownFor.close();
		st_knownFor.close();




		// CREATE query for Actor table
		String sql_4 = "CREATE TABLE Actor" +
				"( mid INT, " +
				" pid INT, " +
				" PRIMARY KEY (mid, pid) " +
				" )";

		// CREATE query for Director table
		String sql_5 = "CREATE TABLE Director" +
				"( mid INT, " +
				" pid INT, " +
				" PRIMARY KEY (mid, pid) " +
				" )";

		// CREATE query for Producer table
		String sql_7 = "CREATE TABLE Producer" +
				"( mid INT, " +
				" pid INT, " +
				" PRIMARY KEY (mid, pid) " +
				" )";

		// CREATE query for Writer table
		String sql_8 = "CREATE TABLE Writer" +
				"( mid INT, " +
				" pid INT, " +
				" PRIMARY KEY (mid, pid) " +
				" )";

		con.prepareStatement(sql_4).execute();
		con.prepareStatement(sql_5).execute();
		con.prepareStatement(sql_7).execute();
		con.prepareStatement(sql_8).execute();

		PreparedStatement st_actor = con.prepareStatement("INSERT IGNORE INTO Actor(mid, pid) VALUES (?, ?)");
		PreparedStatement st_producer = con.prepareStatement("INSERT IGNORE INTO Producer(mid, pid) VALUES (?, ?)");
		PreparedStatement st_director = con.prepareStatement("INSERT IGNORE INTO Director(mid, pid) VALUES (?, ?)");
		PreparedStatement st_writer = con.prepareStatement("INSERT IGNORE INTO Writer(mid, pid) VALUES (?, ?)");


		BufferedReader br_crew = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"))));


		counter = 0;
		int crewCounter = 0;

		while( ( line = br_crew.readLine() ) != null && crewCounter < 30 ) {
			if( counter !=0 ){
				String[] temp = line.split("\t");

				String temp1 = temp[0].strip().substring(2);

				// mid
				int t1 = Integer.parseInt(temp1);

				if (movieDict.containsKey(t1)){
					if( !temp[1].equals("\\N") || !temp[2].equals("\\N") ){

						crewCounter++;

						// create director dictionary
						if( !temp[1].equals("\\N") ){

							String[] tempDirector = temp[1].strip().split(",");
							for(String tempDir : tempDirector){
								st_director.setInt(1, t1);
								st_director.setInt(2, Integer.parseInt(tempDir.strip().substring(2)));
								st_director.addBatch();
							}
						}

						// create writer dictionary
						if( !temp[2].equals("\\N") ){

							String[] tempDirector = temp[2].strip().split(",");
							for(String tempDir : tempDirector){
								st_writer.setInt(1, t1);
								st_writer.setInt(2, Integer.parseInt(tempDir.strip().substring(2)));
								st_writer.addBatch();
							}
						}
					}

					if( crewCounter == 50000 ){
						crewCounter = 1;
						st_director.executeBatch();
						st_writer.executeBatch();
						con.commit();
					}

				}
			}
			counter = 1;
		}

		st_director.executeBatch();
		st_writer.executeBatch();
		con.commit();

//        System.out.println(directorDict);
//        System.out.println("-----");
//        System.out.println(writerDict);

		br_crew.close();

		BufferedReader br_principals = new BufferedReader( new InputStreamReader(
				new GZIPInputStream(
						new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"))));

		int batchCounter = 0;
		counter = 0;

		while( ( line = br_principals.readLine() ) != null ){

			if(counter != 0){
				String[] temp = line.split("\t");

				String temp1 = temp[0].strip().substring(2);
				int t1 = Integer.parseInt(temp1);

				if (movieDict.containsKey(t1)){
					String temp2 = temp[2].strip().substring(2);
					int t2 = Integer.parseInt(temp2);

					batchCounter++;

					if ( temp[3].equals("actor") || temp[3].equals("actress") || temp[3].equals("self") ){
						st_actor.setInt(1, t1);
						st_actor.setInt(2, t2);
						st_actor.addBatch();
					}
					else if ( temp[3].equals("producer") ){
						st_producer.setInt(1, t1);
						st_producer.setInt(2, t2);
						st_producer.addBatch();
					}
					else if ( temp[3].equals("director") ){
						st_director.setInt(1, t1);
						st_director.setInt(2, t2);
						st_director.addBatch();
					}
					else if ( temp[3].equals("writer") ){
						st_writer.setInt(1, t1);
						st_writer.setInt(2, t2);
						st_writer.addBatch();
					}
				}

				if( batchCounter == 500000 ){
					batchCounter = 1;
					st_actor.executeBatch();
					st_producer.executeBatch();
					st_writer.executeBatch();
					st_director.executeBatch();
					con.commit();
				}

			}
			counter = 1;

		}
		st_actor.executeBatch();
		st_producer.executeBatch();
		st_writer.executeBatch();
		st_director.executeBatch();
		con.commit();

		br_principals.close();


		st_actor.close();
		st_producer.close();
		st_writer.close();
		st_director.close();
		con.close();

	}
}
