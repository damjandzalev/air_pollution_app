package air_pollution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class DataConnection {
	String urlDatabase = "jdbc:postgresql://localhost:5432/postgres";
	String userDatabase;
	String passwordDatabase;
    Connection conn = null;
    long k = 0;
	
	public String getUrlDatabase() {
		return urlDatabase;
	}

	public String getUserDatabase() {
		return userDatabase;
	}

	public void setUserDatabase(String userDatabase) {
		this.userDatabase = userDatabase;
	}

	public void setPasswordDatabase(String passwordDatabase) {
		this.passwordDatabase = passwordDatabase;
	}

	public boolean connect() {
		if(userDatabase == null || passwordDatabase == null)
			return false;
        try {
            conn = DriverManager.getConnection(urlDatabase, userDatabase, passwordDatabase);
            System.out.println("Connected to the PostgreSQL server successfully.");
			return true;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }
	
	public String getSimPointId(String longitude, String latitude) {
		String SQL = "SELECT id FROM air_pollution.simulation_point "+
						String.format("WHERE LONGITUDE = %s and LATITUDE = %s", longitude, latitude);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SQL)) {
            ++k;
        	//System.out.println(SQL);
            if(rs.next()) {
            	return rs.getString(1);
            }
            return null;
        } catch (SQLException ex) {
            System.out.println(ex.getMessage()+" "+SQL);
        }
		return null;
	}
	
	public String getSimRunId(String time, String run_time) {
		String SQL = "SELECT id FROM air_pollution.simulation_run "+
						String.format("WHERE run_time = '%s' and time = '%s'", run_time, time);
		try (Statement stmt = conn.createStatement();
		        ResultSet rs = stmt.executeQuery(SQL)) {
            ++k;
            //System.out.println(SQL);
			if(rs.next()) {
				return rs.getString("id");
			}
		} catch (SQLException ex) {
		    System.out.println(ex.getMessage()+" "+SQL);
		}
		return null;
	}
	
	public String getPredictionPollutantId(String sim_p_id, String sim_r_id) {
		String SQL = "SELECT id FROM air_pollution.prediction_pollutant "+
				String.format("WHERE simulation_run_id = %s and simulation_point_id = %s", sim_r_id, sim_p_id);
		try (Statement stmt = conn.createStatement();
		        ResultSet rs = stmt.executeQuery(SQL)) {
            ++k;
            //System.out.println(SQL);
			if(rs.next()) {
				return rs.getString("id");
			}
		} catch (SQLException ex) {
		    System.out.println(ex.getMessage()+" "+SQL);
		}
		return null;
	}
	
	public void insertIntoSimPoint(String longitude, String latitude) {
		String SQL = "INSERT INTO air_pollution.simulation_point(longitude, latitude) " 
				+ String.format(" VALUES(%s, %s)", longitude, latitude);
		// insert the data
		if(getSimPointId(longitude, latitude)==null) {
			try {
	            Statement stmt = conn.createStatement();
	            stmt.executeUpdate(SQL);
	            ++k;
	            //System.out.println(SQL);
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage()+" "+SQL);
	        }
		}
	}
	
	public void insertIntoSimRun(String time, String run_time)  {
		time = time.substring(0,10)+" "+ time.substring(11);
		String SQL = "INSERT INTO air_pollution.simulation_run(time, run_time)" 
				+ String.format(" VALUES('%s'::timestamp, '%s'::timestamp)", time, run_time);
		// insert the data
		try  {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(SQL);
            ++k;
            //System.out.println(SQL);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage()+" "+SQL);
        }
	}
	
	public void insertIntoPredictionPollutant(String sim_r_id, String sim_p_id, String p1, String p2, String p3, String p4, String p5, String p6, String p7, String p8, String p9, String p10, String p11) {
		String SQL = "INSERT INTO air_pollution.prediction_pollutant(simulation_run_id, simulation_point_id, p1, p2, p3, p4, p5, p6, p7 ,p8 ,p9 ,p10, p11)" 
				+ String.format(" VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", sim_r_id, sim_p_id, p1, p2, p3, p4 ,p5 ,p6 ,p7 ,p8 ,p9 ,p10, p11);
		// insert the data
		try  {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(SQL);
            ++k;
            //System.out.println(SQL);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage()+" "+SQL);
        }
		/*String SQL = "UPDATE air_pollution.prediction_pollutant SET P"+polutant_id +" = " + amount 
				+ String.format(" WHERE ID = %s", prediction_pollutant_id);
		// insert the data
		try  {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(SQL);
            ++k;
            //System.out.println(SQL);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage()+" "+SQL);
        }*/
	}
	
	public DataConnection(String userDatabase, String passwordDatabase) {
		this.userDatabase = userDatabase;
		this.passwordDatabase = passwordDatabase;
	}
}