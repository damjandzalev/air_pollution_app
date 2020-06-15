package air_pollution;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Main {
	public static void main(String[] args) throws Exception {
		DataConnection app = new DataConnection("", "");
        if(System.console() == null) {
        	System.out.println("No console available!");
            @SuppressWarnings("resource")
			Scanner input = new Scanner(System.in);
			while(app.connect() == false) {
		        String username, password;
				System.out.println("Username: ");
		        username = input.nextLine();
				System.out.println("Password: ");
		        password = input.nextLine();
		        app.setPasswordDatabase(password);
		        app.setUserDatabase(username);
			}
        }
        else {
            Console input = System.console();
			while(app.connect() == false) {
		        String username, password;
				System.out.println("Username: ");
		        username = new String(input.readLine());
				System.out.println("Password: ");
		        password = new String(input.readPassword());
		        app.setPasswordDatabase(password);
		        app.setUserDatabase(username);
			}
        }
		File file = new File("sim_data.csv");
		BufferedReader inputFile = new BufferedReader(new FileReader(file));
		List<String> lines = new ArrayList<String>() ;
		String s = inputFile.readLine();
		while(s!= null) {
			lines.add(s);
			s = inputFile.readLine();
		}
		inputFile.close();
		String run_time = "2018-03-01 00:00:00.000000";
		Map<Point, String> points = new TreeMap<Point, String>();
		List<SimRun> sim_runs = new ArrayList<SimRun>();
		long timeStart = System.currentTimeMillis();
		Map<PredictionPollutant, Pollutants> pps = new TreeMap<PredictionPollutant, Pollutants>();
		
		//long k = 0, timeExec = timeStart;
		for(String p : lines) {
			String l[] = p.split(",");
			String time = l[0].substring(11, 30);
			String latitude = l[1];
			String longitude = l[2];
			String polutant_id = l[3];
			String amount = l[4];
			Point point = new Point(longitude, latitude);
			String sim_p_id = "";
			if(!points.containsKey(point)) {
				String id = app.getSimPointId(longitude, latitude);
				if(id == null) {
					app.insertIntoSimPoint(longitude, latitude);
					id = app.getSimPointId(longitude, latitude);
				}
				points.put(point, id);
				sim_p_id = points.get(point);
			}
			else {
				sim_p_id = points.get(point);
			}
			
			SimRun sim_run = new SimRun(time, run_time);
			if(!sim_runs.contains(sim_run)) {
				String id = app.getSimRunId(time, run_time);
				if(id == null) {
					app.insertIntoSimRun(time, run_time);
					id = app.getSimRunId(time, run_time);
				}
				sim_run.setId(id);
				sim_runs.add(sim_run);
			}
			else {
				sim_run = sim_runs.get(sim_runs.indexOf(sim_run));
			}
			PredictionPollutant pp = new PredictionPollutant(sim_p_id, sim_run.id);
			Pollutants pollutants = new Pollutants();
			if(pps.containsKey(pp)) {
				pollutants = pps.get(pp);
			}
			else {
				pps.put(pp, pollutants);
			}
			switch(polutant_id) {
				case "1": pollutants.p1 = amount;break;
				case "2": pollutants.p2 = amount;break;
				case "3": pollutants.p3 = amount;break;
				case "4": pollutants.p4 = amount;break;
				case "5": pollutants.p5 = amount;break;
				case "6": pollutants.p6 = amount;break;
				case "7": pollutants.p7 = amount;break;
				case "8": pollutants.p8 = amount;break;
				case "9": pollutants.p9 = amount;break;
				case "10": pollutants.p10 = amount;break;
				case "11": pollutants.p11 = amount;break;
			}
			/*if(k%10000 == 0) {
				System.out.println(String.format("%f - %d", k/((System.currentTimeMillis()-timeExec)/1000.0), pps.size()));
				timeExec = System.currentTimeMillis();
				k = 0;
			}
			++k;*/
		}
		System.out.println(String.format("%f", (System.currentTimeMillis() - timeStart)/1000.0));
		String sqlQuery = "INSERT INTO air_pollution.prediction_pollutant(simulation_run_id, simulation_point_id, p1, p2, p3, p4, p5, p6, p7 ,p8 ,p9 ,p10, p11) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try{
			PreparedStatement pstmt = app.conn.prepareStatement(sqlQuery);
			app.conn.setAutoCommit(false);
			for(PredictionPollutant pp : pps.keySet()){
				Pollutants pollutants = pps.get(pp);
				pstmt.setInt(1,Integer.parseInt(pp.sim_r_id));
				pstmt.setInt(2,Integer.parseInt(pp.sim_p_id));
				pstmt.setDouble(3,Double.parseDouble(pollutants.p1));
				pstmt.setDouble(4,Double.parseDouble(pollutants.p2));
				pstmt.setDouble(5,Double.parseDouble(pollutants.p3));
				pstmt.setDouble(6,Double.parseDouble(pollutants.p4));
				pstmt.setDouble(7,Double.parseDouble(pollutants.p5));
				pstmt.setDouble(8,Double.parseDouble(pollutants.p6));
				pstmt.setDouble(9,Double.parseDouble(pollutants.p7));
				pstmt.setDouble(10,Double.parseDouble(pollutants.p8));
				pstmt.setDouble(11,Double.parseDouble(pollutants.p9));
				pstmt.setDouble(12,Double.parseDouble(pollutants.p10));
				pstmt.setDouble(13,Double.parseDouble(pollutants.p11));
				pstmt.addBatch(); 
			}
			int[] result = pstmt.executeBatch();
			System.out.println("The number of rows inserted in air_pollution.prediction_pollutant: "+ result.length);
			app.conn.commit();
		}
		catch(Exception e){
			e.printStackTrace();
			app.conn.rollback();
		}
		System.out.println("The number of rows inserted in air_pollution.simulation_points: "+ points.size());
		System.out.println("The number of rows inserted in air_pollution.simulation_run: "+ sim_runs.size());
		System.out.println(String.format("Duration: %f seconds", (System.currentTimeMillis() - timeStart)/1000.0));
	}
}