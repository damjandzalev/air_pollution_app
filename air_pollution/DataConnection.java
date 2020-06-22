package air_pollution_app.air_pollution;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

class DataConnection {
    String urlDatabase = "jdbc:postgresql://localhost:5432/postgres";
    String userDatabase;
    String passwordDatabase;
    Connection conn = null;
    //private String cities[] = {"bitola", "gostivar", "kichevo", "kumanovo", "novoselo", "ohrid", "shtip", "skopje", "strumica", "tetovo"};
    private String[] cities = {"skopje"};

    public DataConnection(String userDatabase, String passwordDatabase) {
        this.userDatabase = userDatabase;
        this.passwordDatabase = passwordDatabase;
        connect();
    }

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

    public void writeSimDataToDB() throws SQLException {
        long start_time = System.currentTimeMillis();
        try{
            System.out.println("Reading file...");
            File file = new File("simulation_data.csv");
            Path path = file.toPath();
            BasicFileAttributes fatr = Files.readAttributes(path, BasicFileAttributes.class);
            String creationTime = fatr.creationTime().toString().substring(0,4)+fatr.creationTime().toString().substring(5,7)+fatr.creationTime().toString().substring(8,10);
            String run_time = fatr.creationTime().toString().substring(0,10)+" "+fatr.creationTime().toString().substring(11,26);
            System.out.println(String.format("Sim run time : %s", run_time));
            try {
                String createTable = "CREATE TABLE imports.simulation_data" + creationTime;
                createTable = createTable + " (time TEXT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, pollutant_id INTEGER, amount DOUBLE PRECISION);";
                conn.createStatement().executeUpdate(createTable);
            }
            catch(Exception e){
                System.out.println(e.getMessage());
                return;
            }


            System.out.println("Inserting data into database...");
            String sqlQuery = "INSERT INTO imports.simulation_data"+creationTime+" (time, latitude, longitude, pollutant_id, amount) values (?,?,?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
            conn.setAutoCommit(false);
            BufferedReader input = new BufferedReader(new FileReader(file));
            String s = input.readLine();
            long k = 0;
            while(s != null) {
                String[] l = s.split(",");
                String time = l[0];
                String latitude = l[1];
                String longitude = l[2];
                String pollutant_id = l[3];
                String amount = l[4];
                s = input.readLine();

                pstmt.setString(1,time);
                pstmt.setDouble(2,Double.parseDouble(latitude));
                pstmt.setDouble(3,Double.parseDouble(longitude));
                pstmt.setInt(4,Integer.parseInt(pollutant_id));
                pstmt.setDouble(5,Double.parseDouble(amount));
                pstmt.addBatch();
                ++k;
                if(k%500000 == 0){
                    System.out.println(String.format("Inserting 500000 rows, %d total.", k));
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
            System.out.println(String.format("Inserting %d rows, %d total.", k%500000, k));
            pstmt.executeBatch();
            conn.commit();
            Statement stmt;
            ResultSet rs;
            conn.setAutoCommit(false);
            System.out.println("Normalizing the data in the database...");
            sqlQuery = "SELECT air_pollution.normalizeSimPoints('imports.simulation_data"+creationTime+"')";
            System.out.println("Inserting new points in simulation_point.");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();
            sqlQuery = "SELECT air_pollution.normalizeSimRuns('imports.simulation_data"+creationTime+"', '"+run_time+"')";
            System.out.println("Inserting new rows in simulation_run.");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();


            sqlQuery = "SELECT air_pollution.normalizeSimDataView('imports.simulation_data"+creationTime+"', '"+run_time+"')";
            System.out.println("Creating view for the sim data with the id's from the new runs and points.");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();

            sqlQuery = "SELECT air_pollution.normalizeSimDataInsert('"+run_time+"')";
            System.out.println("Inserting new rows in prediction_pollutant.");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();
            for(int i = 1; i <= 11; ++i){
                sqlQuery = "SELECT air_pollution.normalizeSimDataUpdate("+i+", '"+run_time+"')";
                System.out.println("Inserting values in the new rows for the pollutant with "+i+" id.");
                stmt = conn.createStatement();
                rs = stmt.executeQuery(sqlQuery);
                conn.commit();
            }
            System.out.println("Done!");

            long end_time = System.currentTimeMillis();
            long hours = (end_time-start_time)/3600000;
            long mins = ((end_time-start_time)%3600000)/60000;
            long seconds = (((end_time-start_time)%3600000)%60000)/1000;
            long millis = (((end_time-start_time)%3600000)%60000)%1000;
            System.out.println(String.format("Total time for the simulation data: %02d:%02d:%02d.%03d", hours, mins, seconds, millis));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void runMeasurementsData(){
        String username = "", password = "";
        if(System.console() == null) {
            System.out.println("No console available!");
            Scanner input = new Scanner(System.in);
            System.out.println("Username for pulse.eco: ");
            username = input.nextLine();
            System.out.println("Password for pulse.eco: ");
            password = input.nextLine();
        }
        else {
            Console input = System.console();
            System.out.println("Username for pulse.eco: ");
            username = new String(input.readLine());
            System.out.println("Password for pulse.eco: ");
            password = new String(input.readPassword());
        }
        String plainCreds = username + ":" + password;
        byte[] plainCredsBytes = plainCreds.getBytes(StandardCharsets.UTF_8);
        byte[] base64CredsBytes = org.apache.tomcat.util.codec.binary.Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes, StandardCharsets.UTF_8);

        //Create HTTP Header and RestTemplate object
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Basic " + base64Creds);
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity requestEntity = new HttpEntity(headers);

        //Execute the request
        List<RawData> rawDataList = new ArrayList<RawData>();


        for(String city : cities) {
            try {
                String url = String.format("https://%s.pulse.eco/rest/data24h", city);
                RawData[] output = restTemplate.exchange(url,
                        HttpMethod.GET, requestEntity, RawData[].class).getBody();
                if(output!= null)
                    rawDataList.addAll(Arrays.asList(output));
            }
            catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
        writeMeasurementsToFile(rawDataList);
    }

    private void writeMeasurementsToFile(List<RawData> rawDataList){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        File f = new File("measurements"+dtf.format(now)+".csv");
        try{
            if(!f.createNewFile()){
                System.out.println(f.getName()+" already exists, appending the data.");
            }
            PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
            for(RawData rd : rawDataList){
                if(rd.toString() != null){
                    output.println(rd.toString());
                }
            }
            output.flush();
            output.close();
            System.out.println("Done writing data to file.");
            writeMeasurementsToDB(f, dtf.format(now));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    private void writeMeasurementsToDB(File file, String creationTime) {
        System.out.println("Inserting measurements into database.");
        connect();
        try {
            long start_time = System.currentTimeMillis();
            try {
                String createTable = "CREATE TABLE imports.measurements" + creationTime;
                createTable = createTable + " (sensorId TEXT, latitude TEXT, longitude TEXT, stamp TEXT, type TEXT, value TEXT);";
                conn.createStatement().executeUpdate(createTable);
            }
            catch(Exception e){
                System.out.println(e.getMessage());
                return;
            }
            String sqlQuery = "INSERT INTO imports.measurements"+creationTime+" (sensorId, latitude, longitude, stamp, type, value) values (?,?,?,?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
            conn.setAutoCommit(false);
            BufferedReader input = new BufferedReader(new FileReader(file));
            String s = input.readLine();
            long k = 0;
            while(s != null) {
                String[] l = s.split(",");
                String sensorId = l[0];
                String latitude = l[1];
                String longitude = l[2];
                String stamp = l[3];
                String type = l[4];
                String value = l[5];
                s = input.readLine();

                pstmt.setString(1,sensorId);
                pstmt.setString(2,latitude);
                pstmt.setString(3,longitude);
                pstmt.setString(4,stamp);
                pstmt.setString(5,type);
                pstmt.setString(6,value);
                pstmt.addBatch();
                ++k;
                if(k%5000 == 0){
                    System.out.println(String.format("Inserting 5000 rows, %d total.", k));
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
            System.out.println(String.format("Inserting %d rows, %d total.", k%5000, k));
            System.out.println("Normalizing measurements data.");
            Statement stmt;
            ResultSet rs;
            conn.setAutoCommit(false);
            sqlQuery = "SELECT air_pollution.normalizeMeasurementData('"+creationTime+"')";
            System.out.println(sqlQuery);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();
            pstmt.executeBatch();
            conn.commit();
            long end_time = System.currentTimeMillis();
            long hours = (end_time-start_time)/3600000;
            long mins = ((end_time-start_time)%3600000)/60000;
            long seconds = (((end_time-start_time)%3600000)%60000)/1000;
            long millis = (((end_time-start_time)%3600000)%60000)%1000;
            System.out.println(String.format("Total time for the measurements data (inserting in database): %02d:%02d:%02d.%03d", hours, mins, seconds, millis));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void runSensorData(){
        String username = "", password = "";
        if(System.console() == null) {
            System.out.println("No console available!");
            Scanner input = new Scanner(System.in);
            System.out.println("Username for pulse.eco: ");
            username = input.nextLine();
            System.out.println("Password for pulse.eco: ");
            password = input.nextLine();
        }
        else {
            Console input = System.console();
            System.out.println("Username for pulse.eco: ");
            username = new String(input.readLine());
            System.out.println("Password for pulse.eco: ");
            password = new String(input.readPassword());
        }
        String plainCreds = username + ":" + password;
        byte[] plainCredsBytes = plainCreds.getBytes(StandardCharsets.UTF_8);
        byte[] base64CredsBytes = org.apache.tomcat.util.codec.binary.Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes, StandardCharsets.UTF_8);

        //Create HTTP Header and RestTemplate object
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Basic " + base64Creds);
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity requestEntity = new HttpEntity(headers);

        //Execute the request
        List<Sensor> sensorList = new ArrayList<Sensor>();


        for(String city : cities) {
            try {
                String url = String.format("https://%s.pulse.eco/rest/sensor", city);
                Sensor[] output = restTemplate.exchange(url,
                        HttpMethod.GET, requestEntity, Sensor[].class).getBody();
                if(output!= null)
                    sensorList.addAll(Arrays.asList(output));
            }
            catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
        writeSensorsToFile(sensorList);
    }

    private void writeSensorsToFile(List<Sensor> sensorList){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        File f = new File("sensors"+dtf.format(now)+".csv");
        try{
            if(!f.createNewFile()){
                System.out.println(f.getName()+" already exists, appending the data.");
            }
            PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
            for(Sensor s : sensorList){
                if(s.toString() != null){
                    output.println(s.toString());
                }
            }
            output.flush();
            output.close();
            System.out.println("Done writing sensor data to file.");
            writeSensorsToDB(f, dtf.format(now));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    private void writeSensorsToDB(File file, String creationTime) {
        System.out.println("Inserting sensors into database.");
        connect();
        try {
            long start_time = System.currentTimeMillis();
            try {
                String createTable = "CREATE TABLE imports.sensors" + creationTime;
                createTable = createTable + " (sensorId TEXT, latitude TEXT, longitude TEXT, comments TEXT, type TEXT, description TEXT, status TEXT);";
                conn.createStatement().executeUpdate(createTable);
            }
            catch(Exception e){
                System.out.println(e.getMessage());
                return;
            }
            String sqlQuery = "INSERT INTO imports.sensors"+creationTime+" (sensorId, latitude, longitude, comments, type, description, status) values (?,?,?,?,?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
            conn.setAutoCommit(false);
            BufferedReader input = new BufferedReader(new FileReader(file));
            String s = input.readLine();
            long k = 0;
            while(s != null) {
                String[] l = s.split(",");
                String sensorId = l[0];
                String latitude = l[1];
                String longitude = l[2];
                String comments = l[3];
                String type = l[4];
                String description = l[5];
                String status = l[6];
                s = input.readLine();

                pstmt.setString(1,sensorId);
                pstmt.setString(2,latitude);
                pstmt.setString(3,longitude);
                pstmt.setString(4,comments);
                pstmt.setString(5,type);
                pstmt.setString(6,description);
                pstmt.setString(7,status);
                pstmt.addBatch();
                ++k;
                if(k%5000 == 0){
                    System.out.println(String.format("Inserting 5000 rows, %d total.", k));
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
            System.out.println(String.format("Inserting %d rows, %d total.", k%5000, k));
            pstmt.executeBatch();
            conn.commit();
            System.out.println("Normalizing sensor data.");
            Statement stmt;
            ResultSet rs;
            conn.setAutoCommit(false);
            /*sqlQuery = "SELECT air_pollution.normalizeSensorData('"+creationTime+"')";
            System.out.println(sqlQuery);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlQuery);
            conn.commit();*/
            long end_time = System.currentTimeMillis();
            long hours = (end_time-start_time)/3600000;
            long mins = ((end_time-start_time)%3600000)/60000;
            long seconds = (((end_time-start_time)%3600000)%60000)/1000;
            long millis = (((end_time-start_time)%3600000)%60000)%1000;
            System.out.println(String.format("Total time for the sensor data (inserting in database): %02d:%02d:%02d.%03d", hours, mins, seconds, millis));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}