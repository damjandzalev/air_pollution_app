package air_pollution_app.air_pollution;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

@SpringBootApplication
public class AirPollutionApplication {

    public static void main(String[] args) throws SQLException {
        //database login
        DataConnection app = new DataConnection("", "");
        //sim data
        //app.writeSimDataToDB();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        //Pulse.eco
        //app.runMeasurementsData();(writes read data to file "measurementsyyyyMMdd.csv")
        if(System.console() == null) {
            System.out.println("No console available!");
            @SuppressWarnings("resource")
            Scanner input = new Scanner(System.in);
            while(!app.connect()) {
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
            while(!app.connect()) {
                String username, password;
                System.out.println("Username for database: ");
                username = new String(input.readLine());
                System.out.println("Password for database: : ");
                password = new String(input.readPassword());
                app.setPasswordDatabase(password);
                app.setUserDatabase(username);
            }
        }
        app.runSensorData();
        app.runMeasurementsData();
        app.writeSimDataToDB();
        /*String username = "", password = "";
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
        List<Sensor> sensorsList = new ArrayList<Sensor>();

        String[] cities = {"bitola", "gostivar", "kichevo", "kumanovo", "novoselo", "ohrid", "shtip", "skopje", "strumica", "tetovo"};
        for(String city : cities) {
            try {
                String url = String.format("https://%s.pulse.eco/rest/sensor", city);
                Sensor[] output = restTemplate.exchange(url,
                        HttpMethod.GET, requestEntity, Sensor[].class).getBody();
                if(output!= null)
                    sensorsList.addAll(Arrays.asList(output));
            }
            catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
        for(Sensor s : sensorsList){
            System.out.println(s.toString());
        }*/
    }
}
//https://skopje.pulse.eco/restapi