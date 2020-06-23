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
import java.sql.ResultSet;
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
        DataConnection app = new DataConnection("u171204", "sdhgksd");
        //sim data
        //app.writeSimDataToDB();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        //Pulse.eco
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
                String username;
                String password = "";
                System.out.println("Username: ");
                username = input.readLine();
                System.out.println("Password: ");
                password = new String(input.readPassword());
                app.setPasswordDatabase(password);
                app.setUserDatabase(username);

            }
        }
        System.out.println("Data from 'pulse.eco' ? [y/n]");
        if(System.console() == null) {
            System.out.println("No console available!");
            @SuppressWarnings("resource")
            Scanner input = new Scanner(System.in);
            while(true) {
                String q;
                q = input.nextLine();
                if(q.toLowerCase().equals("y")){
                    System.out.println("Running measurements import.");
                    //app.runSensorData();
                    app.runMeasurementsData();
                    break;
                }
                else if(q.toLowerCase().equals("n")){
                    break;
                }
                else{
                    System.out.println("Invalid input. Data from 'pulse.eco' ? [y/n]");
                }
            }
        }
        else {
            Console input = System.console();
            while(true) {
                String q;
                q = input.readLine();
                if(q.toLowerCase().equals("y")){
                    System.out.println("Running measurements import.");
                    app.runSensorData();
                    //app.runMeasurementsData();
                    break;
                }
                else if(q.toLowerCase().equals("n")){
                    break;
                }
                else{
                    System.out.println("Invalid input. Data from 'pulse.eco' ? [y/n]");
                }
            }
        }
        System.out.println("Import data from sim file? [y/n]");
        if(System.console() == null) {
            System.out.println("No console available!");
            @SuppressWarnings("resource")
            Scanner input = new Scanner(System.in);
            while(true) {
                String q;
                q = input.nextLine();
                if(q.toLowerCase().equals("y")){
                    System.out.println("Running sim_data import.");
                    app.writeSimDataToDB();
                    break;
                }
                else if(q.toLowerCase().equals("n")){
                    break;
                }
                else{
                    System.out.println("Invalid input. Import data from sim file? [y/n]");
                }
            }
        }
        else {
            Console input = System.console();
            while(true) {
                String q;
                q = input.readLine();
                if(q.toLowerCase().equals("y")){
                    System.out.println("Running sim_data import.");
                    app.writeSimDataToDB();
                    break;
                }
                else if(q.toLowerCase().equals("n")){
                    break;
                }
                else{
                    System.out.println("Invalid input. Import data from sim file? [y/n]");
                }
            }
        }
    }
}
//https://skopje.pulse.eco/restapi