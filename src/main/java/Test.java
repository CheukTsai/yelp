import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;

public class Test {
    public static void main(String[] args) {
        // Path to your JSON file
        String filePath = "y_s_u.json";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;

            // Read lines from the file and print them
            while ((line = br.readLine()) != null) {
                Object obj = new JSONParser().parse(line);
                JSONObject jsonObject = (JSONObject) obj;

                // Accessing the parsed data
                String review_id = (String) jsonObject.get("review_id");
                String user_id = (String) jsonObject.get("user_id");

                // Printing the parsed data
                System.out.println("Review: " + review_id);
                System.out.println("User: " + user_id);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
