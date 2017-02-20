package de.arago.autopilot.cloudwatch;

import java.io.*;
import java.util.Properties;

public class ParametersHelper {

    static String getPropertiesPath() {
        String path = System.getenv().get("HOME").toString();
        if(path!=null)
            path += File.separatorChar;
        else
            path = "";
        path += ".autopilot.cli.properties";
        return path;
    }

    static void saveProperties(String uri) {
        Properties p = new Properties();
        p.setProperty("uri", uri);
        try {
            File f = new File(getPropertiesPath());

            if (!f.exists()) f.createNewFile();

            if(f.canWrite()) {
                FileOutputStream out = new FileOutputStream(f);
                p.store(out, "parameters for SOAP access to arago Automatisierungs Engine");
                out.close();
            }
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    static Properties loadProperties() {
        Properties p = new Properties();
        try {
            File f = new File(getPropertiesPath());
            if(f.canRead()) {
                FileInputStream in = new FileInputStream(f);
                p.load(in);
                in.close();
            }
        } catch(Exception e) {
            System.err.println(e.toString());
        }
        return p;
    }

    static String getUTF8TextFileContent(File textFileName) throws IOException {
        StringBuilder content = new StringBuilder();
        String line;
        String lineFeed = System.getProperty("line.separator");
        BufferedReader in;
        try (InputStreamReader r = new InputStreamReader(new FileInputStream(textFileName), "UTF8")) {
            in = new BufferedReader(r);
            while ((line = in.readLine()) != null) {
                content.append(line).append(lineFeed);
            }
        }
        in.close();
        return content.toString();
    }

}
