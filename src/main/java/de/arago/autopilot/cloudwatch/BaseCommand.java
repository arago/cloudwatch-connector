package de.arago.autopilot.cloudwatch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.arago.autopilot.client.api.AutoPilot;
import de.arago.autopilot.client.api.AutoPilotAPIFactory;
import java.util.HashMap;
import java.util.Properties;

public class BaseCommand {

    static final int DEFAULT_TIMEOUT_SECS = 30;
    private static final String TOKEN_ENV_VAR = "_TOKEN";
    private static final String AP_VERSION_VAR = "AUTOPILOT_VERSION_STRING";

    protected String command() {
        return "<main class>";
    }

    protected void usage(String message, JCommander commandline) {
        if (message != null) {
            System.err.println("\n"+message+"\n");
        }
        StringBuilder sb = new StringBuilder();
        String version = System.getenv(AP_VERSION_VAR);
        if (version != null && !version.isEmpty()) {
            sb.append("CLI Version: ").append(version).append("\n\n");
        }
        commandline.setProgramName(command());

        commandline.usage(sb);
        // show current settings
        StringBuilder cs = new StringBuilder();
        // currently only one setting. do this better if more parameters will get stored
        Properties prop = ParametersHelper.loadProperties();
        uri = prop.getProperty("uri");
        if (uri != null) {
            cs.append("\t--url ").append(uri).append("\n");
        }
        if (cs.length() > 0) {
            sb.append("\nCurrently saved settings:\n");
            sb.append(cs);
        }
        System.err.println(sb.toString());
    }

    @Parameter(names = {"-h", "--help"}, description = "Provides helpful information about the commands usage")
    public boolean help = false;
    @Parameter(names = {"-u", "--url"}, description = "endpoint_url is a mandatory parameter that represents the URL of the aAE")
    public String uri = null;
    @Parameter(names = {"-t", "--timeout"}, description = "allowed connection timeout in seconds")
    public int time_out = DEFAULT_TIMEOUT_SECS;
    @Parameter(names = {"-T", "--token"}, description = "A valid access token. It is required if aAE is configured to use access tokens.")
    public String token = "";

    final public int run(String[] args) {
        JCommander commandline = new JCommander(this);
        try {
            commandline.parse(args);
            if (token.isEmpty()) {
                token = System.getenv(TOKEN_ENV_VAR);
                if (token == null) {
                    token = "";
                }
            }
        } catch (com.beust.jcommander.ParameterException ex) {
            usage(ex.getMessage(), commandline);
            return 1;
        }
        if (help) {
            usage(null, commandline);
            return 0;
        }
        if (uri != null) {
            ParametersHelper.saveProperties(uri);
        } else {
            Properties prop = ParametersHelper.loadProperties();
            uri = prop.getProperty("uri");
        }
        return runInnerLogic(commandline);
    }

    protected int runInnerLogic(JCommander commandline) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private final static HashMap<String,AutoPilotAPIFactory> pool = new HashMap<>();

    protected AutoPilotAPIFactory connect(String url) {
        AutoPilotAPIFactory factory = pool.get(url);
        if(factory==null) {
            Properties runtime_props = new Properties();
            if (time_out <= 0) {
                time_out = -1; // infinity
            } else {
                time_out *= 1000; // millisecs
            }
            runtime_props.setProperty("request-timeout", Integer.valueOf(time_out).toString());
            factory = AutoPilot.connect(url, runtime_props);
            pool.put(url, factory);
        }
        return factory;
    }

}
