/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rabbitmqexamplewriter;

/**
 *
 * @author me
 */
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;

public class RabbitMQExampleWriter {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        // Default values for parameters.
        String queueName = "m1";   // was mq1 on the VM version.
        long txMax = 1;
        // VM String hostName = "192.168.200.78";
        String hostName = "192.168.200.11";    // Default host to Ace.

        // This ID and password must be established on the RMQ server, and
        //  must have access to this virtual host, in the admin page
        //  at http://RabbitMQserver:15672
        String userName = "connection1";
        String userPassword = "connection1";

        long lStartTime = 0, lEndTime = 0;
        long txCounter = 0;
        long txInterval = 0;
        long nextTxTime = 0;
        long sleepTime = 0;
        Boolean verboseMode = false;

        CommandLineParser parser = new DefaultParser();
        Options programOptions = new Options();
        Boolean OKtoRun = true;

        System.out.println(RabbitMQExampleWriter.class.getName() + ": Starting.");

        Option myOption = Option.builder("m")
                .longOpt("maxTx")
                .desc("maximum tx count to read.")
                .hasArg()
                .argName("maxTx")
                .build();
        programOptions.addOption(myOption);
        myOption = Option.builder("v")
                .longOpt("verbose")
                .desc("Verbose mode.")
                .build();
        programOptions.addOption(myOption);
        myOption = Option.builder("h")
                .longOpt("host")
                .desc("Host name or IP.")
                .hasArg()
                .argName("host")
                .build();
        programOptions.addOption(myOption);
        myOption = Option.builder("q")
                .longOpt("queueName")
                .desc("Queue Name.")
                .hasArg()
                .argName("queueName")
                .build();
        programOptions.addOption(myOption);
        myOption = Option.builder("i")
                .longOpt("msgInterval")
                .desc("inter-message interval.")
                .hasArg()
                .argName("msgInterval")
                .build();
        programOptions.addOption(myOption);

        CommandLine cmd;
        try {
            cmd = parser.parse(programOptions, args);
            if (cmd.hasOption("m")) {
                System.out.println("Max TX Count set to " + cmd.getOptionValue("m"));
                txMax = Long.parseLong(cmd.getOptionValue("m"));
            } else {
                System.out.println("Max TX Count defaults to " + txMax);
            }
            if (cmd.hasOption("h")) {
                System.out.println("MQ Host set to " + cmd.getOptionValue("h"));
                hostName = cmd.getOptionValue("h");
            } else {
                System.out.println("MQ Host defaults to " + hostName);
            }
            if (cmd.hasOption("q")) {
                System.out.println("MQ Queue Name set to " + cmd.getOptionValue("q"));
                queueName = cmd.getOptionValue("q");
            } else {
                System.out.println("MQ Queue Name defaults to " + queueName);
            }
            if (cmd.hasOption("i")) {
                System.out.println("Inter-message interval set to " + cmd.getOptionValue("i"));
                txInterval = Long.parseLong(cmd.getOptionValue("i"));
            } else {
                System.out.println("Inter-message interval defaults to " + txInterval);
            }
            if (cmd.hasOption("v")) {
                System.out.println("Verbose mode.");
                verboseMode = true;
            }

        } catch (ParseException ex) {
            System.out.println("Parameter parsing exception: " + ex.getMessage());
            OKtoRun = false;
        }

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setHost(hostName);
            factory.setUsername(userName);
            factory.setPassword(userPassword);
            System.out.println("Set user name and password, and host.");

                        System.out.println("now creating Connection.");
            lStartTime = System.currentTimeMillis();
            Connection connection = factory.newConnection();
            lEndTime = System.currentTimeMillis();

            System.out.println("created connection, now creating channel.");
            lStartTime = System.currentTimeMillis();
            Channel channel = connection.createChannel();
            lEndTime = System.currentTimeMillis();
            System.out.println("channel created: " + (lEndTime - lStartTime) + "ms, now sending messages.");
            //channel.queueDeclare(QUEUE_NAME, true, true, false, null);

            lStartTime = System.currentTimeMillis();
            nextTxTime = lStartTime;

            for (txCounter = 0; txCounter < txMax; txCounter++) {
                //System.out.println("Setting message");
                nextTxTime = nextTxTime + txInterval;
                JSONObject payloadObject = new JSONObject();
                payloadObject.put("msg", "Hello World!");
                payloadObject.put("txcnt", txCounter);
                payloadObject.put("ts", System.currentTimeMillis());

                // String message = "Hello World! " + txCounter + ": " + System.currentTimeMillis();
                String message = payloadObject.toJSONString();

                // System.out.print(message);
                channel.basicPublish("",
                        queueName,
                        MessageProperties.PERSISTENT_TEXT_PLAIN, // Keeps msgs thru reboot
                        message.getBytes("UTF-8"));
                lEndTime = System.currentTimeMillis();

                // Sleep until it's time for the next tx.
                sleepTime = nextTxTime - System.currentTimeMillis();

                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ex) {
                        //Logger.getLogger(RabbitMQExampleWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
        System.out.println(" [x] Sent '" + txCounter + "' transactions in " + (lEndTime - lStartTime) + "ms");
        System.out.println("Finished.");
    }
}
