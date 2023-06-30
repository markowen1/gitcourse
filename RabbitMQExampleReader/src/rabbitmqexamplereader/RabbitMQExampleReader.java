/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rabbitmqexamplereader;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import com.rabbitmq.client.DeliverCallback;

// JSON parsing
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author me
 */
public class RabbitMQExampleReader {

    public static Channel channel;
    public static long lStartTime, lEndTime;
    public static long txCounter = 0, txMax = 1;
    public static long txReceived = 0;
    public static boolean ok_to_continue = true;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {

        String QUEUE_NAME = "m1";

        if (args.length > 0) {
            txMax = Long.parseLong(args[0]);
            System.out.println("Setting max tx count to " + txMax);
        }

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setHost("192.168.200.11");
            factory.setUsername("connection1");
            factory.setPassword("connection1");
            System.out.println("Set user name and password, and host.");

            lStartTime = System.currentTimeMillis();
            Connection connection = factory.newConnection();
            lEndTime = System.currentTimeMillis();

            System.out.println("created connection, now creating channel.");
            lStartTime = System.currentTimeMillis();
            channel = connection.createChannel();
            lEndTime = System.currentTimeMillis();
            System.out.println("channel created: " + (lEndTime - lStartTime) + "ms");
            //   channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            // String message = "";
            lStartTime = System.currentTimeMillis();

            //  This is the async callback (lambda)and not in-line execution.
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // System.out.println("Starting to receive a message in the callback.");
                if (ok_to_continue && channel.isOpen()) {
                    String message = new String(delivery.getBody(), "UTF-8");  // This string should be JSON.

                    JSONParser jparser = new JSONParser();
                    JSONObject jobject;
                    String msg = "";
                    long mtxcnt = 0;

                    try {
                        jobject = (JSONObject) jparser.parse(message);

                        msg = (String) jobject.getOrDefault("msg", "");
                        mtxcnt = (Long) jobject.getOrDefault("txcnt", delivery);

                    } catch (ParseException ex) {
                        Logger.getLogger(RabbitMQExampleReader.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    //RabbitMQExampleReader.lEndTime = System.currentTimeMillis();

                    txReceived++;
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);  // meo:  Requiring explicit ACK in the lambda (callback) for each message.

                    // System.out.println(" [x] Received msg: " + txReceived + " " + message + " in " + (lEndTime - lStartTime) + "ms");
                    System.out.println(" [x] Received json: " + txReceived + " " + message + " Msg is " + msg + " TxCnt = " + mtxcnt);
                    //RabbitMQExampleReader.lStartTime = System.currentTimeMillis();          

                    if (txReceived >= txMax) {
                        ok_to_continue = false;

                        // Try to shutdown and exit gracefully.                      
                        if (channel.isOpen()) {
                            try {
                                System.out.println("Closing channel.");
                                channel.close();

                            } catch (Exception ex) {
                                System.out.println("Conflict in graceful shutdown of the channel.");
                                // Logger.getLogger(RabbitMQExampleReader.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }

                        connection.close();
                    }
                }

            };

            System.out.println("Enabling the message receiver callback.");

            //  auto-ack was true, now False
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
                System.out.println("What goes here?");
            });
            System.out.println("Callback is now enabled.");

            lStartTime = System.currentTimeMillis();
            txCounter = txReceived;

            while (ok_to_continue && channel.isOpen()) {
                System.out.println("Waiting... OK: " + ok_to_continue + " Channel open: " + channel.isOpen() + " Count: " + txCounter + " Recv: " + txReceived);
                Thread.sleep(3000);
                lEndTime = System.currentTimeMillis();
                if (txCounter == txReceived) {      // Have NOT received anything in the last time period....
                    ok_to_continue = false;
                    System.out.println("Timing out and stopping.");
                } else {
                    txCounter = txReceived;  // Set them equal after the comparison, to see if they change.
                }

                System.out.println("Timer expired " + " in " + (lEndTime - lStartTime) + "ms  OK: " + ok_to_continue);
                lStartTime = System.currentTimeMillis();
            }
            if (channel.isOpen()) {
                System.out.println("Closing channel.");
                channel.close();
            }
            if (connection.isOpen()) {
                System.out.println("Closing connection.");
                connection.close();
            }
        } catch (IOException | TimeoutException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
        System.out.println("Finished.");

    }

}
