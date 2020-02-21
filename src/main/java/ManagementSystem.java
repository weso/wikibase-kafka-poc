import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import core.kafka.KConsumerListener;
import core.kafka.KProducerListener;
import core.kafka.KafkaHandler;
import core.utils.CSVHandler;
import core.utils.LoggerHandler;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

public class ManagementSystem {


    static KafkaHandler kh = KafkaHandler.getInstance(null);
    final static LoggerHandler loggerHandler = LoggerHandler.getInstance();
    final static Logger logger = LogManager.getLogger(ManagementSystem.class);


    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        //getJsonDataFromCSVResourcesFile("academic_institution.csv");
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
        // hadleSendAcademicData();


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        boolean isWorking = true;
        while (isWorking) {

            printOptions();
            String input = br.readLine();
            switch (input) {
                case "0":
                    isWorking = false;
                    System.out.println("Leaving the application");
                    break;
                case "1":
                    handleSendAcademicData();
                    isWorking = false;
                    break;
                case "2":
                    handleSendTitleDegree();
                    isWorking = false;
                    break;
                case "3":
                    handleResearchers();
                    isWorking = false;
                    break;
                default:
                    System.out.println("\n\n\nOnly the options shown are valid, please select one");
                    printOptions();
                    break;

            }


            System.out.println("...Ended...");
            System.exit(0);
        }

        //hadleSendAcademicData();
    }

    private static void printOptions() {
        System.out.println("***************************************************");
        System.out.println("Select entity to crete/update or type 0 to exit:");
        System.out.println("\t[1]  ACADEMIC INSTITUTIONS");
        System.out.println("\t[2]  TITLE DEGREE");
        System.out.println("\t[3]  RESEARCHERS");
        System.out.println("***************************************************");
    }

    private static void handleSendAcademicData() {

        logger.info("*** Starting send Academic Data by Kafka to Processor ***");

        JsonArray jAcademicData = getJsonDataFromCSVResourcesFile("academic_institution.csv");
        Semaphore semaphore = new Semaphore(1);
        Map<String,String> sended = new HashMap<>();
        Map<String,String> pending = new HashMap<>();
        for (int i = 0; i < jAcademicData.size() ; i++) {
            sended.put(String.valueOf(i),jAcademicData.get(i).toString());
            pending.put(String.valueOf(i),jAcademicData.get(i).toString());
        }

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {
                logger.info("\t--- Sending DONE Academic Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }

            @Override
            public void onMessageFail(String id, Exception e) {
                logger.error("\t--- Sending ERROR Academic Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }
        };

        logger.info("\t--- Processing ("+jAcademicData.size()+") instances");
        for (int i = 0; i < jAcademicData.size() ; i++) {
            JsonObject jData = new JsonObject();
            jData.addProperty("id",i);
            jData.addProperty("type","academic_institution");
            jData.add("data",jAcademicData.get(i));
            kh.addProducer(Integer.toString(i), kProducerListener);
            kh.sendMessage(Integer.toString(i),"data-queue",jData.toString());
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kh.addConsumer("academic_institution_success", "success-queue", Collections.singletonList("success-queue"), true, new KConsumerListener() {
            @Override
            public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                System.out.println("RECEIVED SUCCESSSSSS : Id["+id+"], Topic["+topic+"], Key["+key+"], Partition["+partition+"], offset["+offset+"], message["+message+"]");
                JsonObject jMessage = new JsonParser().parse(message).getAsJsonObject();
                String idMessage = jMessage.get("id").getAsString();
                String type = jMessage.get("type").getAsString();
                System.out.println("ID: "+id+"\tType:"+type);
                if (type.equals("academic_institution")) {
                    pending.remove(idMessage);
                    System.out.println("Pending Size: " + pending.size());
                    if (pending.size() == 0) {
                        semaphore.release();
                    }
                }
            }
        });

        try {
            semaphore.acquire();
            kh.removeConsumer("academic_institution_success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void handleSendTitleDegree() {

        logger.info("*** Starting send Title Degree Data by Kafka to Processor ***");

        JsonArray jAcademicData = getJsonDataFromCSVResourcesFile("title_degree.csv");
        Semaphore semaphore = new Semaphore(1);
        Map<String,String> sended = new HashMap<>();
        Map<String,String> pending = new HashMap<>();
        for (int i = 0; i < jAcademicData.size() ; i++) {
            sended.put(String.valueOf(i),jAcademicData.get(i).toString());
            pending.put(String.valueOf(i),jAcademicData.get(i).toString());
        }

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {
                logger.info("\t--- Sending DONE Title Degree Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }

            @Override
            public void onMessageFail(String id, Exception e) {
                logger.error("\t--- Sending ERROR Title Degree Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }
        };

        logger.info("\t--- Processing ("+jAcademicData.size()+") instances");
        for (int i = 0; i < jAcademicData.size() ; i++) {
            JsonObject jData = new JsonObject();
            jData.addProperty("id",i);
            jData.addProperty("type","title_degree");
            jData.add("data",jAcademicData.get(i));
            kh.addProducer(Integer.toString(i), kProducerListener);
            kh.sendMessage(Integer.toString(i),"data-queue",jData.toString());
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kh.addConsumer("title_degree_success", "success-queue", Collections.singletonList("success-queue"), true, new KConsumerListener() {
            @Override
            public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                System.out.println("RECEIVED SUCCESSSSSS : Id["+id+"], Topic["+topic+"], Key["+key+"], Partition["+partition+"], offset["+offset+"], message["+message+"]");
                JsonObject jMessage = new JsonParser().parse(message).getAsJsonObject();
                String idMessage = jMessage.get("id").getAsString();
                String type = jMessage.get("type").getAsString();
                System.out.println("ID: "+id+"\tType:"+type);
                if (type.equals("title_degree")) {
                    pending.remove(idMessage);
                    System.out.println("Pending Size: " + pending.size());
                    if (pending.size() == 0) {
                        semaphore.release();
                    }
                }
            }
        });

        try {
            semaphore.acquire();
            kh.removeConsumer("title_degree_success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void handleResearchers() {

        logger.info("*** Starting send Title Degree Data by Kafka to Processor ***");

        JsonArray jAcademicData = getJsonDataFromCSVResourcesFile("researcher.csv");
        Semaphore semaphore = new Semaphore(1);
        Map<String,String> sended = new HashMap<>();
        Map<String,String> pending = new HashMap<>();
        for (int i = 0; i < jAcademicData.size() ; i++) {
            sended.put(String.valueOf(i),jAcademicData.get(i).toString());
            pending.put(String.valueOf(i),jAcademicData.get(i).toString());
        }

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {
                logger.info("\t--- Sending DONE Researcher Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }

            @Override
            public void onMessageFail(String id, Exception e) {
                logger.error("\t--- Sending ERROR Researchere Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }
        };

        logger.info("\t--- Processing ("+jAcademicData.size()+") instances");
        for (int i = 0; i < jAcademicData.size() ; i++) {
            JsonObject jData = new JsonObject();
            jData.addProperty("id",i);
            jData.addProperty("type","researcher");
            jData.add("data",jAcademicData.get(i));
            kh.addProducer(Integer.toString(i), kProducerListener);
            kh.sendMessage(Integer.toString(i),"data-queue",jData.toString());
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kh.addConsumer("researcher_success", "success-queue", Collections.singletonList("success-queue"), true, new KConsumerListener() {
            @Override
            public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                System.out.println("RECEIVED SUCCESSSSSS : Id["+id+"], Topic["+topic+"], Key["+key+"], Partition["+partition+"], offset["+offset+"], message["+message+"]");
                JsonObject jMessage = new JsonParser().parse(message).getAsJsonObject();
                String idMessage = jMessage.get("id").getAsString();
                String type = jMessage.get("type").getAsString();
                System.out.println("ID: "+id+"\tType:"+type);
                if (type.equals("researcher")) {
                    pending.remove(idMessage);
                    System.out.println("Pending Size: " + pending.size());
                    if (pending.size() == 0) {
                        semaphore.release();
                    }
                }
            }
        });

        try {
            semaphore.acquire();
            kh.removeConsumer("researcher_success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static JsonArray getJsonDataFromCSVResourcesFile(String name){
        try {
            URL res = Class.forName("ManagementSystem").getClassLoader().getResource(name);
            File file = Paths.get(res.toURI()).toFile();

            JsonArray jData = CSVHandler.parseCSV(file.getAbsolutePath());
            return jData;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

}
