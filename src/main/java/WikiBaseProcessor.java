import org.apache.log4j.Level;

public class WikiBaseProcessor {
    public static void main(String[] args) {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);

        System.out.println("Hola 2");
    }
}
