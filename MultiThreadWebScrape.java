package multoithreadwebscrape;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Raphael Abrahamson
 */

public class MultiThreadedWebScrape {

    Set<String> indexHyperlinksRef;
    Queue<String> hyperQ;
    boolean isConnected;
    Set<String> IndexEmailRef;

    String connect;

    static final int POOL_SIZE = 100;
    static final int THREAD_LIMIT = 100;
    static boolean SUCCESS = false;
    static final String LOCK ="LockObject";
    static final String EMAIL_LOCK ="EmailLockObject";

    public MultiThreadedWebScrape() {
        indexHyperlinksRef = Collections.synchronizedSet(new HashSet<>());
        hyperQ = new LinkedList<>();
        IndexEmailRef = Collections.synchronizedSet(new HashSet<>());

    }

    public static void main(String[] arguments) {
        MultiThreadedWebScrape mws = new MultiThreadedWebScrape();
        mws.hyperQ.add("http://www.touro.edu");
        ThreadScrapeNCrawl disposableThread = new ThreadScrapeNCrawl(mws, "Thread 0");
        disposableThread.seedRun();
        try {
            disposableThread.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(MultiThreadedWebScrape.class.getName()).log(Level.SEVERE, null, ex);
        }
       
        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
        Runnable tnsr;

        for (int i = 1; i < THREAD_LIMIT; i++) {
            tnsr = new ThreadScrapeNCrawl(mws, "Thread " + i);
            executor.execute(tnsr);  // asynchronously launch a worker thread     
            System.out.println("IN THREAD CREATION");
        }

        // pause the main thread;,
        while (!MultiThreadedWebScrape.SUCCESS) {
        }
        
        System.out.println("SHUTDOWN");
            executor.shutdown(); // clean up - graceful shutdown 
        
        while (!executor.isTerminated()) { // wait until there's no more active worker threads ... 
            System.out.println("STILL SHUTTING DOWN");
        }
        System.out.println("FINISHED");
    }

    public void connectToAndUpdateDB(Set<String> addSet) {
        int rowsAffected = 0;
        try {
            isConnected = true;
            Connection connection;
            String currentConnection;

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("Driver Successfully Loaded!");
            String driver = "jdbc:sqlserver:";
            String url = "//lcmdb.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com:";
            String port = "1433";
            String username = "DS2";
            String password = "Touro123";
            String database = "DS2";
            currentConnection
                    = driver
                    + url
                    + port
                    + " ;databaseName=" + database
                    + ";user=" + username
                    + ";password=" + password + ";";
            try {
                connection = DriverManager.getConnection(currentConnection);
                for (String update : addSet) {
                    update=update.trim();
                    System.out.println(update);
                    PreparedStatement state
                            = connection.prepareStatement("USE DS2\n"
                                    + "INSERT INTO WebScrappedEmails (Email_Address) "
                                    + "Values('" + update + "')");
                    rowsAffected += state.executeUpdate();
                }
            } catch (SQLException ex) {
                Logger.getLogger(MultiThreadedWebScrape.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Connected to Database!");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MultiThreadedWebScrape.class.getName()).log(Level.SEVERE, null, ex);
        }
        isConnected = false;
        System.out.println(rowsAffected + " rows affected");
    }
}

class ThreadScrapeNCrawl extends Thread {

    String message;
    MultiThreadedWebScrape mtws;
    int rounds = 0;
    Set<String> localEmails;
    Set<String> localHLinkSet;

    public ThreadScrapeNCrawl(MultiThreadedWebScrape mws, String message) {
        this.message = message;
        localEmails = Collections.synchronizedSet(new HashSet());
        localHLinkSet = Collections.synchronizedSet(new HashSet());
        mtws = mws;

    }

    public void seedRun() {
        
        while (!(mtws.hyperQ.size()>=150)) {
            Document doc;
            try {
                String url = mtws.hyperQ.poll();
                doc = Jsoup.connect(url).get();
                getEmails(doc);
                getHyperlinks(doc);
                manageHyperlinks();
            } catch (IOException ex) {
                Logger.getLogger(ThreadScrapeNCrawl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public void run() {
        while (MultiThreadedWebScrape.SUCCESS==false) {
            String url;
            
            url = mtws.hyperQ.poll();
            
            try {
                rounds++;
                Document doc = Jsoup.connect(url).timeout(3000).get();
                getEmails(doc);
                getHyperlinks(doc);
                manageHyperlinks();

            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            System.out.println("local email batch is: "+this.localEmails.size());
            System.out.println("global email batch is: "+mtws.IndexEmailRef.size());

            synchronized (MultiThreadedWebScrape.EMAIL_LOCK) {
                if (mtws.IndexEmailRef.size() > 1000) {
                    updateDB();
                    MultiThreadedWebScrape.SUCCESS = true;
                    mtws.IndexEmailRef.clear();
                    System.out.println("the global email batch is now: "+mtws.IndexEmailRef.size());
                    System.exit(0);
                }
            }
        }
    }

    private void getEmails(Document doc) {
        Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
        Elements eee = doc.getAllElements();
        eee.stream().map((ede) -> p.matcher(ede.text())).forEach((Matcher matcher) -> {
            while (matcher.find()) {
                String tempString = matcher.group();
                if (tempString.endsWith(".")) {
                    tempString = tempString.substring(0, tempString.length() - 1);
                }
                localEmails.add(tempString);
            }
        });
            if (localEmails.size()>= 50) {
                synchronized ("email key") {
                mtws.IndexEmailRef.addAll(localEmails);
            }
        }

    }

    private void updateDB() {
        mtws.connectToAndUpdateDB(mtws.IndexEmailRef);
    }

    private void getHyperlinks(Document doc) {
        Elements link_elmnt = doc.select("a[href]");
        link_elmnt.stream().forEach((Element e) -> {
            String urlToAdd = e.attr("abs:href");
            if (urlToAdd.matches("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")){
                localHLinkSet.add(urlToAdd);
            }
        }
        );
    }

    //synchronize over these ones
    private void manageHyperlinks() {
        synchronized (MultiThreadedWebScrape.LOCK) {
            localHLinkSet.removeAll(mtws.indexHyperlinksRef);
            Set<String> tempSet = Collections.synchronizedSet(new HashSet<String>(localHLinkSet));
            mtws.indexHyperlinksRef.addAll(tempSet);
            mtws.hyperQ.addAll(tempSet);
            localHLinkSet.clear();
        }
    }
}
