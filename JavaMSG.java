import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.*;
import java.util.stream.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JavaMSG implements Runnable {
    private File file;
    private int runEvery;
    private long lastPosition = 0;
    private boolean run = true;
    private int cnt = 1;
    ArrayList<Sale> lSale;
    List<Sale> mSale = new ArrayList<Sale>();

    public JavaMSG(String inputFile, int interval) {
        file = new File(inputFile);
        this.runEvery = interval;
    }

    public void stop() {
        run = false;
    }

    public void run() {
        try {

            while(run) {
                

                Thread.sleep(runEvery);
                long fileLength = file.length();
                if(fileLength > lastPosition) { 
                    RandomAccessFile fh = new RandomAccessFile(file, "r");
                    fh.seek(lastPosition);
	
					String sale = fh.readLine();
					String[] salesRecord = sale.split(";");
					String productType = salesRecord[0]; 
					double value = Double.parseDouble(salesRecord[1]); 
                    
                    //DISPLAY CURRENT RECORD
                    System.out.println(productType + " " + String.valueOf(value) + " " + String.valueOf(cnt));

                    //ADD TO LIST FOR GROUPING
                    mSale.add(new Sale(productType, value));

                    //DISPLAY EVERY 10 RECORDS
                    if (cnt%10==0)
                    {
                       //DO THE GROUPING
                        
                        Map<String,DoubleSummaryStatistics> result = mSale.stream()
                        .collect(Collectors.groupingBy(Sale::getProductType, Collectors.summarizingDouble(Sale::getValue)));
                    
                        //DISPLAY GROUPING
                        System.out.println(result + "\r\n");
                        
                       
                       
                        System.out.println("MESSAGE FOR  " + cnt + " " + productType + " " + String.valueOf(value) + " " + String.valueOf(cnt));

                    }
                    if (cnt==50)
                    {
                       
                        //EXIT IF 50
                        System.out.println("THE PROCCESS WILL QUIT. MAX MESSAGES REACHED (50)");
                        System.exit(1); 
                    }
                    cnt++;
					
                    lastPosition = fh.getFilePointer();
                    fh.close();
                }
            }
        }
        catch(Exception e) {
            System.out.println(e.toString());
            stop();
        }
    }

    public static void main(String argv[]) {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        //ACCEPT A ; DELIMITED FILE
        JavaMSG javamsg = new JavaMSG("sale.txt", 1);
        executor.execute(javamsg);

      
    } 
	
	class Sale {

		String productType;
		double value;
		
		Sale(String productType, double value) {

			this.productType = productType;
			this.value = value;
			
        }
        public String getProductType() {
            return productType;
        }
    
        public void setProductType(String productType) {
            this.productType = productType;
        }
        public Double getValue() {
            return value;
        }
    
        public void setValue(Double value) {
            this.value = value;
        }
	}
}