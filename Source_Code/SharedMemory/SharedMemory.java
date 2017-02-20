import java.io.*;
import java.util.*;

public class SharedMemory extends Thread {
		private String threadName;
		private Thread t;
		
		sortData sd;
		
		SharedMemory(String name, sortData sd)
		{
			threadName = name;
			this.sd = sd;
			System.out.println("Creating  "+threadName);
		}
		
		public void run()
		{
			
			System.out.println("Running  " +threadName);		
				//System.out.println("Inside for loop  " + threadName +" " + i);
				// Putting thread to sleep
				//Thread.sleep(50);
			synchronized(sd)
			{
				try {
					System.out.println("Time taken for reading 10 GB data");
					sd.sortDataFile("Input");
				}
				 catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			System.out.println(threadName + "Exiting");

			}
		public void start()
		{
			System.out.println("Starting" + threadName);
			if( t == null)
			{
				t = new Thread(this, threadName);
				t.start();
			}
		}
		public static void main(String[] args) throws IOException
		{
			sortData sdObj = new sortData();
			SharedMemory sm1 = new SharedMemory("Thread-1", sdObj );
			sm1.start();
			
			SharedMemory sm2 = new SharedMemory("Thread-2", sdObj);
			sm2.start();
			
			try{
				
			sm1.join();
			sm2.join();
				
			}catch(Exception e)
			{
				System.out.println("Interrupted");
			}
		}
}

class sortData
{
	public void sortDataFile(String filename) throws IOException
	{
		TreeMap tm = new TreeMap();

		//PrintWriter out = new PrintWriter("C:\\Users\\Sanjay Chaudhary\\workspace_1\\SM\\src\\Output.txt"); 
		FileInputStream fstream = new FileInputStream ("C:\\Users\\Sanjay Chaudhary\\workspace_1\\SM\\src\\Input");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        //FileOutputStream iout = new FileOutputStream("C:\\Users\\Sanjay Chaudhary\\workspace_1\\SM\\src\\Output.txt");
        File file = new File("C:\\Users\\Sanjay Chaudhary\\workspace_1\\SM\\src\\Output.txt");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        String inputLine;
       // bw.write("hi");
		double startTime = System.nanoTime();

        while( (inputLine = br.readLine()) != null)
        {
        	String key = inputLine.substring(0, 10);
        	String value = inputLine.substring(10, inputLine.length());
        	tm.put(key, value);
        }
		
        Set set = tm.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
           Map.Entry mentry = (Map.Entry)iterator.next();
           System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
           System.out.println(mentry.getValue());
           
           StringBuilder sb = new StringBuilder();
           sb.append(mentry.getKey());
           sb.append(mentry.getValue());
           bw.write(sb.toString());
           bw.newLine();
        }
        double endTime = System.nanoTime();
		double totalTime = (endTime - startTime)/1000000000;
		System.out.println("Time taken"+totalTime);
        bw.close();
	}



}
