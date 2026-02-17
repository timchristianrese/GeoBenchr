package benchmark_client;

public class BenchmarkMarkerLogger {

    public final String runID;
    public BenchmarkMarkerLogger(String runID) {
        this.runID = runID;
    }

    public void logText(String text) {
        // open benchmark_usage_runID.txt and append text
        try (java.io.FileWriter fw = new java.io.FileWriter("benchmark_usage.log", true);
             java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
             java.io.PrintWriter out = new java.io.PrintWriter(bw)) {
            out.println(text);
        } catch (java.io.IOException e) {
            System.err.println("Error writing benchmark marker log: " + e.getMessage());   
            
        }
    }
    public void moveLogFile() {
        //change benchmark_usage.txt to benchmark_usage_runID.txt
        java.io.File oldFile = new java.io.File("benchmark_usage.log");
        java.io.File newFile = new java.io.File("benchmark_usage_" + runID + ".log");
        if (oldFile.exists()) {
            boolean success = oldFile.renameTo(newFile);
            if (!success) {
                System.err.println("Error renaming benchmark marker log file.");  
            }
        }
    }
}   
