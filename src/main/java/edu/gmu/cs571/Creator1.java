package edu.gmu.cs571;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.util.Hashtable;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.math3.distribution.NormalDistribution;

public class Creator1 {

    public static void run() throws IOException {
        
        /**
         * Options
         */
        // number of files = number of folders x files per folder
        //             100 = 2          x 50        // debug only
        //           1,000 = 10         x 100       // debug only
        //
        //          10,000 = 20         x 500
        //         100,000 = 100        x 1000
        //       1,000,000 = 100        x 10,000
        //      10,000,000 = 500        x 20,000
        //     100,000,000 = 1000       x 100,000
        //   1,000,000,000 = 5000       x 200,000 

        String startTime = LocalTime.now().toString();
        System.out.println("Start Time: " + startTime);
        
        int NUM_FOLDERS = 5000;
        int NUM_FILES_PER_FOLDER = 200000;
        long NUM_TOTAL_FILES = 1000000000;

        if (NUM_TOTAL_FILES != (NUM_FOLDERS * NUM_FILES_PER_FOLDER)) {
            System.out.println("Invalid parameters");
            return;
        }

        System.out.println("....................................................................................................[%] [Folder] [bytes] [msec] [Throughput MB/sec]");

        String outputLog = startTime + "," + Long.toString(System.nanoTime());    // start capturing the stats

        String STORAGE_PATH = "/home/sohail/12thdd/run1/";       // HDD or SSD

        Path dir = Paths.get(STORAGE_PATH);
        FileStore fs = Files.getFileStore(dir);
        long total_space = fs.getTotalSpace();
        long free_space_before = fs.getUsableSpace();       // 12 000 138 625 024
        long unallocated_space_before = fs.getUnallocatedSpace();

        Hashtable<String, Statistics> allStatitics = new Hashtable<>();     // Hashtable collects statistics for all tables

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Constants.THREADS);

        for (int i = 1; i <= NUM_FOLDERS; i++) {
            CreateFolderAndFiles createFolderAndFiles = new CreateFolderAndFiles(STORAGE_PATH + Integer.toString(i), NUM_FILES_PER_FOLDER, allStatitics);
            executor.execute(createFolderAndFiles);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Executor shutdown got interrupted");
        }

        /**
         * Process statics
         */
        System.out.println();
        System.out.println("Processing statistics...");
        System.out.println("Number of folders: " + NUM_FOLDERS);
        System.out.println("Number of files per folder: " + NUM_FILES_PER_FOLDER);
        System.out.println("Number of total files: " + NUM_TOTAL_FILES);
        System.out.println("Number of threads: " + Constants.THREADS);

        outputLog = outputLog + "," + NUM_FOLDERS + "," + NUM_FILES_PER_FOLDER + "," + NUM_TOTAL_FILES + "," + Constants.THREADS;

        long folders_create_time = 0;
        long file_write_cumulative_time = 0;
        long file_create_max_time = Long.MIN_VALUE;
        long file_create_min_time = Long.MAX_VALUE;
        long file_create_average_time = 0;
        long file_data = 0;

        long bucket_size[] = new long[Constants.BUCKETS_SIZE];
        long bucket_write[] = new long[Constants.BUCKETS_WRITE];

        /**
         * Build statistics
         */
        SortedMap<Integer, Float> throughput_list = new TreeMap<Integer, Float>();                    // capture throughput during the run
        int divider = NUM_FOLDERS / 20;     // 20 buckets to get the throughput trend

        Set<Entry<String, Statistics>> entrySet = allStatitics.entrySet();
        for (Entry<String, Statistics> statistics : entrySet) {                                         // iterate over all folder statistics
            
            folders_create_time = folders_create_time + statistics.getValue().folder_create_time;       // get total folder create time

            file_write_cumulative_time = file_write_cumulative_time + statistics.getValue().file_write_cumulative_time;     // get total file write time for the folder
            
            file_create_average_time = file_create_average_time + statistics.getValue().file_create_average_time;   // average

            file_data = file_data + statistics.getValue().file_data_per_folder;                         // get total data stored for this folder

            int folder_id = Integer.parseInt(statistics.getValue().folder_id);
            if (NUM_FOLDERS <= 20 || folder_id % divider == 0) {
                throughput_list.put(folder_id, (((float)statistics.getValue().file_data_per_folder/(float)statistics.getValue().file_write_cumulative_time)*1000));
                //System.out.println("Folder ID: " + folder_id);
            }
            
            if (file_create_min_time > statistics.getValue().file_create_min_time)                      // min
                file_create_min_time = statistics.getValue().file_create_min_time;
            if (file_create_max_time < statistics.getValue().file_create_max_time)                      // max
                file_create_max_time = statistics.getValue().file_create_max_time;

            for (int i = 0; i < Constants.BUCKETS_SIZE; i++) {                                           // fill the bucket with file sizes
                bucket_size[i] = bucket_size[i] + statistics.getValue().bucket_size[i];
            }
            for (int i = 0; i < Constants.BUCKETS_WRITE; i++) {                                          // fill the bucket with write statistics
                bucket_write[i] = bucket_write[i] + statistics.getValue().bucket_write[i];
            }

            /*
            System.out.println("Folder: " + statistics.getKey() + 
                    ", Folder Create: " + displayValue(statistics.getValue().folder_create_time) +
                    ", File Create-> Average: " + displayValue(statistics.getValue().file_create_average_time) +
                    ", Min: " + displayValue(statistics.getValue().file_create_min_time) +
                    ", Max: " + displayValue(statistics.getValue().file_create_max_time) +
                    ", Cumulative: " + displayValue(statistics.getValue().file_write_cumulative_time)
                    );
            */

        }

        System.out.println("File size histogram...");
        for (int i = 0; i < Constants.BUCKETS_SIZE; i++) {
            //System.out.println("File size @ bucket[" + i + "]: " + bucket_size[i]);
            System.out.println("File size range " + (Constants.fileSize_range[i][0]) + "~" + (Constants.fileSize_range[i][1]) + " (bytes): " + bucket_size[i]);
            outputLog = outputLog + "," + bucket_size[i];
        }
        System.out.println("File write speed histogram...");
        for (int i = 0; i < Constants.BUCKETS_WRITE; i++) {
            //System.out.println("File write @ bucket[" + i + "]: " + bucket_write[i]);
            System.out.println("File write range " + (Constants.write_range[i][0])/1000 + "~" + (Constants.write_range[i][1])/1000 + " (microsec): " + bucket_write[i]);
            outputLog = outputLog + "," + bucket_write[i];
        }
        
        String folder_throughput_log = "";
        System.out.println("Folder throughput histogram...");
        Set<Entry<Integer, Float>> throughput_set = throughput_list.entrySet();
        for (Entry<Integer, Float> throughput : throughput_set) {
            int folder_id = (Integer) throughput.getKey();
            float folder_throughput = (Float) throughput.getValue();
            folder_throughput_log = folder_throughput_log + "," + folder_throughput;
            System.out.println("Folder ID: " + folder_id + "  Throughput: " + folder_throughput);
        }

        System.out.println("Folders total create time: " + displayValue(folders_create_time));
        System.out.println("Files total write time: " + displayValue(file_write_cumulative_time));
        System.out.println("Files create min time: " + displayValue(file_create_min_time));
        System.out.println("Files create max time: " + displayValue(file_create_max_time));
        System.out.println("Files create average time: " + displayValue(file_write_cumulative_time/NUM_TOTAL_FILES));
        System.out.println("Files cumulative data: " + file_data);
        System.out.println("Files average size: " + file_data/NUM_TOTAL_FILES);
        System.out.println("Filesystem throughput (MB/sec): " + ((float)file_data/(float)file_write_cumulative_time)*1000);

        long free_space_after = fs.getUsableSpace();
        long unallocated_space_after = fs.getUnallocatedSpace();
        System.out.println("Total space: " + total_space + 
            ", Free space before: " + free_space_before + ", Free space after: " + free_space_after +
            ", Unallocated space before: " + unallocated_space_before + ", Unallocated space after: " + unallocated_space_after + ", Block size: " + fs.getBlockSize());
        
        outputLog = outputLog + "," + folders_create_time;
        outputLog = outputLog + "," + file_write_cumulative_time;
        outputLog = outputLog + "," + file_create_min_time;
        outputLog = outputLog + "," + file_create_max_time;
        outputLog = outputLog + "," + file_write_cumulative_time/NUM_TOTAL_FILES;
        outputLog = outputLog + "," + file_data;
        outputLog = outputLog + "," + file_data/NUM_TOTAL_FILES;
        outputLog = outputLog + "," + (((float)file_data/(float)file_write_cumulative_time)*1000);
        
        outputLog = outputLog + folder_throughput_log;

        outputLog = outputLog + "," + total_space + "," + free_space_before + "," + free_space_after + "," + unallocated_space_before + "," + unallocated_space_after;

        outputLog = outputLog + "," + Long.toString(System.nanoTime());

        String endTime = LocalTime.now().toString();

        outputLog = outputLog + "," + endTime;

        System.out.println("End Time: " + endTime);

        System.out.println(outputLog);

    }

    public static String displayValue(long nanoseconds) {
        String string = "";

        double msec = (double) nanoseconds/1000000;
        double sec = msec/1000;

        string = Double.toString(msec) + " msec";
        if (msec >= 500)
            string = string +  " (" + Double.toString(sec) + " sec)";
        return string;
    }
    
}

class CreateFolderAndFiles implements Runnable {

    private boolean dry_run = false;          // turn on or off to eliminate disk I/O durations

    private String folderName;
    private Statistics statistics = new Statistics();
    private Hashtable<String, Statistics> allStatistics;
    private int NUM_FILES_PER_FOLDER;

    public CreateFolderAndFiles(String folderName, int NUM_FILES_PER_FOLDER, Hashtable<String, Statistics> allStatistics) {
        this.folderName = folderName;
        this.allStatistics = allStatistics;
        this.NUM_FILES_PER_FOLDER = NUM_FILES_PER_FOLDER;
    }
 
    public String getFolderName() {
        return this.folderName;
    }        

    @Override
    public void run() {

        String progressBar = "";
        
        /**
         * File size normal distribution
         */
        double mean = 5500.0;
        double sd = 1000.0;        
        NormalDistribution nd = new NormalDistribution(mean, sd);
        
        /**
         * Create folder
         */
        Path path = Paths.get(this.folderName);
        
        statistics.folder_create_start_time = System.nanoTime();
        if (dry_run == false) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        statistics.folder_create_end_time = System.nanoTime();
        statistics.folder_create_time = statistics.folder_create_end_time - statistics.folder_create_start_time;

        /**
         * Create files in the folder
         */
        OpenOption[] options = new OpenOption[] { StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };
        int fileSize;

        long file_write_start_time = 0;
        long file_write_end_time = 0;
        long file_write_time = 0;
        long folder_file_total_write_time = 0;
        int progressPrev = 0;
        int progressCurr = 0;

        for (int i = 1; i <= NUM_FILES_PER_FOLDER; i++) {
            
            fileSize = Math.abs((int)(nd.sample()));      // create file size

            statistics.file_data_per_folder = statistics.file_data_per_folder + fileSize;

            // file size histogram
            for (int j = 0; j < Constants.BUCKETS_SIZE; j++) {
                if (fileSize >= Constants.fileSize_range[j][0] && fileSize < Constants.fileSize_range[j][1]) {
                    statistics.bucket_size[j] = statistics.bucket_size[j] + 1;     
                }
            }
            
            path = Paths.get(this.folderName + "/" + Long.toString(System.nanoTime()));
            statistics.folder_id = Paths.get(this.folderName).getFileName().toString();
            file_write_start_time = System.nanoTime();      // individual file start and end time tracking
            if (dry_run == false) {
                try {
                    Files.createFile(path);
                    byte[] file_bytes = add_checksum(new byte[fileSize]);
                    Files.write(path, file_bytes, options);
                    if (verify_checkcum(file_bytes) == false)
                        System.out.println("File read verification failed");
                    //else
                    //    System.out.println("Verification OK");
                    //Files.write(path, new byte[fileSize], options);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            file_write_end_time = System.nanoTime();
            file_write_time = file_write_end_time - file_write_start_time;
            statistics.file_write_cumulative_time = statistics.file_write_cumulative_time + file_write_time;

            folder_file_total_write_time = folder_file_total_write_time + file_write_time;

            // write speed histogram
            for (int j = 0; j < Constants.BUCKETS_WRITE; j++) {
                if (file_write_time >= Constants.write_range[j][0] && file_write_time < Constants.write_range[j][1]) {
                    statistics.bucket_write[j] = statistics.bucket_write[j] + 1;
                }
            }
    
            // find Min, Max
            if (file_write_time < statistics.file_create_min_time)
                statistics.file_create_min_time = file_write_time;
            if (file_write_time > statistics.file_create_max_time)
                statistics.file_create_max_time = file_write_time;

            progressCurr = (int)((float)i*100/(float)NUM_FILES_PER_FOLDER);
            if (progressCurr > progressPrev) {
                progressBar = progressBar + ".";
                System.out.print("\r" + progressBar + "[" + progressCurr + "%] [" + Paths.get(this.folderName).getFileName() + "] [" + statistics.file_data_per_folder + "] [" + folder_file_total_write_time + "] [" + (float)statistics.file_data_per_folder/(float)folder_file_total_write_time*1000 + "]");
                progressPrev = progressCurr;
            }

        }
        System.out.println();
        statistics.file_create_average_time = folder_file_total_write_time/NUM_FILES_PER_FOLDER;
        allStatistics.put(this.folderName, statistics);
    }

    public byte[] add_checksum(byte[] file_bytes) {
        
        ByteBuffer checksum_buffer = ByteBuffer.allocate(Long.BYTES);
        Checksum crc32 = new CRC32();
        long checksum_long = 0;
        
        int fileSize = file_bytes.length;
        
        crc32.update(file_bytes, 0, fileSize - Long.BYTES);     // make space for checksum storage, its 8 bytes will replace last 8 bytes of the byte array
        
        checksum_long = crc32.getValue();
        
        byte[] checksum_bytes = checksum_buffer.putLong(checksum_long).array();
        
        for (int i = 0; i < Long.BYTES; i++) {
            file_bytes[fileSize - Long.BYTES + i] = checksum_bytes[i];      // replace last 8 bytes of file data with checksum bytes
        }

        return file_bytes;
    }

    public boolean verify_checkcum(byte[] file_bytes) {
        
        ByteBuffer checksum_buffer = ByteBuffer.allocate(Long.BYTES);
        Checksum crc32 = new CRC32();
        long checksum_long = 0;

        int fileSize = file_bytes.length;

        crc32.update(file_bytes, 0, fileSize - Long.BYTES);     // make space for checksum storage, its 8 bytes will replace last 8 bytes of the byte array

        checksum_long = crc32.getValue();

        byte[] checksum_bytes = checksum_buffer.putLong(checksum_long).array();

        for (int i = 0; i < Long.BYTES; i++) {
            //System.out.println(file_bytes[fileSize - Long.BYTES + i] + "," + checksum_bytes[i]);
            if (file_bytes[fileSize - Long.BYTES + i] != checksum_bytes[i])
                return false;
        }

        return true;
    }

    /*
    public void store_fileSize_bucket(Statistics statistics, int fileSize) {

        int size_multiplier = 1000;            // adjust the multiplier for dry run vs real run

        int fileSize_range[][] = {
            {0 * size_multiplier,  1 * size_multiplier},
            {1 * size_multiplier,  2 * size_multiplier},
            {2 * size_multiplier,  3 * size_multiplier},
            {3 * size_multiplier,  4 * size_multiplier},
            {4 * size_multiplier,  5 * size_multiplier},
            {5 * size_multiplier,  6 * size_multiplier},
            {6 * size_multiplier,  7 * size_multiplier},
            {7 * size_multiplier,  8 * size_multiplier},
            {8 * size_multiplier,  9 * size_multiplier},
            {9 * size_multiplier,  10 * size_multiplier},
        };


        if (fileSize >= 1000 && fileSize < 2000)
            statistics.bucket_size[0] = statistics.bucket_size[0] + 1;
        else if (fileSize >= 2000 && fileSize < 3000)
            statistics.bucket_size[1] = statistics.bucket_size[1] + 1;
        else if (fileSize >= 3000 && fileSize < 4000)
            statistics.bucket_size[2] = statistics.bucket_size[2] + 1;
        else if (fileSize >= 4000 && fileSize < 5000)
            statistics.bucket_size[3] = statistics.bucket_size[3] + 1;
        else if (fileSize >= 5000 && fileSize < 6000)
            statistics.bucket_size[4] = statistics.bucket_size[4] + 1;
        else if (fileSize >= 6000 && fileSize < 7000)
            statistics.bucket_size[5] = statistics.bucket_size[5] + 1;
        else if (fileSize >= 7000 && fileSize < 8000)
            statistics.bucket_size[6] = statistics.bucket_size[6] + 1;
        else if (fileSize >= 8000 && fileSize < 9000)
            statistics.bucket_size[7] = statistics.bucket_size[7] + 1;
        else if (fileSize >= 9000 && fileSize <= 10000)
            statistics.bucket_size[8] = statistics.bucket_size[8] + 1;

    }

    public void store_write_bicket(Statistics statistics, long file_write_time) {

        int multiplier = 45;            // adjust the multiplier for dry run vs real run

        int range[][] = {
            {0 * multiplier,  1 * multiplier},
            {1 * multiplier,  2 * multiplier},
            {2 * multiplier,  3 * multiplier},
            {3 * multiplier,  4 * multiplier},
            {4 * multiplier,  5 * multiplier},
            {5 * multiplier,  6 * multiplier},
            {6 * multiplier,  7 * multiplier},
            {7 * multiplier,  8 * multiplier},
            {8 * multiplier,  9 * multiplier},
            {9 * multiplier,  10 * multiplier},
        };

        //System.out.println("Start:" + range[1][0] + ", " + range[1][1]);

        for (int i = 0; i < Constants.BUCKETS_WRITE; i++) {
            if (file_write_time >= range[i][0] && file_write_time < range[i][1]) {
                statistics.bucket_write[i] = statistics.bucket_write[i] + 1;     
            //} else {
            //    System.out.println(range[i][0] + ", " + range[i][1]);
            }
        }

    }
    */
    
}

/**
 * Per folder statics
 * 
 */
class Statistics {

    String folder_id;

    long folder_create_start_time;
    long folder_create_end_time;
    long folder_create_time = 0;                // time to create this folder

    long files_create_start_time;
    long files_create_end_time;
    long files_create_time = 0;                  // total time to create ALL files in this folder

    long file_create_min_time = Long.MAX_VALUE; // minimum time to create a file
    long file_create_max_time = Long.MIN_VALUE; // maximum time to create a file
    long file_create_average_time = 0;          // average time to create a file

    long file_write_cumulative_time = 0;

    long file_data_per_folder = 0;

    long bucket_size[] = new long[Constants.BUCKETS_SIZE];      // bucket to store file sizes

    long bucket_write[] = new long[Constants.BUCKETS_WRITE];

    long bucket_read[] = new long[Constants.BUCKETS_READ];


    public Statistics() {

    }
}
