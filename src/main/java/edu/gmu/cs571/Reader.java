package edu.gmu.cs571;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.Checksum;
import java.util.zip.CRC32;

public class Reader {

    private static int current_folder_id = 0;
    private static Random randomGenerator = new Random();

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
    public final static String STORAGE_PATH = "/home/sohail/4thdd/run1/";
    public static int NUM_FOLDERS = 1000;
    public static int NUM_FILES_PER_FOLDER = 100000;
    //long NUM_TOTAL_FILES = 10000;
    public static int READ_BUCKET = 20;
    public final static long multiplier = 6000;            // adjust the multiplier for dry run vs real run
    public final static long read_range[][] = {
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
        {10 * multiplier,  11 * multiplier},
        {11 * multiplier,  12 * multiplier},
        {12 * multiplier,  13 * multiplier},
        {13 * multiplier,  14 * multiplier},
        {14 * multiplier,  15 * multiplier},
        {15 * multiplier,  16 * multiplier},
        {16 * multiplier,  17 * multiplier},
        {17 * multiplier,  18 * multiplier},
        {18 * multiplier,  19 * multiplier},
        {19 * multiplier,  20 * multiplier},
        {20 * multiplier,  21 * multiplier},
    };

    
    public Reader() {

    }
    
    public static void run() throws IOException {

        long NUM_TOTAL_FILES = NUM_FOLDERS * NUM_FILES_PER_FOLDER;
        if (NUM_TOTAL_FILES != (NUM_FOLDERS * NUM_FILES_PER_FOLDER)) {
            System.out.println("Invalid parameters");
            return;
        }

        String startTime = LocalTime.now().toString();
        System.out.println("Start Time: " + startTime);
        long start_time_nanosec = System.nanoTime();
        System.out.println(start_time_nanosec);

        String outlog = startTime + "," + start_time_nanosec;

        System.out.println("....................................................................................................[%][Folder ID, Files Processed, Failed Checksums, Total File Read Time, Read Time Min, Ave, Max]");
        
        Hashtable<String, ReaderStatistics> folders_statistics = new Hashtable<>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Constants.THREADS);

        boolean random = false;     // change the flag for random reads or sequential reads

        if (random == true) {
            do {
                current_folder_id = generateFolderID(NUM_FOLDERS + ((NUM_FOLDERS * 10)/100));
                if (current_folder_id <= NUM_FOLDERS && folders_statistics.containsKey(String.valueOf(current_folder_id)) == false) {       // if this folder has not been analyzed, generate a thread and execute
                    ReaderStatistics folder_statistics = new ReaderStatistics();
                    folders_statistics.put(String.valueOf(current_folder_id), folder_statistics);
                    ReadFoldersFiles readFolderAndFiles = new ReadFoldersFiles(current_folder_id, folder_statistics, random);
                    executor.execute(readFolderAndFiles);
                } 
                //else
                //    System.out.println("Clash: " + current_folder_id);
            } while (folders_statistics.size() < NUM_FOLDERS);
        }
        else {
            for (int i = 1; i <= NUM_FOLDERS; i++) {
                if (folders_statistics.containsKey(String.valueOf(i)) == false) {       // if this folder has not been analyzed, generate a thread and execute
                    ReaderStatistics folder_statistics = new ReaderStatistics();
                    folders_statistics.put(String.valueOf(i), folder_statistics);                    
                    ReadFoldersFiles readFolderAndFiles = new ReadFoldersFiles(i, folder_statistics, random);
                    executor.execute(readFolderAndFiles);
                }
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Executor shutdown got interrupted");
        }
        System.out.println("Folders processed: " + folders_statistics.size());
        long end_time_nanosec = System.nanoTime();

        /**
         * Process statics
         */
        System.out.println();
        System.out.println("Processing statistics...");
        System.out.println("Number of folders: " + NUM_FOLDERS);
        System.out.println("Number of files per folder: " + NUM_FILES_PER_FOLDER);
        System.out.println("Number of total files: " + NUM_TOTAL_FILES);
        System.out.println("Number of threads: " + Constants.THREADS);

        //System.out.print("Processing folder: ");
        ReaderStatistics run_statistics = new ReaderStatistics();
        for (Map.Entry<String, ReaderStatistics> entry : folders_statistics.entrySet()) {
            //System.out.print(entry.getKey() + ",");
            run_statistics.count_files_failed_verification  = run_statistics.count_files_failed_verification    + entry.getValue().count_files_failed_verification;
            run_statistics.total_file_data                  = run_statistics.total_file_data                    + entry.getValue().total_file_data;
            run_statistics.total_files_read_time            = run_statistics.total_files_read_time              + entry.getValue().total_files_read_time;
            
            if (run_statistics.file_read_time_min > entry.getValue().file_read_time_min) {
                //System.out.println("Min: " + run_statistics.file_read_time_min + " < " + entry.getValue().file_read_time_min);
                run_statistics.file_read_time_min = entry.getValue().file_read_time_min;
            }
            if (run_statistics.file_read_time_max < entry.getValue().file_read_time_max) {
                //System.out.println("Max: " + run_statistics.file_read_time_max + " < " + entry.getValue().file_read_time_max);
                run_statistics.file_read_time_max = entry.getValue().file_read_time_max;
            }
            for (int i = 0; i < Reader.NUM_FILES_PER_FOLDER; i++) {
                run_statistics.file_read_time[i] = run_statistics.file_read_time[i] + entry.getValue().file_read_time[i];
            }
            for (int i = 0; i < Reader.READ_BUCKET; i++) {
                run_statistics.read_histogram_bucket[i] = run_statistics.read_histogram_bucket[i] + entry.getValue().read_histogram_bucket[i];
            }
        }
        run_statistics.file_read_time_ave = run_statistics.total_files_read_time / (Reader.NUM_FILES_PER_FOLDER * Reader.NUM_FOLDERS);
        System.out.println();
        System.out.println("Total files read time: " + display_nanoseconds(run_statistics.total_files_read_time));
        System.out.println("Total file data: " + run_statistics.total_file_data);
        System.out.println("File read time (min): " + display_nanoseconds(run_statistics.file_read_time_min));
        System.out.println("File read time (max): " + display_nanoseconds(run_statistics.file_read_time_max));
        System.out.println("File read time (ave): " + display_nanoseconds(run_statistics.file_read_time_ave));
        System.out.println("File reads per sec: " + (long)(((Reader.NUM_FILES_PER_FOLDER * Reader.NUM_FOLDERS)*1000000000.0)/(float)(end_time_nanosec - start_time_nanosec)));
        System.out.println("Number of failed read checksum: " + run_statistics.count_files_failed_verification);
        System.out.println("Total run time: " + display_nanoseconds(end_time_nanosec - start_time_nanosec));

        outlog = outlog + "," + run_statistics.total_files_read_time;
        outlog = outlog + "," + run_statistics.total_file_data;
        outlog = outlog + "," + run_statistics.file_read_time_min;
        outlog = outlog + "," + run_statistics.file_read_time_max;
        outlog = outlog + "," + run_statistics.file_read_time_ave;
        outlog = outlog + "," + run_statistics.count_files_failed_verification;
        
        long total_files_read_time_by_folder = 0;
        for (int i = 0; i < Reader.NUM_FILES_PER_FOLDER; i++) {
            total_files_read_time_by_folder = total_files_read_time_by_folder + run_statistics.file_read_time[i];
        }
        System.out.println("Total files read time by folder: " + display_nanoseconds(total_files_read_time_by_folder));
        outlog = outlog + "," + total_files_read_time_by_folder;
        
        for (int i = 0; i < Reader.READ_BUCKET; i++) {
            System.out.println("Bucket[" + i + "]: " + run_statistics.read_histogram_bucket[i]);
            outlog = outlog + "," + run_statistics.read_histogram_bucket[i];
        }

        outlog = outlog + "," + System.nanoTime();
        outlog = outlog + "," + LocalTime.now().toString();

        System.out.println(outlog);

    }

    public static String display_nanoseconds(long nanoseconds) {
        String s = "";
        if (nanoseconds < 1000)     // nanosec
            s = String.valueOf(nanoseconds) + " nanosec";
        else if (nanoseconds >= 1000 && nanoseconds <= 1000000) {   // microsec
            s = String.valueOf((float)nanoseconds/1000.0) + " microsec";
        } else if (nanoseconds >= 1000000 && nanoseconds <= 1000000000) {   // millisec
            s = String.valueOf((float)nanoseconds/1000000.0) + " millisec";
        } else if (nanoseconds >= 1000000000) {   // sec
            s = String.valueOf((float)nanoseconds/1000000000.0) + " sec";
        }
        return s;
    }

    public static int generateFolderID(int numberOfFolders) {
        current_folder_id = randomGenerator.nextInt(numberOfFolders) + 1;
        //System.out.println("Folder ID: " + current_folder_id);
        return current_folder_id;
    }
}

class ReadFoldersFiles implements Runnable {

    int folder_id;
    ReaderStatistics folder_statistics;
    private Checksum crc32 = new CRC32();
    private boolean random;

    public ReadFoldersFiles(int folder_id, ReaderStatistics folder_statistics, boolean random) {
        this.folder_id = folder_id;
        this.folder_statistics = folder_statistics;
        this.random = random;
    }
    
    @Override
    public void run() {

        //Random randomGenerator = new Random();
        String folder_id = String.valueOf(this.folder_id);
        String folder_absolute_path = Reader.STORAGE_PATH + "/" + folder_id;

        try {
            // build map of directories to thier names can be retrieved sequentially or randomly
            Map<Integer, String> dir_list = new HashMap<>();
            Path folder_path = Paths.get(folder_absolute_path);
            if (Files.isDirectory(folder_path)) {
                List<Path> file_paths = Files.list(folder_path).collect(Collectors.toList());
                int file_index = 1;
                for (Path file_path : file_paths) {
                    dir_list.put(Integer.valueOf(file_index++), file_path.toAbsolutePath().toString());
                    if (file_index > Reader.NUM_FILES_PER_FOLDER)
                        break;
                }
            }

            Map<String, ReaderStatistics> processed_files = new HashMap<>();   // build this map for files that are processed

            int file_id = 0;
            long files_in_the_folder = dir_list.size();
            long progressPrev = 0;
            long progressCurr = 0;
            String progressBar = "";
            long count_files_failed_verification = 0;
            long file_read_start_time = 0;
            String file_path;
            Path path;
            
            byte[] file_bytes = new byte[1];
            byte[] file_buffer = new byte[16384];
            int byte_count = 0;

            Iterator dir_iterator = dir_list.keySet().iterator();
            
            do {
                if (random) {
                    if (dir_iterator.hasNext())    // psuedo random
                        file_id = (int) dir_iterator.next();
                }
                else
                    file_id++;
                
                file_read_start_time = System.nanoTime();

                file_path = dir_list.get(Integer.valueOf(file_id));        // get a file name to be read and verified

                // read file
                path = Paths.get(file_path);
                //file_bytes = Files.readAllBytes(path);   // read the file

                
                FileInputStream fis = new FileInputStream(path.toAbsolutePath().toString());
                BufferedInputStream bis = new BufferedInputStream(fis, 16384);
                while((byte_count = bis.read(file_buffer, 0, 16384)) != -1) {
                    file_bytes = new byte[byte_count];
                    //System.out.println(byte_count + "," + file_bytes.length);
                }
                //if (verify_checkcum(file_bytes) == false) {     // verify file content
                //    System.out.println("Checksum verification failed for file " + file_path);
                //    count_files_failed_verification++;
                //}

                folder_statistics.file_read_time[file_id-1] = System.nanoTime() - file_read_start_time;
                folder_statistics.total_files_read_time = folder_statistics.total_files_read_time + folder_statistics.file_read_time[file_id-1]; 

                // read histogram
                for (int i = 0; i < Reader.READ_BUCKET; i++) {
                   if (folder_statistics.file_read_time[file_id-1] >= Reader.read_range[i][0] && folder_statistics.file_read_time[file_id-1] < Reader.read_range[i][1]) {
                       folder_statistics.read_histogram_bucket[i] = folder_statistics.read_histogram_bucket[i] + 1;
                   }
                }
                
                processed_files.put(file_path, null);
                folder_statistics.total_file_data = folder_statistics.total_file_data + file_bytes.length;
                file_bytes = null;

                bis.close();
                fis.close();

                // find Min, Max
                if (folder_statistics.file_read_time[file_id-1] < folder_statistics.file_read_time_min) {
                    folder_statistics.file_read_time_min = folder_statistics.file_read_time[file_id-1];
                }
                if (folder_statistics.file_read_time[file_id-1] > folder_statistics.file_read_time_max) {
                    folder_statistics.file_read_time_max = folder_statistics.file_read_time[file_id-1];
                }

                // display progress bar
                progressCurr = (int)((float)processed_files.size()*100/(float)files_in_the_folder);
                if (progressCurr > progressPrev) {
                    progressBar = progressBar + ".";
                    System.out.print("\r" + progressBar + "[" + progressCurr + "%][" + folder_id);
                    progressPrev = progressCurr;
                }

            } while (processed_files.size() < files_in_the_folder);
            
            folder_statistics.count_files_failed_verification = count_files_failed_verification;
            folder_statistics.file_read_time_ave = folder_statistics.total_files_read_time/processed_files.size();
            
            System.out.println( "," + processed_files.size() +
                                "," + count_files_failed_verification +
                                "," + folder_statistics.total_files_read_time + 
                                "," + folder_statistics.file_read_time_min +
                                "," + folder_statistics.file_read_time_ave +
                                "," + folder_statistics.file_read_time_max +
                                //"," + missed_file_id +
                                "]");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public boolean verify_checkcum(byte[] file_bytes) {    
        ByteBuffer checksum_buffer = ByteBuffer.allocate(Long.BYTES);
        long checksum_long = 0;
        crc32.reset();
        int fileSize = file_bytes.length;
        crc32.update(file_bytes, 0, fileSize - Long.BYTES);     // make space for checksum storage, its 8 bytes will replace last 8 bytes of the byte array
        checksum_long = crc32.getValue();
        byte[] checksum_bytes = checksum_buffer.putLong(checksum_long).array();
        for (int i = 0; i < Long.BYTES; i++) {
            if (file_bytes[fileSize - Long.BYTES + i] != checksum_bytes[i])
                return false;
        }
        return true;
    }

}

class ReaderStatistics {
    int time = 0;
    long total_file_data = 0;
    long[] file_read_time = new long[Reader.NUM_FILES_PER_FOLDER];
    long total_files_read_time = 0;
    long count_files_failed_verification = 0;
    int read_histogram_bucket[] = new int[Reader.READ_BUCKET];
    long file_read_time_min = Long.MAX_VALUE;
    long file_read_time_max = Long.MIN_VALUE;
    long file_read_time_ave = 0;
}
