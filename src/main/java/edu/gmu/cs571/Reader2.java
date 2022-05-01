package edu.gmu.cs571;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class Reader2 {

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

    public static int NUM_FOLDERS = 5000;
    public static int NUM_FILES_PER_FOLDER = 200000;
    public static int READ_BUCKET = 20;
    public static boolean random = false;                    // change it to false for sequential, true for random
    public final static String STORAGE_PATH = "/home/sohail/12thdd/run1/";
    
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

    public static void run() throws IOException {

        String startTime = LocalTime.now().toString();
        System.out.println("Start Time: " + startTime);
        long start_time_nanosec = System.nanoTime();
        System.out.println(start_time_nanosec);

        String outlog = startTime + "," + start_time_nanosec;

        System.out.println("....................................................................................................[%][Folder ID, Files Processed, Failed Checksums, Total File Read Time, Read Time Min, Ave, Max]");

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Constants.THREADS);
        Hashtable<String, ReadStatistics2> folders_statistics = new Hashtable<>();
        if (random == false) {
            Files.list(new File(STORAGE_PATH).toPath())
            //.limit(1000)      // TODO: remove
            .sorted()
            .forEach(folder_absolute_path -> {
                ReadStatistics2 folder_statistics = new ReadStatistics2();
                folders_statistics.put(folder_absolute_path.toAbsolutePath().toString(), folder_statistics);
                executor.execute(new ReadFoldersFiles2(folder_absolute_path.toAbsolutePath().toString(), folder_statistics));
            });
        } 
        else {
            Files.list(new File(STORAGE_PATH).toPath())
            //.limit(1000)      // TODO: remove
            .forEach(folder_absolute_path -> {
                ReadStatistics2 folder_statistics = new ReadStatistics2();
                folders_statistics.put(folder_absolute_path.toAbsolutePath().toString(), folder_statistics);
                executor.execute(new ReadFoldersFiles2(folder_absolute_path.toAbsolutePath().toString(), folder_statistics));
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Executor shutdown got interrupted");
        }
        long end_time_nanosec = System.nanoTime();

        /**
         * Processing statistics
         */
        System.out.println();
        System.out.println("Processing statistics...");
        System.out.println("Number of folders: " + NUM_FOLDERS);
        System.out.println("Number of files per folder: " + NUM_FILES_PER_FOLDER);
        System.out.println("Number of total files: " + NUM_FOLDERS * NUM_FILES_PER_FOLDER);
        System.out.println("Number of threads: " + Constants.THREADS);        

        //System.out.print("Processing folder: ");
        ReadStatistics2 run_statistics = new ReadStatistics2();
        for (Map.Entry<String, ReadStatistics2> entry : folders_statistics.entrySet()) {

            run_statistics.count_files_failed_verification  = run_statistics.count_files_failed_verification    + entry.getValue().count_files_failed_verification;
            run_statistics.total_file_data                  = run_statistics.total_file_data                    + entry.getValue().total_file_data;
            run_statistics.total_files_read_time            = run_statistics.total_files_read_time              + entry.getValue().total_files_read_time;
            
            if (run_statistics.file_read_time_min > entry.getValue().file_read_time_min) {
                run_statistics.file_read_time_min = entry.getValue().file_read_time_min;
            }
            if (run_statistics.file_read_time_max < entry.getValue().file_read_time_max) {
                run_statistics.file_read_time_max = entry.getValue().file_read_time_max;
            }

            for (int i = 0; i < Reader2.NUM_FILES_PER_FOLDER; i++) {
                run_statistics.file_read_time[i] = run_statistics.file_read_time[i] + entry.getValue().file_read_time[i];
            }
            
            for (int i = 0; i < Reader2.READ_BUCKET; i++) {
                run_statistics.read_histogram_bucket[i] = run_statistics.read_histogram_bucket[i] + entry.getValue().read_histogram_bucket[i];
            }
        }
        run_statistics.file_read_time_ave = run_statistics.total_files_read_time / (Reader2.NUM_FILES_PER_FOLDER * Reader2.NUM_FOLDERS);
        System.out.println();
        System.out.println("Total files read time: " + display_nanoseconds(run_statistics.total_files_read_time));
        System.out.println("Total file data: " + run_statistics.total_file_data);
        System.out.println("File read time (min): " + display_nanoseconds(run_statistics.file_read_time_min));
        System.out.println("File read time (max): " + display_nanoseconds(run_statistics.file_read_time_max));
        System.out.println("File read time (ave): " + display_nanoseconds(run_statistics.file_read_time_ave));
        System.out.println("File reads per sec: " + (long)(((Reader2.NUM_FILES_PER_FOLDER * Reader2.NUM_FOLDERS)*1000000000.0)/(float)(end_time_nanosec - start_time_nanosec)));
        System.out.println("Number of failed read checksum: " + run_statistics.count_files_failed_verification);
        System.out.println("Total run time: " + display_nanoseconds(end_time_nanosec - start_time_nanosec));

        outlog = outlog + "," + run_statistics.total_files_read_time;
        outlog = outlog + "," + run_statistics.total_file_data;
        outlog = outlog + "," + run_statistics.file_read_time_min;
        outlog = outlog + "," + run_statistics.file_read_time_max;
        outlog = outlog + "," + run_statistics.file_read_time_ave;
        outlog = outlog + "," + run_statistics.count_files_failed_verification;
        
        long total_files_read_time_by_folder = 0;
        for (int i = 0; i < Reader2.NUM_FILES_PER_FOLDER; i++) {
            total_files_read_time_by_folder = total_files_read_time_by_folder + run_statistics.file_read_time[i];
        }
        System.out.println("Total files read time by folder: " + display_nanoseconds(total_files_read_time_by_folder));
        outlog = outlog + "," + total_files_read_time_by_folder;
        
        for (int i = 0; i < Reader2.READ_BUCKET; i++) {
            System.out.println(display_nanoseconds(read_range[i][0]) + "\t - " + display_nanoseconds(read_range[i][1]) + ":\t" + run_statistics.read_histogram_bucket[i]);
            //System.out.println("Bucket[" + i + "]: " + run_statistics.read_histogram_bucket[i]);
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

    
}

class ReadFoldersFiles2 implements Runnable {

    private String folder_absolute_path;
    private ReadStatistics2 folder_statistics;

    public ReadFoldersFiles2(String folder_absolute_path, ReadStatistics2 folder_statistics) {
        this.folder_absolute_path = folder_absolute_path;
        this.folder_statistics = folder_statistics;
    }

    @Override
    public void run() {

        Checksum crc32 = new CRC32();
        long folder_start_time = System.nanoTime();
        long folder_end_time = 0;
        long file_read_start_time = 0;
        long count_files_failed_verification = 0;
        Path path;
        int buffer_size = 16*1024;
        byte[] bytes = new byte[buffer_size];
        int bytes_read = 0;
        long folder_bytes_read = 0;
        int file_id = 0;
        FileInputStream fis;
        BufferedInputStream reader;

        Iterator<Path> files;
        try (Stream<Path> paths = Files.walk(Paths.get(folder_absolute_path))) {
            if (Reader2.random == false)
                files = paths.filter(Files::isRegularFile).sorted().iterator();
            else
                files = paths.filter(Files::isRegularFile).iterator();

            while (files.hasNext() && file_id < Reader2.NUM_FILES_PER_FOLDER) {   // loop thru the files in the folder
                file_read_start_time = System.nanoTime();
                path = files.next();
                fis = new FileInputStream(path.toAbsolutePath().toString());
                reader = new BufferedInputStream(fis, buffer_size);
                // remove comments for real run
                bytes_read = reader.read(bytes);
                if (verify_checkcum(bytes, bytes_read, crc32) == false) {
                    //System.out.println("CRC32 failed");
                    count_files_failed_verification++;
                }
                reader.close();
                fis.close();
                folder_bytes_read = folder_bytes_read + bytes_read;
                folder_statistics.total_file_data = folder_statistics.total_file_data + bytes_read;
                folder_statistics.file_read_time[file_id] = System.nanoTime() - file_read_start_time;
                folder_statistics.total_files_read_time = folder_statistics.total_files_read_time + folder_statistics.file_read_time[file_id];

                // read histogram
                for (int i = 0; i < Reader2.READ_BUCKET; i++) {
                   if (folder_statistics.file_read_time[file_id] >= Reader2.read_range[i][0] && folder_statistics.file_read_time[file_id] < Reader2.read_range[i][1]) {
                       folder_statistics.read_histogram_bucket[i] = folder_statistics.read_histogram_bucket[i] + 1;
                   }
                }

                // find Min, Max
                if (folder_statistics.file_read_time[file_id] < folder_statistics.file_read_time_min) {
                    folder_statistics.file_read_time_min = folder_statistics.file_read_time[file_id];
                }
                if (folder_statistics.file_read_time[file_id] > folder_statistics.file_read_time_max) {
                    folder_statistics.file_read_time_max = folder_statistics.file_read_time[file_id];
                }

                file_id++;  // increment file id
            }

            folder_statistics.count_files_failed_verification = count_files_failed_verification;
            folder_statistics.file_read_time_ave = folder_statistics.total_files_read_time/(file_id-1);

            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
        }            
        folder_end_time = System.nanoTime();
        System.out.println(folder_absolute_path + "," + 
                            folder_bytes_read + "," +
                            (float)(folder_end_time-folder_start_time)/1000000000.0);
        System.gc();
    
    }

    public static boolean verify_checkcum(byte[] file_bytes, int length, Checksum crc32) {    
        ByteBuffer checksum_buffer = ByteBuffer.allocate(Long.BYTES);
        long checksum_long = 0;
        crc32.reset();
        //int fileSize = file_bytes.length;
        int fileSize = length;
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

class ReadStatistics2 {
    long total_file_data = 0;
    long[] file_read_time = new long[Reader2.NUM_FILES_PER_FOLDER];
    long total_files_read_time = 0;
    long count_files_failed_verification = 0;
    int read_histogram_bucket[] = new int[Reader2.READ_BUCKET];
    long file_read_time_min = Long.MAX_VALUE;
    long file_read_time_max = Long.MIN_VALUE;
    long file_read_time_ave = 0;
}