package edu.gmu.cs571;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.math3.distribution.NormalDistribution;

public class FileCreator1 {
    
    public static void run() throws IOException {
        /**
         * Options
         */
        // number of files = number of folders x files per folder
        //           1,000 = 10         x 100
        //          10,000 = 20         x 500
        //         100,000 = 100        x 1000
        //       1,000,000 = 100        x 10,000
        //      10,000,000 = 500        x 20,000
        //     100,000,000 = 1000       x 100,000
        //   1,000,000,000 = 5000       x 200,000 

        boolean dry_run = true;          // turn on or off to eliminate disk I/O durations

        long number_of_files = 1000;
        int number_of_folders = 10;
        int files_per_folder = 100;
        String storage_path = "/home/sohail/12thdd/run1/";       // HDD
        double mean = 5500.0;
        double sd = 1000.0;
        NormalDistribution nd = new NormalDistribution(mean, sd);
        int bucket_size[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};

        long bucket_write[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};      // in nanoseconds
        long bucket_boundaries[] = {0, 100, 120, 140, 160, 180, 200, 220, 240, 280, 300};

        
        if (number_of_files != number_of_folders * files_per_folder){
            System.out.println("Fix the file/folder count equation");
            return;
        }
        
        long start_time_run = System.nanoTime();
        long start_time_folders = start_time_run;
        String fully_qualified_folder_path;
        Path path;

        String folder_creation_start_time = LocalTime.now().toString();

        LinkedHashMap<String, Path> folder_paths = new LinkedHashMap<>();
        for (int folder_id = 1; folder_id <= number_of_folders; folder_id++) {
            fully_qualified_folder_path = storage_path + folder_id;
            path = Paths.get(fully_qualified_folder_path);
            folder_paths.put(fully_qualified_folder_path, path);
            // create folder here
            if (dry_run == false)
                Files.createDirectories(path);
            //System.out.println("Folder path:" + fully_qualified_folder_path);
        }
        long end_time_folders = System.nanoTime();
        long total_time_folders = end_time_folders - start_time_folders;
        //System.out.println(number_of_folders + " Folders create in " + ((double)total_time_folders/1000000000) + " seconds, Average time:" + (double)(total_time_folders/number_of_folders)/1000 + " msec");

        long file_id = 1;
        long start_time_folder;
        OpenOption[] options = new OpenOption[] { StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };
        int fileSize;
        long file_write_start_time = 0, file_write_end_time = 0, file_write_cumulative_time = 0, file_write_min_time = 10000, file_write_max_time = 0;

        for (Entry<String, Path> folder_path:folder_paths.entrySet()) {
            start_time_folder = System.nanoTime();
            
            for (int i = 1; i <= files_per_folder; i++, file_id++) {
                //System.out.println("Key:" + folder_path.getKey() + ", Value:" + folder_path.getValue() + ", i:" + i + ", file_id:" + file_id++ + ", Duration:" + (System.nanoTime()-start_time_file) + " msec");
                // create file of random size between 1K and 10K
                //fileSize = Math.abs((int)(nd.sample()));
                fileSize = (int)(nd.sample());
                //fileSize = (int)(nd.sample()*1000);
                //fileSize = 512;
                if (fileSize >= 1000 && fileSize < 2000)
                    bucket_size[0] = bucket_size[0] + 1;
                else if (fileSize >= 2000 && fileSize < 3000)
                    bucket_size[1] = bucket_size[1] + 1;
                else if (fileSize >= 3000 && fileSize < 4000)
                    bucket_size[2] = bucket_size[2] + 1;
                else if (fileSize >= 4000 && fileSize < 5000)
                    bucket_size[3] = bucket_size[3] + 1;
                else if (fileSize >= 5000 && fileSize < 6000)
                    bucket_size[4] = bucket_size[4] + 1;
                else if (fileSize >= 6000 && fileSize < 7000)
                    bucket_size[5] = bucket_size[5] + 1;
                else if (fileSize >= 7000 && fileSize < 8000)
                    bucket_size[6] = bucket_size[6] + 1;
                else if (fileSize >= 8000 && fileSize < 9000)
                    bucket_size[7] = bucket_size[7] + 1;
                else if (fileSize >= 9000 && fileSize <= 10000)
                    bucket_size[8] = bucket_size[8] + 1;
                
                path = Paths.get(folder_path.getValue() + "/" + Long.toString(System.nanoTime()));
                //System.out.println("File:" + path.toString() + ", Size:" + fileSize);

                file_write_start_time = System.nanoTime();
                if (dry_run == false) {
                    Files.createFile(path);
                    Files.write(path, new byte[fileSize], options);                
                }
                file_write_end_time = System.nanoTime() - file_write_start_time;
                file_write_cumulative_time = file_write_cumulative_time + file_write_end_time;
                //System.out.println("File write time: " + file_write_end_time);
                if (file_write_end_time < file_write_min_time)
                    file_write_min_time = file_write_end_time;
                if (file_write_end_time > file_write_max_time)
                    file_write_max_time = file_write_end_time;
                
                //if (file_write_end_time >= 0 && file_write_end_time < bucket_boundaries[0])
                //    bucket_write[0] = bucket_write[0] + 1;
                     if (file_write_end_time >= bucket_boundaries[0] && file_write_end_time < bucket_boundaries[1])
                    bucket_write[0] = bucket_write[0] + 1;
                else if (file_write_end_time >= bucket_boundaries[1] && file_write_end_time < bucket_boundaries[2])
                    bucket_write[1] = bucket_write[1] + 1;
                else if (file_write_end_time >= bucket_boundaries[2] && file_write_end_time < bucket_boundaries[3])
                    bucket_write[2] = bucket_write[2] + 1;
                else if (file_write_end_time >= bucket_boundaries[3] && file_write_end_time < bucket_boundaries[4])
                    bucket_write[3] = bucket_write[3] + 1;
                else if (file_write_end_time >= bucket_boundaries[4] && file_write_end_time < bucket_boundaries[5])
                    bucket_write[4] = bucket_write[4] + 1;
                else if (file_write_end_time >= bucket_boundaries[5] && file_write_end_time < bucket_boundaries[6])
                    bucket_write[5] = bucket_write[5] + 1;
                else if (file_write_end_time >= bucket_boundaries[6] && file_write_end_time < bucket_boundaries[7])
                    bucket_write[6] = bucket_write[6] + 1;
                else if (file_write_end_time >= bucket_boundaries[7] && file_write_end_time < bucket_boundaries[8])
                    bucket_write[7] = bucket_write[7] + 1;
                else if (file_write_end_time >= bucket_boundaries[8] && file_write_end_time < bucket_boundaries[9])
                    bucket_write[8] = bucket_write[8] + 1;
                else if (file_write_end_time >= bucket_boundaries[9] && file_write_end_time < bucket_boundaries[10])
                    bucket_write[9] = bucket_write[9] + 1;
                else
                    bucket_write[10] = bucket_write[10] + 1;

            }
            System.out.println("Time: " + LocalTime.now() + ", Folder: " + folder_path.getKey() + ", Duration: " + (System.nanoTime()-start_time_folder)/1000000 + " msec");
        }
        long total_run_time = System.nanoTime() - start_time_run;
        System.out.println("Time: " + folder_creation_start_time + ", " + number_of_folders + " Folders created in " + ((double)total_time_folders/1000000000) + " seconds or " + ((double)total_time_folders/1000000) + " msec, Average time:" + (double)(total_time_folders/number_of_folders)/1000000 + " msec");
        System.out.println("Time: " + LocalTime.now() + ", Total run time: " + (double)total_run_time/1000000000 + " sec or " + (double)total_run_time/1000000 + " msec, Average file creation time: " + (double)(total_run_time/number_of_files)/1000000 + " msec, Total files created: " + (file_id-1));
        
        for (int i=0; i <= 8; i++)
            System.out.println("Bucket_Size[" + i + "]: " + bucket_size[i]);

        for (int i=0; i < bucket_write.length-1; i++) {
            //System.out.println("Bucket_Write[" + i + "] " + bucket_boundaries[i] + "-" + bucket_boundaries[i+1] + ":" + bucket_write[i]);
            System.out.println("Bucket_Write[" + bucket_boundaries[i] + "-" + bucket_boundaries[i+1] + "]:" + bucket_write[i]);
        }
        //System.out.println("Bucket_Write[" + 10 + "] " + bucket_boundaries[10] + "+: " + bucket_write[10]);
        System.out.println("Bucket_Write[" + 10 + "] " + bucket_boundaries[10] + "+: " + bucket_write[10]);
        
        
        System.out.println("Max: " + file_write_max_time + ", Min: " + file_write_min_time + ", Average: " + file_write_cumulative_time/number_of_files);

        //folder_paths.entrySet().stream()
        //    .sorted(Map.Entry.comparingByValue())
        //    .forEach(entry -> System.out.println(entry.getKey()));

    }
}
