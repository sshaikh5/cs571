package edu.gmu.cs571;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Hello world!
 *
 */
public class App 
{
    //private Checksum crc32 = new CRC32();
    
    public static void main( String[] args ) throws IOException
    {
        //Creator1.run();
        Reader2.run();

        //Creator1.run();
        //Reader.run();
/*
        Checksum crc32 = new CRC32();
        String folder_absolute_path;
        long folder_start_time = 0;
        long folder_end_time = 0;
        Path path;
        File file;
        int buffer_size = 16*1024;
        byte[] bytes = new byte[buffer_size];
        int bytes_read = 0;
        long folder_bytes_read = 0;

        FileInputStream fis;
        //DataInputStream reader;
        BufferedInputStream reader;
        long fis_start = 0;
        long fis_end = 0;
        long reader_start = 0;
        long reader_end = 0;

        for (int folder = 1; folder <= 1000; folder++) {

            folder_start_time = System.nanoTime();    
            folder_absolute_path = "/home/sohail/4thdd/run1/" + Integer.toString(folder);
            folder_bytes_read = 0;

            try (Stream<Path> paths = Files.walk(Paths.get(folder_absolute_path))) {

                Iterator<Path> files = paths.filter(Files::isRegularFile).sorted().iterator();

                while (files.hasNext()) {   // loop thru the files in the folder
                    path = files.next();
                    reader_start = System.nanoTime();
                    fis = new FileInputStream(path.toAbsolutePath().toString());
                    reader = new BufferedInputStream(fis, buffer_size);
                    //reader = new DataInputStream(fis);
                    bytes_read = reader.read(bytes);
                    reader_end = System.nanoTime();
                    if (verify_checkcum(bytes, bytes_read, crc32) == false) {
                        System.out.println("CRC32 failed - " + bytes_read);
                    }
                    reader.close();
                    fis.close();
                    folder_bytes_read = folder_bytes_read + bytes_read;
                    //if (((int)(reader_end-reader_start)/1000) > 2000) {
                    //    System.gc();
                    //    //System.out.println("------------------" + ((int)(reader_end-reader_start)/1000));
                    //}
                    //System.out.println("(" + folder_absolute_path + "," + 
                    //                    folder_bytes_read + "," + 
                    //                    //(int)(fis_end-fis_start)/1000 + "," + 
                    //                    (int)(reader_end-reader_start)/1000 + ")");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }            
            folder_end_time = System.nanoTime();
            System.out.println(folder_absolute_path + "," + 
                                folder_bytes_read + "," +
                                //folder_end_time + "," + 
                                //folder_start_time + "," + 
                                (float)(folder_end_time-folder_start_time)/1000000000.0);
            System.gc();
        }
*/
    }

/*    
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
*/    

}
