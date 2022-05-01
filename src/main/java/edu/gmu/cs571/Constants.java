package edu.gmu.cs571;

public class Constants {
    
    //public final static String STORAGE_PATH = "/home/sohail/2tssd/run1/";

    public final static int THREADS = 1;

    // file size histogram
    public final static int BUCKETS_SIZE = 10;
    public final static int size_multiplier = 1000;     // from 1KB to 10KB file size buckets
    public final static int fileSize_range[][] = {
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
        {10 * size_multiplier,  11 * size_multiplier},
    };

    // write speed histogram
    public final static int BUCKETS_WRITE = 20;          // this needs to match with the array below
    public final static int multiplier = 25000;            // adjust the multiplier for dry run vs real run
    public final static int write_range[][] = {
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

    public final static int BUCKETS_READ = 9;

    //public final static int READ_BUCKET = 20;

}
