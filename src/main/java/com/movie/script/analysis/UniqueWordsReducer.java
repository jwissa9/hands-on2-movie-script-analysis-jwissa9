package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> unique = new HashSet<>();

        //gets the values associated from the key
        //hashset ensures uniqueness
        for (Text v : values) {
            unique.add(v.toString());
        }

        //character's name (key) and their unique word (value) are written to the output from the hash set
        for (String u : unique) {
            context.write(key, new Text(u));
        }
    }
}
