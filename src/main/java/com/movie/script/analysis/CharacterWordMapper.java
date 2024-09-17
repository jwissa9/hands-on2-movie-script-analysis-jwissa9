package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(":", 2); //splits apart the character and their quote
        
        if (tokens.length == 2) {
            String character = tokens[0].trim(); //gets the character name
            String quote = tokens[1].trim(); //gets their quote
            
            String[] items = quote.split("\\s+"); //splits on any whitespace

            for (String i: items) { //key value pair as character and their most frequent word count
                word.set(i.toLowerCase());
                characterWord.set(character + ":" + word);
                context.write(characterWord, one);
            }

        }

    }
}
