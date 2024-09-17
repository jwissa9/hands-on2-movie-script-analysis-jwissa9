package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //gets the line
        String[] tokens = line.split(":", 2); //splits apart the character and their quote

        if (tokens.length == 2) {
            String characterName = tokens[0].trim(); //gets the character
            String quote = tokens[1].trim(); //gets their quote

            String[] words = quote.split("\\s+"); //splits on any whitespace
            int count = words.length; //counts the length

            character.set(characterName); //get the character name and their word count
            wordCount.set(count);
            context.write(character, wordCount);
        }
    }
}
