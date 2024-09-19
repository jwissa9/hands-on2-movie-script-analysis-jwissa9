package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(":", 2); //split the character and their quote
        if (tokens.length == 2) {
            String name = tokens[0].trim(); //character name
            String quote = tokens[1].trim(); //quote

            //get the words from the quote, split by whitespace
            String[] words = quote.split("\\s+");
            //create a new hash set the stores the unique words
            Set<String> uniqueWords = new HashSet<>();

            //put the words in the hash set. the hash set ensures only unique words, no duplicates
            for (String i : words) {
                uniqueWords.add(i.toLowerCase());
            }

            //set the name as the key and the word as the value
            //set each unique word as the key for each character
            character.set(name);
            for (String u : uniqueWords) {
                word.set(u);
                context.write(character, word);
            }
        }

    }

}
