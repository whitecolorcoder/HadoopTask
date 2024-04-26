import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordFreq implements Writable {

    private String word;
    private int freq;

    public WordFreq(String word, int freq) {
        this.word = word;
        this.freq = freq;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(word);
        output.writeInt(freq);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        word = input.readUTF();
        freq = input.readInt();
    }

    @Override
    public String toString() {
        return "word: " + word + "; frequency=" + freq;
    }

}