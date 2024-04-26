import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordFreq implements Writable {

    private String word;
    private String next;
    private int freq;

    public WordFreq(String word, String next, int freq) {
        this.word = word;
        this.next = next;
        this.freq = freq;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(word);
        output.writeChars(next);
        output.writeInt(freq);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        word = input.readUTF();
        next = input.readUTF();
        freq = input.readInt();
    }
   
    @Override
    public String toString() {
        return "word: " + word + "next: " + next + "; frequency=" + freq;
    }

    public int compareTo(WordFreq o) {
        if (o == null) {
            return 1;
        }
        if (this.freq != o.freq) {
            return this.freq - o.freq;
        }
        if (!this.word.equals(o.word)) {
            return this.word.compareTo(o.word);
        }
        if (!this.next.equals(o.next)) {
            return this.next.compareTo(o.next);
        }
        return 0;
    }

}