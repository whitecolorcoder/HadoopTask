import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import java.util.regex.Pattern;

public class NextAnalyzer extends StopwordAnalyzerBase {

    public NextAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        return new TokenStreamComponents(new PatternTokenizer(Pattern.compile("([^_\\W]+-*)+|[.!?]+",
                Pattern.UNICODE_CHARACTER_CLASS), 0));
    }
}