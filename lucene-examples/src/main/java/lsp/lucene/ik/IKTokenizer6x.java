package lsp.lucene.ik;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;

public class IKTokenizer6x extends Tokenizer {

    private IKSegmenter _IKImplement;

    private final CharTermAttribute charTermAttribute;

    private final OffsetAttribute offsetAttribute;

    private final TypeAttribute typeAttribute;

    private int endPosition;

    public IKTokenizer6x(boolean useSmart) {
        super();
        offsetAttribute = addAttribute(OffsetAttribute.class);
        charTermAttribute = addAttribute(CharTermAttribute.class);
        typeAttribute = addAttribute(TypeAttribute.class);
        _IKImplement = new IKSegmenter(input, useSmart);
    }

    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();
        Lexeme nextLexem = _IKImplement.next();
        if (nextLexem != null) {
            charTermAttribute.append(nextLexem.getLexemeText());
            charTermAttribute.setLength(nextLexem.getLength());
            offsetAttribute.setOffset(nextLexem.getBeginPosition(), nextLexem.getEndPosition());
            endPosition = nextLexem.getEndPosition();
            typeAttribute.setType(nextLexem.getLexemeText());
            return true;
        }
        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        _IKImplement.reset(input);
    }

    @Override
    public void end() throws IOException {
        super.end();
        int finalOffset = correctOffset(this.endPosition);
        offsetAttribute.setOffset(finalOffset, finalOffset);
    }
}
