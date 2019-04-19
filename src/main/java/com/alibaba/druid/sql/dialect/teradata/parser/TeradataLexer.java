package com.alibaba.druid.sql.dialect.teradata.parser;

import com.alibaba.druid.sql.parser.Keywords;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;

import java.util.HashMap;
import java.util.Map;

public class TeradataLexer extends Lexer {
    public final static Keywords DEFAULT_TD_KEYWORDS;


    static {
        Map<String, Token> map = new HashMap<String, Token>();
        map.putAll(Keywords.DEFAULT_KEYWORDS.getKeywords());

        map.put("SEL", Token.SEL);
        map.put("LOCKING", Token.LOCKING);
        map.put("ACCESS", Token.ACCESS);

//        map.put("PARTITION", Token.PARTITION);
//        map.put("PARTITIONED", Token.PARTITIONED);
        map.put("RANGE_N", Token.RANGE_N);

        map.put("VOLATILE", Token.VOLATILE);
        map.put("MULTISET", Token.MULTISET);

        map.put("FORMAT", Token.FORMAT);

        DEFAULT_TD_KEYWORDS = new Keywords(map);
    }

    public TeradataLexer(String input) {
        super(input);
        super.keywods = DEFAULT_TD_KEYWORDS;
    }

//    public void scanComment() {
//        if (ch != '/' && ch != '-') {
//            throw new IllegalStateException();
//        }
//
//        mark = pos;
//        bufPos = 0;
//        scanChar();
//
//        // /*+ */
//        if (ch == '*') {
//            scanChar();
//            bufPos++;
//
//            while (ch == ' ') {
//                scanChar();
//                bufPos++;
//            }
//
//            boolean isHint = false;
//            int startHintSp = bufPos + 1;
//            if (ch == '+') {
//                isHint = true;
//                scanChar();
//                bufPos++;
//            }
//
//            for (; ; ) {
//                if (ch == '*' && charAt(pos + 1) == '/') {
//                    bufPos += 2;
//                    scanChar();
//                    scanChar();
//                    break;
//                }
//
//                scanChar();
//                bufPos++;
//            }
//
//            if (isHint) {
//                stringVal = subString(mark + startHintSp, (bufPos - startHintSp) - 1);
//                token = Token.HINT;
//            } else {
//                stringVal = subString(mark, bufPos);
//                token = Token.MULTI_LINE_COMMENT;
//                if (keepComments) {
//                    addComment(stringVal);
//                }
//            }
//
//            if (token != Token.HINT && !isAllowComment()) {
//                throw new NotAllowCommentException();
//            }
//
//            return;
//        }
//
//        if (!isAllowComment()) {
//            throw new NotAllowCommentException();
//        }
//
//        if (ch == '/' || ch == '-') {
//            scanChar();
//            bufPos++;
//
//            for (; ; ) {
//                if (ch == '\r') {
//                    if (charAt(pos + 1) == '\n') {
//                        bufPos += 2;
//                        scanChar();
//                        break;
//                    }
//                    bufPos++;
//                    break;
//                } else if (ch == EOI) {
//                    break;
//                }
//
//                if (ch == '\n') {
//                    scanChar();
//                    bufPos++;
//                    break;
//                }
//
//                scanChar();
//                bufPos++;
//            }
//
//            stringVal = subString(mark + 1, bufPos);
//            token = Token.LINE_COMMENT;
//            if (keepComments) {
//                addComment(stringVal);
//            }
//            endOfComment = isEOF();
//            return;
//        }
//    }
}
