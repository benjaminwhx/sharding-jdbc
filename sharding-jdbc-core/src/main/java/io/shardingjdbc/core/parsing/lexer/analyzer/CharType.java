/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.core.parsing.lexer.analyzer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * 字符类型
 * 
 * @author zhangliang 
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CharType {
    
    /**
     * input的结束字符
     * 代表十进制26
     */
    public static final byte EOI = 0x1A;

    public static void main(String[] args) {
        System.out.println(EOI);
    }
    /**
     * 校验是不是空白
     * 
     * @param ch to be adjusted char
     * @return is whitespace or not
     */
    public static boolean isWhitespace(final char ch) {
        // ch <= 32 && ch != 26 || ch >= 127 && ch <= 160
        return ch <= 0x20 && EOI != ch || ch >= 0x7F && ch <= 0xA0;
    }
    
    /**
     * 校验是不是input的end
     *
     * @param ch to be adjusted char
     * @return is end of input or not
     */
    public static boolean isEndOfInput(final char ch) {
        return ch == EOI;
    }
    
    /**
     * 校验是不是字母
     *
     * @param ch to be adjusted char
     * @return is alphabet or not
     */
    public static boolean isAlphabet(final char ch) {
        return ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z';
    }
    
    /**
     * 校验是不是数字
     *
     * @param ch to be adjusted char
     * @return is alphabet or not
     */
    public static boolean isDigital(final char ch) {
        return ch >= '0' && ch <= '9';
    }
    
    /**
     * 校验是不是语法
     *
     * @param ch to be adjusted char
     * @return is symbol or not
     */
    public static boolean isSymbol(final char ch) {
        return '(' == ch || ')' == ch || '[' == ch || ']' == ch || '{' == ch || '}' == ch || '+' == ch || '-' == ch || '*' == ch || '/' == ch || '%' == ch || '^' == ch || '=' == ch
                || '>' == ch || '<' == ch || '~' == ch || '!' == ch || '?' == ch || '&' == ch || '|' == ch || '.' == ch || ':' == ch || '#' == ch || ',' == ch || ';' == ch;
    }
}
