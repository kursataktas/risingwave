// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Tokenizer
//!
//! The tokenizer (a.k.a. lexer) converts a string into a sequence of tokens.
//!
//! The tokens then form the input for the parser, which outputs an Abstract Syntax Tree (AST).

#[cfg(not(feature = "std"))]
use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt;
use core::fmt::Debug;
use core::iter::Peekable;
use core::str::Chars;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::{CstyleEscapedString, DollarQuotedString};
use crate::keywords::{Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX};

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF,
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
    /// An unsigned numeric literal
    Number(String),
    /// A character that could not be tokenized
    Char(char),
    /// Single quoted string: i.e: 'string'
    SingleQuotedString(String),
    /// Dollar quoted string: i.e: $$string$$ or $tag_name$string$tag_name$
    DollarQuotedString(DollarQuotedString),
    /// Single quoted string with c-style escapes: i.e: E'string'
    CstyleEscapesString(CstyleEscapedString),
    /// "National" string literal: i.e: N'string'
    NationalStringLiteral(String),
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral(String),
    /// Parameter symbols: i.e:  $1, $2
    Parameter(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(Whitespace),
    /// Double equals sign `==`
    DoubleEq,
    /// Equality operator `=`
    Eq,
    /// Not Equals operator `<>` (or `!=` in some dialects)
    Neq,
    /// Less Than operator `<`
    Lt,
    /// Greater Than operator `>`
    Gt,
    /// Less Than Or Equals operator `<=`
    LtEq,
    /// Greater Than Or Equals operator `>=`
    GtEq,
    /// Spaceship operator <=>
    Spaceship,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mul,
    /// Division operator `/`
    Div,
    /// Modulo Operator `%`
    Mod,
    /// String concatenation `||`
    Concat,
    /// Left parenthesis `(`
    LParen,
    /// Right parenthesis `)`
    RParen,
    /// Period (used for compound identifiers or projections into nested types)
    Period,
    /// Colon `:`
    Colon,
    /// DoubleColon `::` (used for casting in postgresql)
    DoubleColon,
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon,
    /// Backslash `\` used in terminating the COPY payload with `\.`
    Backslash,
    /// Left bracket `[`
    LBracket,
    /// Right bracket `]`
    RBracket,
    /// Ampersand `&`
    Ampersand,
    /// Pipe `|`
    Pipe,
    /// Caret `^`
    Caret,
    /// Prefix `^@`
    Prefix,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
    /// Right Arrow `=>`
    RArrow,
    /// Sharp `#` used for PostgreSQL Bitwise XOR operator
    Sharp,
    /// Tilde `~` used for PostgreSQL Bitwise NOT operator or case sensitive match regular
    /// expression operator
    Tilde,
    /// `~*` , a case insensitive match regular expression operator in PostgreSQL
    TildeAsterisk,
    /// `!~` , a case sensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTilde,
    /// `!~*` , a case insensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTildeAsterisk,
    /// `~~`, a case sensitive LIKE expression operator in PostgreSQL
    DoubleTilde,
    /// `~~*` , a case insensitive ILIKE regular expression operator in PostgreSQL
    DoubleTildeAsterisk,
    /// `!~~` , a case sensitive NOT LIKE regular expression operator in PostgreSQL
    ExclamationMarkDoubleTilde,
    /// `!~~*` , a case insensitive NOT ILIKE regular expression operator in PostgreSQL
    ExclamationMarkDoubleTildeAsterisk,
    /// `<<`, a bitwise shift left operator in PostgreSQL
    ShiftLeft,
    /// `>>`, a bitwise shift right operator in PostgreSQL
    ShiftRight,
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    ExclamationMark,
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    DoubleExclamationMark,
    /// AtSign `@` used for PostgreSQL abs operator
    AtSign,
    /// `|/`, a square root math operator in PostgreSQL
    PGSquareRoot,
    /// `||/` , a cube root math operator in PostgreSQL
    PGCubeRoot,
    /// `->`, access JSON object field or array element in PostgreSQL
    Arrow,
    /// `->>`, access JSON object field or array element as text in PostgreSQL
    LongArrow,
    /// `#>`, extract JSON sub-object at the specified path in PostgreSQL
    HashArrow,
    /// `#>>`, extract JSON sub-object at the specified path as text in PostgreSQL
    HashLongArrow,
    /// `#-`, delete a key from a JSON object in PostgreSQL
    HashMinus,
    /// `@>`, does the left JSON value contain the right JSON path/value entries at the top level
    AtArrow,
    /// `<@`, does the right JSON value contain the left JSON path/value entries at the top level
    ArrowAt,
    /// `?`, does the string exist as a top-level key within the JSON value
    QuestionMark,
    /// `?|`, do any of the strings exist as top-level keys or array elements?
    QuestionMarkPipe,
    /// `?&`, do all of the strings exist as top-level keys or array elements?
    QuestionMarkAmpersand,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Word(ref w) => write!(f, "{}", w),
            Token::Number(ref n) => write!(f, "{}", n),
            Token::Char(ref c) => write!(f, "{}", c),
            Token::SingleQuotedString(ref s) => write!(f, "'{}'", s),
            Token::DollarQuotedString(ref s) => write!(f, "{}", s),
            Token::NationalStringLiteral(ref s) => write!(f, "N'{}'", s),
            Token::HexStringLiteral(ref s) => write!(f, "X'{}'", s),
            Token::CstyleEscapesString(ref s) => write!(f, "E'{}'", s),
            Token::Parameter(ref s) => write!(f, "${}", s),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{}", ws),
            Token::DoubleEq => f.write_str("=="),
            Token::Spaceship => f.write_str("<=>"),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("<>"),
            Token::Lt => f.write_str("<"),
            Token::Gt => f.write_str(">"),
            Token::LtEq => f.write_str("<="),
            Token::GtEq => f.write_str(">="),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mul => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::Concat => f.write_str("||"),
            Token::Mod => f.write_str("%"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Period => f.write_str("."),
            Token::Colon => f.write_str(":"),
            Token::DoubleColon => f.write_str("::"),
            Token::SemiColon => f.write_str(";"),
            Token::Backslash => f.write_str("\\"),
            Token::LBracket => f.write_str("["),
            Token::RBracket => f.write_str("]"),
            Token::Ampersand => f.write_str("&"),
            Token::Caret => f.write_str("^"),
            Token::Prefix => f.write_str("^@"),
            Token::Pipe => f.write_str("|"),
            Token::LBrace => f.write_str("{"),
            Token::RBrace => f.write_str("}"),
            Token::RArrow => f.write_str("=>"),
            Token::Sharp => f.write_str("#"),
            Token::ExclamationMark => f.write_str("!"),
            Token::DoubleExclamationMark => f.write_str("!!"),
            Token::Tilde => f.write_str("~"),
            Token::TildeAsterisk => f.write_str("~*"),
            Token::ExclamationMarkTilde => f.write_str("!~"),
            Token::ExclamationMarkTildeAsterisk => f.write_str("!~*"),
            Token::DoubleTilde => f.write_str("~~"),
            Token::DoubleTildeAsterisk => f.write_str("~~*"),
            Token::ExclamationMarkDoubleTilde => f.write_str("!~~"),
            Token::ExclamationMarkDoubleTildeAsterisk => f.write_str("!~~*"),
            Token::AtSign => f.write_str("@"),
            Token::ShiftLeft => f.write_str("<<"),
            Token::ShiftRight => f.write_str(">>"),
            Token::PGSquareRoot => f.write_str("|/"),
            Token::PGCubeRoot => f.write_str("||/"),
            Token::Arrow => f.write_str("->"),
            Token::LongArrow => f.write_str("->>"),
            Token::HashArrow => f.write_str("#>"),
            Token::HashLongArrow => f.write_str("#>>"),
            Token::HashMinus => f.write_str("#-"),
            Token::AtArrow => f.write_str("@>"),
            Token::ArrowAt => f.write_str("<@"),
            Token::QuestionMark => f.write_str("?"),
            Token::QuestionMarkPipe => f.write_str("?|"),
            Token::QuestionMarkAmpersand => f.write_str("?&"),
        }
    }
}

impl Token {
    pub fn make_keyword(keyword: &str) -> Self {
        Token::make_word(keyword, None)
    }

    pub fn make_word(word: &str, quote_style: Option<char>) -> Self {
        let word_uppercase = word.to_uppercase();
        Token::Word(Word {
            value: word.to_string(),
            quote_style,
            keyword: if quote_style.is_none() {
                let keyword = ALL_KEYWORDS.binary_search(&word_uppercase.as_str());
                keyword.map_or(Keyword::NoKeyword, |x| ALL_KEYWORDS_INDEX[x])
            } else {
                Keyword::NoKeyword
            },
        })
    }

    pub fn with_location(self, location: Location) -> TokenWithLocation {
        TokenWithLocation::new(self, location.line, location.column)
    }
}

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Word {
    /// The value of the token, without the enclosing quotes, and with the
    /// escape sequences (if any) processed (TODO: escapes are not handled)
    pub value: String,
    /// An identifier can be "quoted" (&lt;delimited identifier> in ANSI parlance).
    /// The standard and most implementations allow using double quotes for this,
    /// but some implementations support other quoting styles as well (e.g. \[MS SQL])
    pub quote_style: Option<char>,
    /// If the word was not quoted and it matched one of the known keywords,
    /// this will have one of the values from dialect::keywords, otherwise empty
    pub keyword: Keyword,
}

impl fmt::Display for Word {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote_style {
            Some(s) if s == '"' || s == '[' || s == '`' => {
                write!(f, "{}{}{}", s, self.value, Word::matching_end_quote(s))
            }
            None => f.write_str(&self.value),
            _ => panic!("Unexpected quote_style!"),
        }
    }
}

impl Word {
    fn matching_end_quote(ch: char) -> char {
        match ch {
            '"' => '"', // ANSI and most dialects
            '[' => ']', // MS SQL
            '`' => '`', // MySQL
            _ => panic!("unexpected quoting style!"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Whitespace {
    Space,
    Newline,
    Tab,
    SingleLineComment { comment: String, prefix: String },
    MultiLineComment(String),
}

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Whitespace::Space => f.write_str(" "),
            Whitespace::Newline => f.write_str("\n"),
            Whitespace::Tab => f.write_str("\t"),
            Whitespace::SingleLineComment { prefix, comment } => write!(f, "{}{}", prefix, comment),
            Whitespace::MultiLineComment(s) => write!(f, "/*{}*/", s),
        }
    }
}

/// Location in input string
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Location {
    /// Line number, starting from 1
    pub line: u64,
    /// Line column, starting from 1
    pub column: u64,
}

/// A [Token] with [Location] attached to it
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TokenWithLocation {
    pub token: Token,
    pub location: Location,
}

impl TokenWithLocation {
    pub fn new(token: Token, line: u64, column: u64) -> TokenWithLocation {
        TokenWithLocation {
            token,
            location: Location { line, column },
        }
    }

    pub fn wrap(token: Token) -> TokenWithLocation {
        TokenWithLocation::new(token, 0, 0)
    }
}

impl PartialEq<Token> for TokenWithLocation {
    fn eq(&self, other: &Token) -> bool {
        &self.token == other
    }
}

impl PartialEq<TokenWithLocation> for Token {
    fn eq(&self, other: &TokenWithLocation) -> bool {
        self == &other.token
    }
}

impl fmt::Display for TokenWithLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.token == Token::EOF {
            write!(f, "EOF at the end")
        } else {
            write!(
                f,
                "{} at line:{}, column:{}",
                self.token, self.location.line, self.location.column
            )
        }
    }
}

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError {
    pub message: String,
    pub line: u64,
    pub col: u64,
}

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at Line: {}, Column {}",
            self.message, self.line, self.col
        )
    }
}

#[cfg(feature = "std")]
impl std::error::Error for TokenizerError {}

/// SQL Tokenizer
pub struct Tokenizer<'a> {
    query: &'a str,
    line: u64,
    col: u64,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(query: &'a str) -> Self {
        Self {
            query,
            line: 1,
            col: 1,
        }
    }

    /// Tokenize the statement and produce a vector of tokens with locations.
    pub fn tokenize_with_location(&mut self) -> Result<Vec<TokenWithLocation>, TokenizerError> {
        let mut peekable = self.query.chars().peekable();

        let mut tokens: Vec<TokenWithLocation> = vec![];

        while let Some(token) = self.next_token(&mut peekable)? {
            match &token {
                Token::Whitespace(Whitespace::Newline) => {
                    self.line += 1;
                    self.col = 1;
                }

                Token::Whitespace(Whitespace::Tab) => self.col += 4,
                Token::Word(w) if w.quote_style.is_none() => self.col += w.value.len() as u64,
                Token::Word(w) if w.quote_style.is_some() => self.col += w.value.len() as u64 + 2,
                Token::Number(s) => self.col += s.len() as u64,
                Token::SingleQuotedString(s) => self.col += s.len() as u64,
                _ => self.col += 1,
            }

            let token_with_location = TokenWithLocation::new(token, self.line, self.col);

            tokens.push(token_with_location);
        }
        Ok(tokens)
    }

    /// Tokenize the statement and produce a vector of tokens without locations.
    #[allow(dead_code)]
    fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        self.tokenize_with_location()
            .map(|v| v.into_iter().map(|t| t.token).collect())
    }

    /// Get the next token or return None
    fn next_token(&self, chars: &mut Peekable<Chars<'_>>) -> Result<Option<Token>, TokenizerError> {
        match chars.peek() {
            Some(&ch) => match ch {
                ' ' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Space)),
                '\t' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Tab)),
                '\n' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Newline)),
                '\r' => {
                    // Emit a single Whitespace::Newline token for \r and \r\n
                    chars.next();
                    if let Some('\n') = chars.peek() {
                        chars.next();
                    }
                    Ok(Some(Token::Whitespace(Whitespace::Newline)))
                }
                'N' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_single_quoted_string(chars)?;
                            Ok(Some(Token::NationalStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "N"
                            let s = self.tokenize_word('N', chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                x @ 'e' | x @ 'E' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // E'...' - a <character string literal>
                            let s = self.tokenize_single_quoted_string_with_escape(chars)?;
                            Ok(Some(Token::CstyleEscapesString(s)))
                        }
                        _ => {
                            // regular identifier starting with an "E"
                            let s = self.tokenize_word(x, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // The spec only allows an uppercase 'X' to introduce a hex
                // string, but PostgreSQL, at least, allows a lowercase 'x' too.
                x @ 'x' | x @ 'X' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // X'...' - a <binary string literal>
                            let s = self.tokenize_single_quoted_string(chars)?;
                            Ok(Some(Token::HexStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "X"
                            let s = self.tokenize_word(x, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // identifier or keyword
                ch if is_identifier_start(ch) => {
                    chars.next(); // consume the first char
                    let s = self.tokenize_word(ch, chars);

                    if s.chars().all(|x| x.is_ascii_digit() || x == '.') {
                        let mut s = peeking_take_while(&mut s.chars().peekable(), |ch| {
                            ch.is_ascii_digit() || ch == '.'
                        });
                        let s2 = peeking_take_while(chars, |ch| ch.is_ascii_digit() || ch == '.');
                        s += s2.as_str();
                        return Ok(Some(Token::Number(s)));
                    }
                    Ok(Some(Token::make_word(&s, None)))
                }
                // string
                '\'' => {
                    let s = self.tokenize_single_quoted_string(chars)?;

                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // delimited (quoted) identifier
                quote_start if is_delimited_identifier_start(quote_start) => {
                    chars.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let s = peeking_take_while(chars, |ch| ch != quote_end);
                    if chars.next() == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(format!(
                            "Expected close delimiter '{}' before EOF.",
                            quote_end
                        ))
                    }
                }
                // numbers and period
                '0'..='9' | '.' => {
                    let mut s = peeking_take_while(chars, |ch| ch.is_ascii_digit());

                    // match binary literal that starts with 0x
                    if s == "0" && chars.peek() == Some(&'x') {
                        chars.next();
                        let s2 = peeking_take_while(
                            chars,
                            |ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'),
                        );
                        return Ok(Some(Token::HexStringLiteral(s2)));
                    }

                    // match one period
                    if let Some('.') = chars.peek() {
                        s.push('.');
                        chars.next();
                    }
                    s += &peeking_take_while(chars, |ch| ch.is_ascii_digit());

                    // No number -> Token::Period
                    if s == "." {
                        return Ok(Some(Token::Period));
                    }

                    match chars.peek() {
                        // Number is a scientific number (1e6)
                        Some('e') | Some('E') => {
                            s.push('e');
                            chars.next();

                            if let Some('-') = chars.peek() {
                                s.push('-');
                                chars.next();
                            }
                            s += &peeking_take_while(chars, |ch| ch.is_ascii_digit());
                            return Ok(Some(Token::Number(s)));
                        }
                        // Not a scientific number
                        _ => {}
                    };
                    Ok(Some(Token::Number(s)))
                }
                // punctuation
                '(' => self.consume_and_return(chars, Token::LParen),
                ')' => self.consume_and_return(chars, Token::RParen),
                ',' => self.consume_and_return(chars, Token::Comma),
                // operators
                '-' => {
                    chars.next(); // consume the '-'
                    match chars.peek() {
                        Some('-') => {
                            chars.next(); // consume the second '-', starting a single-line comment
                            let comment = self.tokenize_single_line_comment(chars);
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                                prefix: "--".to_owned(),
                                comment,
                            })))
                        }
                        Some('>') => {
                            chars.next(); // consume first '>'
                            match chars.peek() {
                                Some('>') => {
                                    chars.next(); // consume second '>'
                                    Ok(Some(Token::LongArrow))
                                }
                                _ => Ok(Some(Token::Arrow)),
                            }
                        }
                        // a regular '-' operator
                        _ => Ok(Some(Token::Minus)),
                    }
                }
                '/' => {
                    chars.next(); // consume the '/'
                    match chars.peek() {
                        Some('*') => {
                            chars.next(); // consume the '*', starting a multi-line comment
                            self.tokenize_multiline_comment(chars)
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus),
                '*' => self.consume_and_return(chars, Token::Mul),
                '%' => self.consume_and_return(chars, Token::Mod),
                '|' => {
                    chars.next(); // consume the '|'
                    match chars.peek() {
                        Some('/') => self.consume_and_return(chars, Token::PGSquareRoot),
                        Some('|') => {
                            chars.next(); // consume the second '|'
                            match chars.peek() {
                                Some('/') => self.consume_and_return(chars, Token::PGCubeRoot),
                                _ => Ok(Some(Token::Concat)),
                            }
                        }
                        // Bitshift '|' operator
                        _ => Ok(Some(Token::Pipe)),
                    }
                }
                '=' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::RArrow),
                        _ => Ok(Some(Token::Eq)),
                    }
                }
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::Neq),
                        Some('!') => self.consume_and_return(chars, Token::DoubleExclamationMark),
                        Some('~') => {
                            chars.next();
                            match chars.peek() {
                                Some('~') => {
                                    chars.next();
                                    match chars.peek() {
                                        Some('*') => self.consume_and_return(
                                            chars,
                                            Token::ExclamationMarkDoubleTildeAsterisk,
                                        ),
                                        _ => Ok(Some(Token::ExclamationMarkDoubleTilde)),
                                    }
                                }
                                Some('*') => self
                                    .consume_and_return(chars, Token::ExclamationMarkTildeAsterisk),
                                _ => Ok(Some(Token::ExclamationMarkTilde)),
                            }
                        }
                        _ => Ok(Some(Token::ExclamationMark)),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => {
                            chars.next();
                            match chars.peek() {
                                Some('>') => self.consume_and_return(chars, Token::Spaceship),
                                _ => Ok(Some(Token::LtEq)),
                            }
                        }
                        Some('>') => self.consume_and_return(chars, Token::Neq),
                        Some('<') => self.consume_and_return(chars, Token::ShiftLeft),
                        Some('@') => self.consume_and_return(chars, Token::ArrowAt),
                        _ => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::GtEq),
                        Some('>') => self.consume_and_return(chars, Token::ShiftRight),
                        _ => Ok(Some(Token::Gt)),
                    }
                }
                ':' => {
                    chars.next();
                    match chars.peek() {
                        Some(':') => self.consume_and_return(chars, Token::DoubleColon),
                        _ => Ok(Some(Token::Colon)),
                    }
                }
                '$' => Ok(Some(self.tokenize_dollar_preceded_value(chars)?)),
                ';' => self.consume_and_return(chars, Token::SemiColon),
                '\\' => self.consume_and_return(chars, Token::Backslash),
                '[' => self.consume_and_return(chars, Token::LBracket),
                ']' => self.consume_and_return(chars, Token::RBracket),
                '&' => self.consume_and_return(chars, Token::Ampersand),
                '^' => {
                    chars.next();
                    match chars.peek() {
                        Some('@') => self.consume_and_return(chars, Token::Prefix),
                        _ => Ok(Some(Token::Caret)),
                    }
                }
                '{' => self.consume_and_return(chars, Token::LBrace),
                '}' => self.consume_and_return(chars, Token::RBrace),
                '~' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('~') => {
                            chars.next();
                            match chars.peek() {
                                Some('*') => {
                                    self.consume_and_return(chars, Token::DoubleTildeAsterisk)
                                }
                                _ => Ok(Some(Token::DoubleTilde)),
                            }
                        }
                        Some('*') => self.consume_and_return(chars, Token::TildeAsterisk),
                        _ => Ok(Some(Token::Tilde)),
                    }
                }
                '#' => {
                    chars.next(); // consume the '#'
                    match chars.peek() {
                        Some('-') => self.consume_and_return(chars, Token::HashMinus),
                        Some('>') => {
                            chars.next(); // consume first '>'
                            match chars.peek() {
                                Some('>') => {
                                    chars.next(); // consume second '>'
                                    Ok(Some(Token::HashLongArrow))
                                }
                                _ => Ok(Some(Token::HashArrow)),
                            }
                        }
                        // a regular '#' operator
                        _ => Ok(Some(Token::Sharp)),
                    }
                }
                '@' => {
                    chars.next(); // consume the '@'
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::AtArrow),
                        // a regular '@' operator
                        _ => Ok(Some(Token::AtSign)),
                    }
                }
                '?' => {
                    chars.next(); // consume the '?'
                    match chars.peek() {
                        Some('|') => self.consume_and_return(chars, Token::QuestionMarkPipe),
                        Some('&') => self.consume_and_return(chars, Token::QuestionMarkAmpersand),
                        // a regular '?' operator
                        _ => Ok(Some(Token::QuestionMark)),
                    }
                }
                other => self.consume_and_return(chars, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    /// Tokenize dollar preceded value (i.e: a string/placeholder)
    fn tokenize_dollar_preceded_value(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<Token, TokenizerError> {
        let mut s = String::new();
        let mut value = String::new();

        chars.next();

        if let Some('$') = chars.peek() {
            chars.next();

            let mut is_terminated = false;
            let mut prev: Option<char> = None;

            while let Some(&ch) = chars.peek() {
                if prev == Some('$') {
                    if ch == '$' {
                        chars.next();
                        is_terminated = true;
                        break;
                    } else {
                        s.push('$');
                        s.push(ch);
                    }
                } else if ch != '$' {
                    s.push(ch);
                }

                prev = Some(ch);
                chars.next();
            }

            return if chars.peek().is_none() && !is_terminated {
                self.tokenizer_error("Unterminated dollar-quoted string")
            } else {
                Ok(Token::DollarQuotedString(DollarQuotedString {
                    value: s,
                    tag: None,
                }))
            };
        } else {
            value.push_str(&peeking_take_while(chars, |ch| {
                ch.is_alphanumeric() || ch == '_'
            }));

            if let Some('$') = chars.peek() {
                chars.next();
                s.push_str(&peeking_take_while(chars, |ch| ch != '$'));

                match chars.peek() {
                    Some('$') => {
                        chars.next();
                        for (_, c) in value.chars().enumerate() {
                            let next_char = chars.next();
                            if Some(c) != next_char {
                                return self.tokenizer_error(format!(
                                    "Unterminated dollar-quoted string at or near \"{}\"",
                                    value
                                ));
                            }
                        }

                        if let Some('$') = chars.peek() {
                            chars.next();
                        } else {
                            return self
                                .tokenizer_error("Unterminated dollar-quoted string, expected $");
                        }
                    }
                    _ => {
                        return self.tokenizer_error("Unterminated dollar-quoted, expected $");
                    }
                }
            } else {
                return Ok(Token::Parameter(value));
            }
        }

        Ok(Token::DollarQuotedString(DollarQuotedString {
            value: s,
            tag: if value.is_empty() { None } else { Some(value) },
        }))
    }

    fn tokenizer_error<R>(&self, message: impl Into<String>) -> Result<R, TokenizerError> {
        Err(TokenizerError {
            message: message.into(),
            col: self.col,
            line: self.line,
        })
    }

    // Consume characters until newline
    fn tokenize_single_line_comment(&self, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut comment = peeking_take_while(chars, |ch| ch != '\n');
        if let Some(ch) = chars.next() {
            assert_eq!(ch, '\n');
            comment.push(ch);
        }
        comment
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(chars, is_identifier_part));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_single_quoted_string(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();
        chars.next(); // consume the opening quote

        // slash escaping is specific to MySQL dialect
        let mut is_escaped = false;
        while let Some(&ch) = chars.peek() {
            match ch {
                '\'' => {
                    chars.next(); // consume
                    if is_escaped {
                        s.push(ch);
                        is_escaped = false;
                    } else if chars.peek().map(|c| *c == '\'').unwrap_or(false) {
                        s.push(ch);
                        chars.next();
                    } else {
                        return Ok(s);
                    }
                }
                '\\' => {
                    s.push(ch);
                    chars.next();
                }
                _ => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        self.tokenizer_error("Unterminated string literal")
    }

    /// Read a single qutoed string with escape
    fn tokenize_single_quoted_string_with_escape(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<CstyleEscapedString, TokenizerError> {
        let mut terminated = false;
        let mut s = String::new();
        chars.next(); // consume the opening quote

        while let Some(&ch) = chars.peek() {
            match ch {
                '\'' => {
                    chars.next(); // consume
                    if chars.peek().map(|c| *c == '\'').unwrap_or(false) {
                        s.push('\\');
                        s.push(ch);
                        chars.next();
                    } else {
                        terminated = true;
                        break;
                    }
                }
                '\\' => {
                    s.push(ch);
                    chars.next();
                    if chars
                        .peek()
                        .map(|c| *c == '\'' || *c == '\\')
                        .unwrap_or(false)
                    {
                        s.push(chars.next().unwrap());
                    }
                }
                _ => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }

        if !terminated {
            return self.tokenizer_error("Unterminated string literal");
        }

        let unescaped = match Self::unescape_c_style(&s) {
            Ok(unescaped) => unescaped,
            Err(e) => return self.tokenizer_error(e),
        };

        Ok(CstyleEscapedString {
            value: unescaped,
            raw: s,
        })
    }

    /// Helper function used to convert string with c-style escapes into a normal string
    /// e.g. 'hello\x3fworld' -> 'hello?world'
    ///
    /// Detail of c-style escapes refer from:
    /// <https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-UESCAPE:~:text=4.1.2.2.%C2%A0String%20Constants%20With%20C%2DStyle%20Escapes>
    fn unescape_c_style(s: &str) -> Result<String, String> {
        fn hex_byte_process(
            chars: &mut Peekable<Chars<'_>>,
            res: &mut String,
            len: usize,
            default_char: char,
        ) -> Result<(), String> {
            let mut unicode_seq: String = String::with_capacity(len);
            for _ in 0..len {
                if let Some(c) = chars.peek() && c.is_ascii_hexdigit() {
                    unicode_seq.push(chars.next().unwrap());
                } else {
                    break;
                }
            }

            if unicode_seq.is_empty() && len == 2 {
                res.push(default_char);
                return Ok(());
            } else if unicode_seq.len() < len && len != 2 {
                return Err("invalid unicode sequence: must be \\uXXXX or \\UXXXXXXXX".to_string());
            }

            if len == 2 {
                let number = [u8::from_str_radix(&unicode_seq, 16)
                    .map_err(|e| format!("invalid unicode sequence: {}", e))?];

                res.push(
                    std::str::from_utf8(&number)
                        .map_err(|err| format!("invalid unicode sequence: {}", err))?
                        .chars()
                        .next()
                        .unwrap(),
                );
            } else {
                let number = u32::from_str_radix(&unicode_seq, 16)
                    .map_err(|e| format!("invalid unicode sequence: {}", e))?;
                res.push(
                    char::from_u32(number)
                        .ok_or_else(|| format!("invalid unicode sequence: {}", unicode_seq))?,
                );
            }
            Ok(())
        }

        fn octal_byte_process(
            chars: &mut Peekable<Chars<'_>>,
            res: &mut String,
            digit: char,
        ) -> Result<(), String> {
            let mut unicode_seq: String = String::with_capacity(3);
            unicode_seq.push(digit);
            for _ in 0..2 {
                if let Some(c) = chars.peek() && matches!(*c, '0'..='7') {
                    unicode_seq.push(chars.next().unwrap());
                } else {
                    break;
                }
            }

            let number = [u8::from_str_radix(&unicode_seq, 8)
                .map_err(|e| format!("invalid unicode sequence: {}", e))?];

            res.push(
                std::str::from_utf8(&number)
                    .map_err(|err| format!("invalid unicode sequence: {}", err))?
                    .chars()
                    .next()
                    .unwrap(),
            );
            Ok(())
        }

        let mut chars = s.chars().peekable();
        let mut res = String::with_capacity(s.len());

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    None => {
                        return Err("unterminated escape sequence".to_string());
                    }
                    Some(next_c) => match next_c {
                        'b' => res.push('\u{08}'),
                        'f' => res.push('\u{0C}'),
                        'n' => res.push('\n'),
                        'r' => res.push('\r'),
                        't' => res.push('\t'),
                        'x' => hex_byte_process(&mut chars, &mut res, 2, 'x')?,
                        'u' => hex_byte_process(&mut chars, &mut res, 4, 'u')?,
                        'U' => hex_byte_process(&mut chars, &mut res, 8, 'U')?,
                        digit @ '0'..='7' => octal_byte_process(&mut chars, &mut res, digit)?,
                        _ => res.push(next_c),
                    },
                }
            } else {
                res.push(c);
            }
        }

        Ok(res)
    }

    fn tokenize_multiline_comment(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();

        let mut nested = 1;
        let mut last_ch = ' ';

        loop {
            match chars.next() {
                Some(ch) => {
                    if last_ch == '/' && ch == '*' {
                        nested += 1;
                    } else if last_ch == '*' && ch == '/' {
                        nested -= 1;
                        if nested == 0 {
                            s.pop();
                            break Ok(Some(Token::Whitespace(Whitespace::MultiLineComment(s))));
                        }
                    }
                    s.push(ch);
                    last_ch = ch;
                }
                None => break self.tokenizer_error("Unexpected EOF while in a multi-line comment"),
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn consume_and_return(
        &self,
        chars: &mut Peekable<Chars<'_>>,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
        chars.next();
        Ok(Some(t))
    }
}

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(
    chars: &mut Peekable<Chars<'_>>,
    mut predicate: impl FnMut(char) -> bool,
) -> String {
    let mut s = String::new();
    while let Some(&ch) = chars.peek() {
        if predicate(ch) {
            chars.next(); // consume
            s.push(ch);
        } else {
            break;
        }
    }
    s
}

/// Determine if a character starts a quoted identifier. The default
/// implementation, accepting "double quoted" ids is both ANSI-compliant
/// and appropriate for most dialects (with the notable exception of
/// MySQL, MS SQL, and sqlite). You can accept one of characters listed
/// in `Word::matching_end_quote` here
fn is_delimited_identifier_start(ch: char) -> bool {
    ch == '"'
}

/// Determine if a character is a valid start character for an unquoted identifier
fn is_identifier_start(ch: char) -> bool {
    // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    // We don't yet support identifiers beginning with "letters with
    // diacritical marks and non-Latin letters"
    ch.is_ascii_alphabetic() || ch == '_'
}

/// Determine if a character is a valid unquoted identifier character
fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '$' || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizer_error_impl() {
        let err = TokenizerError {
            message: "test".into(),
            line: 1,
            col: 1,
        };
        #[cfg(feature = "std")]
        {
            use std::error::Error;
            assert!(err.source().is_none());
        }
        assert_eq!(err.to_string(), "test at Line: 1, Column 1");
    }

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_select_float() {
        let sql = String::from("SELECT .1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from(".1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_scalar_function() {
        let sql = String::from("SELECT sqrt(1)");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("sqrt", None),
            Token::LParen,
            Token::Number(String::from("1")),
            Token::RParen,
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_string_concat() {
        let sql = String::from("SELECT 'a' || 'b'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("a")),
            Token::Whitespace(Whitespace::Space),
            Token::Concat,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("b")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_bitwise_op() {
        let sql = String::from("SELECT one | two ^ three");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("one", None),
            Token::Whitespace(Whitespace::Space),
            Token::Pipe,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("two", None),
            Token::Whitespace(Whitespace::Space),
            Token::Caret,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("three", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_logical_xor() {
        let sql =
            String::from("SELECT true XOR true, false XOR false, true XOR false, false XOR true");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("id", None),
            Token::Whitespace(Whitespace::Space),
            Token::Eq,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("LIMIT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("5")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_explain_select() {
        let sql = String::from("EXPLAIN SELECT * FROM customer WHERE id = 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("EXPLAIN"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("id", None),
            Token::Whitespace(Whitespace::Space),
            Token::Eq,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_explain_analyze_select() {
        let sql = String::from("EXPLAIN ANALYZE SELECT * FROM customer WHERE id = 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("EXPLAIN"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("ANALYZE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("id", None),
            Token::Whitespace(Whitespace::Space),
            Token::Eq,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_predicate() {
        let sql = String::from("SELECT * FROM customer WHERE salary != 'Not Provided'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("salary", None),
            Token::Whitespace(Whitespace::Space),
            Token::Neq,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("Not Provided")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string() {
        let sql = String::from("\nمصطفىh");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        // println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_newline_in_string_literal() {
        let sql = String::from("'foo\r\nbar\nbaz'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::SingleQuotedString("foo\r\nbar\nbaz".to_string())];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_unterminated_string_literal() {
        let sql = String::from("select 'foo");
        let mut tokenizer = Tokenizer::new(&sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(TokenizerError {
                message: "Unterminated string literal".to_string(),
                line: 1,
                col: 8,
            })
        );
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        // println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::Newline),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("table"),
            Token::Whitespace(Whitespace::Tab),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_right_arrow() {
        let sql = String::from("FUNCTION(key=>value)");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_word("FUNCTION", None),
            Token::LParen,
            Token::make_word("key", None),
            Token::RArrow,
            Token::make_word("value", None),
            Token::RParen,
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_word("a", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("IS"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("NULL"),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment() {
        let sql = String::from("0--this is a comment\n1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::SingleLineComment {
                prefix: "--".to_string(),
                comment: "this is a comment\n".to_string(),
            }),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment_at_eof() {
        let sql = String::from("--this is a comment");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "--".to_string(),
            comment: "this is a comment".to_string(),
        })];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment() {
        let sql = String::from("0/*multi-line\n* /comment*/1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* /comment".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_nested_multiline_comment() {
        let sql = String::from("0/*multi-line\n* \n/* comment \n /*comment*/*/ */ /comment*/1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* \n/* comment \n /*comment*/*/ */ /comment".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment_with_even_asterisks() {
        let sql = String::from("\n/** Comment **/\n");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::MultiLineComment("* Comment *".to_string())),
            Token::Whitespace(Whitespace::Newline),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_mismatched_quotes() {
        let sql = String::from("\"foo");
        let mut tokenizer = Tokenizer::new(&sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(TokenizerError {
                message: "Expected close delimiter '\"' before EOF.".to_string(),
                line: 1,
                col: 1,
            })
        );
    }

    #[test]
    fn tokenize_newlines() {
        let sql = String::from("line1\nline2\rline3\r\nline4\r");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_word("line1", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line2", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line3", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line4", None),
            Token::Whitespace(Whitespace::Newline),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_pg_regex_match() {
        let sql = "SELECT col ~ '^a', col ~* '^a', col !~ '^a', col !~* '^a'";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::Tilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::TildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkTilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkTildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_select_array() {
        let sql = String::from("SELECT '{1, 2, 3}'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("{1, 2, 3}")),
        ];

        compare(expected, tokens);
    }

    fn compare(expected: Vec<Token>, actual: Vec<Token>) {
        // println!("------------------------------");
        // println!("tokens   = {:?}", actual);
        // println!("expected = {:?}", expected);
        // println!("------------------------------");
        assert_eq!(expected, actual);
    }
}
