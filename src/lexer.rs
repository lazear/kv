use std::char;
use std::iter::Peekable;
use std::str;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Token {
    Disconnect,
    Create,
    Read,
    Update,
    Delete,
    Subscribe,
    Array(Vec<Token>),
    Identifier(String),
    Integer(i64),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Error {
    Delimiter(usize),
    UnexpectedEOF,
    Expected(char, char, usize),
    Parse,
}

pub struct Lexer<'a> {
    input: Peekable<str::Chars<'a>>,
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn from(s: &'a str) -> Self {
        Lexer {
            input: s.chars().peekable(),
            pos: 0,
        }
    }

    fn peek(&mut self) -> Result<char, Error> {
        match self.input.peek() {
            Some(&c) => Ok(c),
            None => Err(Error::UnexpectedEOF),
        }
    }

    fn consume(&mut self) -> Result<char, Error> {
        let next = self.input.next();
        if next.is_some() {
            self.pos += 1;
        }
        next.ok_or(Error::UnexpectedEOF)
    }

    fn consume_while<F: Fn(char) -> bool>(&mut self, lambda: F) -> Result<String, Error> {
        let mut s = String::new();
        while let Ok(c) = self.peek() {
            if lambda(c) {
                s.push(self.consume().expect("We have already peeked"));
            } else {
                return Ok(s);
            }
        }
        Ok(s)
    }

    fn consume_until_crlf(&mut self) -> Result<String, Error> {
        let mut s = String::new();
        while let Ok(c) = self.peek() {
            if c != '\r' {
                s.push(self.consume().expect("We have already peeked"));
            } else {
                return self.try_consume_crlf().map(|_| s);
            }
        }
        Ok(s)
    }

    fn try_consume_crlf(&mut self) -> Result<(), Error> {
        match self.peek() {
            Ok('\r') => {
                self.consume()?;
                match self.peek() {
                    Ok('\n') => self.consume().map(|_| ()),
                    Ok(c) => Err(Error::Expected('\n', c, self.pos)),
                    Err(e) => Err(e),
                }
            }
            Ok(c) => Err(Error::Expected('\r', c, self.pos)),
            Err(e) => Err(e),
        }
    }

    fn identifier(&self, s: String) -> Token {
        match s.as_ref() {
            "DISCONNECT" => Token::Disconnect,
            "CREATE" => Token::Create,
            "READ" => Token::Read,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "SUB" => Token::Subscribe,
            _ => Token::Identifier(s),
        }
    }

    pub fn lex(&mut self) -> Result<Token, Error> {
        if let Ok(c) = self.peek() {
            match c {
                '$' => {
                    let _ = self.consume()?;
                    let n = self.consume_while(|c| c.is_numeric())?;
                    let len = n.parse::<usize>().map_err(|_| Error::Parse)?;
                    self.try_consume_crlf()?;
                    let string = self.input.by_ref().take(len).collect::<String>();
                    self.try_consume_crlf()?;

                    return Ok(self.identifier(string));
                }
                '*' => {
                    let _ = self.consume()?;
                    let n = self.consume_while(|c| c.is_numeric())?;
                    let len = n.parse::<usize>().map_err(|_| Error::Parse)?;
                    self.try_consume_crlf()?;
                    let mut array = Vec::with_capacity(len);
                    for _ in 0..len {
                        array.push(self.lex()?);
                    }
                    return Ok(Token::Array(array));
                }
                ':' => {
                    let _ = self.consume()?;
                    let n = self.consume_until_crlf()?;
                    let num = n.parse::<i64>().map_err(|_| Error::Parse)?;
                    return Ok(Token::Integer(num));
                }
                _ => return Err(Error::Delimiter(self.pos)),
            };
        }
        Err(Error::UnexpectedEOF)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn lex_array_nested() {
        let mut lexer =
            Lexer::from("*3\r\n$6\r\nCREATE\r\n$3\r\nkey\r\n*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n");
        assert_eq!(
            lexer.lex(),
            Ok(Token::Array(vec![
                Token::Create,
                Token::Identifier(String::from("key")),
                Token::Array(vec![
                    Token::Identifier(String::from("val1")),
                    Token::Identifier(String::from("val2"))
                ])
            ]))
        );
    }

    #[test]
    fn lex_array() {
        let mut lexer = Lexer::from("*3\r\n$6\r\nhello!\r\n$3\r\nSUB\r\n:12341234\r\n");
        assert_eq!(
            lexer.lex(),
            Ok(Token::Array(vec![
                Token::Identifier(String::from("hello!")),
                Token::Subscribe,
                Token::Integer(12341234)
            ]))
        );
    }

    #[test]
    fn lex_int() {
        let mut lexer = Lexer::from(":-100346\r\n");
        assert_eq!(lexer.lex(), Ok(Token::Integer(-100346)));
    }
}
