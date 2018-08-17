use super::lexer::{Lexer, Token, Token::*};
use std::collections::VecDeque;
use std::fmt;
use std::str;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct Parser {
    tokens: VecDeque<Token>,
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    Text(String),
    Integer(i64),
    Array(Vec<Value>),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Text(ref s) => write!(f, "${}\r\n{}\r\n", s.len(), s),
            Value::Integer(ref i) => write!(f, ":{}\r\n", i),
            Value::Array(ref array) => write!(f, "*{}\r\n", array.len()).and(
                array
                    .iter()
                    .map(|v| write!(f, "{}", v))
                    .collect::<fmt::Result>(),
            ),
            Null => write!(f, ""),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Command {
    Disconnect,
    Create(String, Value),
    Read(String),
    Update(String, Value),
    Delete(String),
    Subscribe(String),
}

impl Parser {
    pub fn from(s: &[u8]) -> Option<Parser> {
        if let Ok(Token::Array(array)) = Lexer::from(str::from_utf8(s).ok()?).lex() {
            Some(Parser {
                tokens: VecDeque::from(array),
            })
        } else {
            None
        }
    }

    fn token_to_value(&self, token: Token) -> Value {
        match token {
            Token::Identifier(s) => Value::Text(s),
            Token::Integer(i) => Value::Integer(i),
            Token::Array(array) => Value::Array(
                array
                    .into_iter()
                    .map(|token| self.token_to_value(token))
                    .collect(),
            ),
            _ => Value::Null,
        }
    }

    fn expect_identifier(&mut self) -> Option<String> {
        match self.tokens.pop_front()? {
            Token::Identifier(s) => Some(s),
            _ => None,
        }
    }

    fn pop_front(&mut self) -> Option<Value> {
        self.tokens
            .pop_front()
            .map(|token| self.token_to_value(token))
    }

    pub fn parse(&mut self) -> Option<Vec<Command>> {
        let mut cmd = Vec::new();
        while let Some(token) = self.tokens.pop_front() {
            match token {
                Disconnect => cmd.push(Command::Disconnect),
                Create => cmd.push(Command::Create(
                    self.expect_identifier()?,
                    self.pop_front()?,
                )),
                Read => cmd.push(Command::Read(self.expect_identifier()?)),
                Update => cmd.push(Command::Update(
                    self.expect_identifier()?,
                    self.pop_front()?,
                )),
                Delete => cmd.push(Command::Delete(self.expect_identifier()?)),
                Subscribe => cmd.push(Command::Subscribe(self.expect_identifier()?)),
                Token::Array(array) => {
                    self.tokens.extend(array);
                }
                _ => return None,
            }
        }
        println!("Parsed {:?}", cmd);
        Some(cmd)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_array() {
        let mut parser =
            Parser::from(b"*3\r\n$6\r\nCREATE\r\n$3\r\nkey\r\n*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n")
                .unwrap();
        assert_eq!(
            parser.parse(),
            Some(vec![Command::Create(
                String::from("key"),
                Value::Array(vec![
                    Value::Text(String::from("val1")),
                    Value::Text(String::from("val2"))
                ])
            ),])
        );
    }

    #[test]
    fn parse_fail() {
        let mut parser = Parser::from(b":-100346\r\n");
        assert_eq!(parser, None);
    }

    #[test]
    fn parse_cmd() {
        let mut parser = Parser::from(b"*2\r\n$3\r\nSUB\r\n$3\r\nkey\r\n").unwrap();
        assert_eq!(
            parser.parse(),
            Some(vec![Command::Subscribe(String::from("key"))])
        );
    }
}
