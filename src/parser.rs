use super::lexer;
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
        write!(f, "{}", self.encode())
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

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Error {
    Expected(String, Token),
    Terminated,
    Syntax(lexer::Error),
    InvalidUTF8,
}

impl Parser {
    pub fn from(s: &[u8]) -> Result<Parser, Error> {
        match Lexer::from(str::from_utf8(s).map_err(|_| Error::InvalidUTF8)?)
            .lex()
            .map_err(Error::Syntax)?
        {
            Token::Array(array) => Ok(Parser {
                tokens: VecDeque::from(array),
            }),
            t => Err(Error::Expected("token array".into(), t)),
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

    fn expect_identifier(&mut self) -> Result<String, Error> {
        match self.tokens.pop_front() {
            Some(Token::Identifier(s)) => Ok(s),
            Some(t) => Err(Error::Expected("identifier".into(), t)),
            None => Err(Error::Terminated),
        }
    }

    fn pop_front(&mut self) -> Result<Value, Error> {
        match self.tokens.pop_front() {
            Some(token) => Ok(self.token_to_value(token)),
            None => Err(Error::Terminated),
        }
    }

    pub fn parse(&mut self) -> Result<Vec<Command>, Error> {
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
                t => return Err(Error::Expected("command or array".into(), t)),
            }
        }
        Ok(cmd)
    }
}

impl Value {
    pub fn encode(&self) -> String {
        match self {
            Value::Null => "*0\r\n".to_string(),
            Value::Text(ref s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::Array(ref a) => a.iter().fold(format!("*{}\r\n", a.len()), |mut acc, val| {
                acc.push_str(&val.encode());
                acc
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn enocde_array() {
        let answer = "*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n";
        assert_eq!(
            answer,
            encode(&Value::Array(vec![
                Value::Text(String::from("val1")),
                Value::Text(String::from("val2"))
            ]))
        );
    }

    #[test]
    fn parse_array() {
        let mut parser =
            Parser::from(b"*3\r\n$6\r\nCREATE\r\n$3\r\nkey\r\n*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n")
                .unwrap();
        assert_eq!(
            parser.parse(),
            Ok(vec![Command::Create(
                String::from("key"),
                Value::Array(vec![
                    Value::Text(String::from("val1")),
                    Value::Text(String::from("val2"))
                ])
            ),])
        );
    }

    #[test]
    fn parse_cmd() {
        let mut parser = Parser::from(b"*2\r\n$3\r\nSUB\r\n$3\r\nkey\r\n").unwrap();
        assert_eq!(
            parser.parse(),
            Ok(vec![Command::Subscribe(String::from("key"))])
        );
    }
}
