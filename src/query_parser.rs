// Ref: https://dzone.com/articles/the-internal-architecture-of-the-sqlite-database

use anyhow::bail;
use itertools::Itertools;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_till, take_until, take_while_m_n},
    character::complete::{alphanumeric1, anychar, space0, space1},
    combinator::{map_res, opt, rest, success},
    multi::{many_till, separated_list0, separated_list1},
    sequence::{delimited, terminated, Tuple},
    IResult, Parser,
};

#[derive(Debug, PartialEq)]
pub enum FunctionArgument {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub enum Function {
    Count(FunctionArgument),
}

#[derive(Debug, PartialEq)]
pub enum Selection {
    ColumnName(String),
    AggregateFunction(Function),
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub selection_list: Vec<Selection>,
    pub from_table: String,
}

// TODO: Use these nom functions:
// map_opt: Maps a function returning an Option on the output of a parser
// map_res: Maps a function returning a Result on the output of a parser

fn parse_selection_list(input: &str) -> IResult<&str, Vec<Selection>> {
    let (input, (raw_selections, _from)) = many_till(
        delimited(
            alt((space1, tag(","))),
            alt((tag_no_case("COUNT(*)"), alphanumeric1)),
            alt((space1, tag(","))),
        ),
        tag_no_case("from"),
    )(input)?;

    let selections = raw_selections
        .iter()
        .map(|raw_selection| raw_selection.to_lowercase())
        .map(|raw_selection| match raw_selection.as_str() {
            "count(*)" => Selection::AggregateFunction(Function::Count(FunctionArgument::All)),
            (s) => Selection::ColumnName(s.to_string()),
        })
        .collect_vec();

    Ok((input, selections))
}

// "SELECT COUNT(*) FROM apples"
// "SELECT name FROM apples"
// "SELECT name, color FROM apples"
// "SELECT name, color FROM apples WHERE color = 'Yellow'"
// "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"
// "SELECT id, name FROM companies WHERE country = 'eritrea'"

pub fn parse_query(input: &str) -> IResult<&str, Query> {
    let (input, select_tag) = tag_no_case("SELECT")(input)?;

    let (input, selection_list) = parse_selection_list(input)?;

    let (input, from_table) = delimited(space0, alphanumeric1, space0)(input)?;

    Ok((
        input,
        Query {
            selection_list: selection_list,
            from_table: from_table.to_string(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // "SELECT COUNT(*) FROM apples"
    // "SELECT name FROM apples"
    // "SELECT name, color FROM apples"
    // "SELECT name, color FROM apples WHERE color = 'Yellow'"
    // "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"
    // "SELECT id, name FROM companies WHERE country = 'eritrea'"

    #[test]
    fn test_parse_query_count() {
        let count_query = "SELECT COUNT(*) FROM apples";

        let (raw_query, query) = parse_query(count_query).unwrap();

        assert_eq!(
            query.selection_list,
            vec![Selection::AggregateFunction(Function::Count(
                FunctionArgument::All
            ))]
        );
        assert_eq!(query.from_table, "apples");
        assert_eq!(raw_query, "");
    }

    #[test]
    fn test_parse_query_multi_columns() {
        let count_query = "SELECT name, color FROM carrots";

        let (raw_query, query) = parse_query(count_query).unwrap();

        assert_eq!(
            query.selection_list,
            vec![
                Selection::ColumnName("name".to_string()),
                Selection::ColumnName("color".to_string())
            ]
        );
        assert_eq!(query.from_table, "carrots");
        assert_eq!(raw_query, "");
    }
}
