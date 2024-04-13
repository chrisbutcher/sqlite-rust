// Ref: https://dzone.com/articles/the-internal-architecture-of-the-sqlite-database

use itertools::Itertools;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::{alphanumeric1, multispace0, multispace1},
    combinator::opt,
    multi::{many_till, separated_list1},
    sequence::{delimited, pair, separated_pair},
    IResult,
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
pub struct AndCondition {
    pub column_name: String,
    pub value: String,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub selection_list: Vec<Selection>,
    pub from_table: String,
    pub and_conditions: Option<Vec<AndCondition>>,
}

// TODO: Use these nom functions:
// map_opt: Maps a function returning an Option on the output of a parser
// map_res: Maps a function returning a Result on the output of a parser

fn parse_selection_list(input: &str) -> IResult<&str, Vec<Selection>> {
    let (input, (raw_selections, _from)) = many_till(
        delimited(
            alt((multispace1, tag(","))),
            alt((tag_no_case("COUNT(*)"), alphanumeric1)),
            alt((multispace1, tag(","))),
        ),
        tag_no_case("from"),
    )(input)?;

    let selections = raw_selections
        .iter()
        .map(|raw_selection| raw_selection.to_lowercase())
        .map(|raw_selection| match raw_selection.as_str() {
            "count(*)" => Selection::AggregateFunction(Function::Count(FunctionArgument::All)),
            s => Selection::ColumnName(s.to_string()),
        })
        .collect_vec();

    Ok((input, selections))
}

fn parse_where_conditions(input: &str) -> IResult<&str, Vec<AndCondition>> {
    let (input, (_, _)) = pair(tag_no_case("WHERE"), multispace1)(input)?;

    // TODO: Handle ORs?
    let (input, raw_conditions) = separated_list1(
        delimited(multispace0, tag_no_case("AND"), multispace0),
        separated_pair(
            take_till(|c| c == ' '),
            delimited(
                multispace0,
                nom::character::complete::char('='),
                multispace0,
            ),
            delimited(
                nom::character::complete::char('\''),
                take_till(|c| c == '\''),
                nom::character::complete::char('\''),
            ),
        ),
    )(input)?;

    let conditions = raw_conditions
        .iter()
        .map(|c| AndCondition {
            column_name: c.0.to_string(),
            value: c.1.to_string(),
        })
        .collect_vec();

    Ok((input, conditions))
}

pub fn parse_query(input: &str) -> IResult<&str, Query> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, selection_list) = parse_selection_list(input)?;
    let (input, from_table) = delimited(multispace0, alphanumeric1, multispace0)(input)?;
    let (input, conditions) = opt(parse_where_conditions)(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        Query {
            selection_list,
            from_table: from_table.to_string(),
            and_conditions: conditions,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_parse_query_multiple_where_condition() {
        let count_query = "  SELECT    id,    name    FROM    superheroes   WHERE     eye_color   =   'Pink Eyes'   AND  favourite_food   =    'pizza'      ";

        let (raw_query, query) = parse_query(count_query).unwrap();

        assert_eq!(
            query.selection_list,
            vec![
                Selection::ColumnName("id".to_string()),
                Selection::ColumnName("name".to_string())
            ]
        );
        assert_eq!(query.from_table, "superheroes");

        assert_eq!(
            query.and_conditions,
            Some(vec![
                AndCondition {
                    column_name: "eye_color".to_string(),
                    value: "Pink Eyes".to_string()
                },
                AndCondition {
                    column_name: "favourite_food".to_string(),
                    value: "pizza".to_string()
                }
            ])
        );
        assert_eq!(raw_query, "");
    }

    #[test]
    fn test_parse_query_where_condition() {
        let count_query =
            "SELECT id, name FROM superheroes    WHERE    eye_color    =   'Pink Eyes'   ";

        let (raw_query, query) = parse_query(count_query).unwrap();

        assert_eq!(
            query.selection_list,
            vec![
                Selection::ColumnName("id".to_string()),
                Selection::ColumnName("name".to_string())
            ]
        );
        assert_eq!(query.from_table, "superheroes");

        assert_eq!(
            query.and_conditions,
            Some(vec![AndCondition {
                column_name: "eye_color".to_string(),
                value: "Pink Eyes".to_string()
            }])
        );

        assert_eq!(raw_query, "");
    }
}
