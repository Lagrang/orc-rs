use std::borrow::Borrow;
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use colored::*;
use googletest::matcher::Matcher;
use googletest::matcher::MatcherResult;
use googletest::matchers::eq;
use googletest::verify_that;

use crate::tail::FileTail;

pub(crate) struct HashMapMatcher<K, V> {
    expected: HashMap<K, V>,
    err_msg: Cell<String>,
}

impl<K: Debug + Eq + Hash, V: Debug + PartialEq> Matcher for HashMapMatcher<K, V> {
    type ActualT = HashMap<K, V>;

    fn matches(&self, actual: &HashMap<K, V>) -> MatcherResult {
        if self.expected.len() != actual.len() {
            self.err_msg.set(format!(
                "Expected hashmap has size {}, actual is {}",
                self.expected.len(),
                actual.len()
            ));
            return MatcherResult::NoMatch;
        }

        for (key, val) in actual.iter() {
            if let Some(other_val) = self.expected.get(key) {
                if val != other_val {
                    return MatcherResult::NoMatch;
                }
            } else {
                return MatcherResult::NoMatch;
            }
        }

        MatcherResult::Match
    }

    fn describe(&self, matcher_result: MatcherResult) -> String {
        match matcher_result {
            MatcherResult::Match => format!("is same as {:?}", self.expected),
            MatcherResult::NoMatch => {
                format!(
                    "Hashmap is not the same as expected: {}.",
                    self.err_msg.take()
                )
            }
        }
    }
}

pub(crate) fn hashmap_eq<K: Clone, V: Clone>(expected: &HashMap<K, V>) -> HashMapMatcher<K, V> {
    HashMapMatcher {
        expected: expected.clone(),
        err_msg: Cell::default(),
    }
}

pub fn diff<L: Debug, R: Debug>(left: &L, right: &R) -> String {
    let left_str = format!("{left:?}");
    let right_str = format!("{right:?}");
    let diff = similar::utils::diff_words(similar::Algorithm::Patience, &left_str, &right_str);

    diff.iter()
        .map(|(tag, val)| match tag {
            similar::ChangeTag::Delete => val.to_owned().red(),
            similar::ChangeTag::Insert => val.to_owned().green(),
            similar::ChangeTag::Equal => val.to_owned().white(),
        })
        .fold(String::new(), |a, b| format!("{a}{b}"))
}

pub(crate) struct ArrowSchemaMatcher {
    expected: Arc<arrow::datatypes::Schema>,
    diff: Cell<String>,
}

impl Matcher for ArrowSchemaMatcher {
    type ActualT = Arc<arrow::datatypes::Schema>;

    fn matches(&self, actual: &Arc<arrow::datatypes::Schema>) -> MatcherResult {
        if &self.expected == actual {
            MatcherResult::Match
        } else {
            self.diff.set(diff(
                &format!("{:?}", self.expected),
                &format!("{:?}", actual),
            ));
            MatcherResult::NoMatch
        }
    }

    fn describe(&self, matcher_result: MatcherResult) -> String {
        match matcher_result {
            MatcherResult::NoMatch => format!("Schema is different:\n{}", self.diff.take()),
            MatcherResult::Match => "Schemas are equal.".to_owned(),
        }
    }
}

pub(crate) fn arrow_schema_eq(expected: Arc<arrow::datatypes::Schema>) -> ArrowSchemaMatcher {
    ArrowSchemaMatcher {
        expected: expected.clone(),
        diff: Cell::default(),
    }
}

pub(crate) struct FileTailMatcher {
    expected: FileTail,
    diff: Cell<String>,
    err_msg: Cell<String>,
}

impl Matcher for FileTailMatcher {
    type ActualT = FileTail;

    fn matches(&self, actual: &FileTail) -> MatcherResult {
        let result = verify_that!(self.expected.content_size, eq(actual.content_size))
            .and(verify_that!(
                self.expected.header_size,
                eq(actual.header_size)
            ))
            .and(verify_that!(
                self.expected.postscript.compression(),
                eq(actual.postscript.compression())
            ))
            .and(verify_that!(
                self.expected.metadata,
                hashmap_eq(&actual.metadata)
            ))
            .and(verify_that!(self.expected.version, eq(actual.version)))
            .and(verify_that!(self.expected.row_count, eq(actual.row_count)))
            .and(verify_that!(
                self.expected.row_index_stride,
                eq(actual.row_index_stride)
            ))
            .and(verify_that!(
                actual.schema,
                arrow_schema_eq(self.expected.schema.clone())
            ));
        // .and(verify_that!(&self.expected.stripes, eq(&actual.stripes)))
        // .and(verify_that!(
        //     &self.expected.column_statistics,
        //     eq(&actual.column_statistics)
        // ));

        if let Some(err) = result.as_ref().err() {
            self.err_msg.set(err.to_string());
            self.diff.set(diff(&self.expected, actual));
        }

        result
            .map(|_| MatcherResult::Match)
            .unwrap_or(MatcherResult::NoMatch)
    }

    fn describe(&self, matcher_result: MatcherResult) -> String {
        match matcher_result {
            MatcherResult::Match => format!("is same as {:?}", self.expected),
            MatcherResult::NoMatch => {
                format!(
                    " is not the same as expected: {}.
                        Diff:
                        {}",
                    self.err_msg.take(),
                    self.diff.take()
                )
            }
        }
    }
}

pub(crate) fn same_tail(expected: FileTail) -> FileTailMatcher {
    FileTailMatcher {
        expected,
        diff: Cell::default(),
        err_msg: Cell::default(),
    }
}
