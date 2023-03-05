use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use googletest::matcher::Matcher;
use googletest::matcher::MatcherResult;

pub struct HashMapMatcher<K, V> {
    expected: HashMap<K, V>,
    err_msg: Cell<String>,
}

impl<K: Debug + Eq + Hash, V: Debug + PartialEq> Matcher<HashMap<K, V>> for HashMapMatcher<K, V> {
    fn matches(&self, actual: &HashMap<K, V>) -> MatcherResult {
        if self.expected.len() != actual.len() {
            self.err_msg.set(format!(
                "Expected hashmap has size {}, actual is {}",
                self.expected.len(),
                actual.len()
            ));
            return MatcherResult::DoesNotMatch;
        }

        for (key, val) in actual.iter() {
            if let Some(other_val) = self.expected.get(key) {
                if val != other_val {
                    return MatcherResult::DoesNotMatch;
                }
            } else {
                return MatcherResult::DoesNotMatch;
            }
        }

        MatcherResult::Matches
    }

    fn describe(&self, matcher_result: MatcherResult) -> String {
        match matcher_result {
            MatcherResult::Matches => format!("is same as {:?}", self.expected),
            MatcherResult::DoesNotMatch => {
                format!(
                    "Hashmap is not the same as expected: {}.",
                    self.err_msg.take()
                )
            }
        }
    }
}

pub fn hashmap_eq<K: Clone, V: Clone>(expected: &HashMap<K, V>) -> HashMapMatcher<K, V> {
    HashMapMatcher {
        expected: expected.clone(),
        err_msg: Cell::default(),
    }
}
