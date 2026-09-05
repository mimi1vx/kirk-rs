//! Test and suite definitions mirroring `kirk/libkirk/data.py`.
//!
//! Constructors enforce the same invariants as upstream: an empty test name
//! or command is rejected with an error instead of raising `ValueError`.

use std::collections::HashMap;

use crate::errors::KirkError;

/// Definition of a single test to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Test {
    name: String,
    command: String,
    cwd: Option<String>,
    env: HashMap<String, String>,
    args: Vec<String>,
    parallelizable: bool,
}

impl Test {
    /// Create a new test definition.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Framework`] when `name` or `command` is empty.
    pub fn new(name: &str, command: &str) -> Result<Self, KirkError> {
        if name.is_empty() {
            return Err(KirkError::Framework("Test must have a name".to_owned()));
        }
        if command.is_empty() {
            return Err(KirkError::Framework("Test must have a command".to_owned()));
        }
        Ok(Self {
            name: name.to_owned(),
            command: command.to_owned(),
            cwd: None,
            env: HashMap::new(),
            args: Vec::new(),
            parallelizable: false,
        })
    }

    /// Set the command arguments.
    #[must_use]
    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    /// Set the working directory of the command.
    #[must_use]
    pub fn with_cwd(mut self, cwd: &str) -> Self {
        self.cwd = Some(cwd.to_owned());
        self
    }

    /// Set the environment variables used to run the command.
    #[must_use]
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env = env;
        self
    }

    /// Set whether the test can run in parallel.
    #[must_use]
    pub fn with_parallelizable(mut self, parallelizable: bool) -> Self {
        self.parallelizable = parallelizable;
        self
    }

    /// Name of the test.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Command to execute.
    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }

    /// Arguments of the command.
    #[must_use]
    pub fn arguments(&self) -> &[String] {
        &self.args
    }

    /// Whether the test can run in parallel.
    #[must_use]
    pub fn parallelizable(&self) -> bool {
        self.parallelizable
    }

    /// Working directory of the command, if any.
    #[must_use]
    pub fn cwd(&self) -> Option<&str> {
        self.cwd.as_deref()
    }

    /// Environment variables used to run the command.
    #[must_use]
    pub fn env(&self) -> &HashMap<String, String> {
        &self.env
    }

    /// Full command line, with arguments appended.
    #[must_use]
    pub fn full_command(&self) -> String {
        if self.args.is_empty() {
            self.command.clone()
        } else {
            format!("{} {}", self.command, self.args.join(" "))
        }
    }

    /// Force the test to be parallelizable.
    pub fn force_parallel(&mut self) {
        self.parallelizable = true;
    }
}

/// Testing suite definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suite {
    name: String,
    tests: Vec<Test>,
}

impl Suite {
    /// Create a new suite. Upstream does not validate the name here.
    #[must_use]
    pub fn new(name: &str, tests: Vec<Test>) -> Self {
        Self {
            name: name.to_owned(),
            tests,
        }
    }

    /// Name of the testing suite.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set the suite name.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Framework`] when `value` is empty.
    pub fn set_name(&mut self, value: &str) -> Result<(), KirkError> {
        if value.is_empty() {
            return Err(KirkError::Framework("empty suite name".to_owned()));
        }
        value.clone_into(&mut self.name);
        Ok(())
    }

    /// Tests of the suite.
    #[must_use]
    pub fn tests(&self) -> &[Test] {
        &self.tests
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_name_fails() {
        let err = Test::new("", "ls").unwrap_err();
        assert!(matches!(err, KirkError::Framework(_)));
        assert!(err.to_string().contains("Test must have a name"));
    }

    #[test]
    fn empty_command_fails() {
        let err = Test::new("ls-test", "").unwrap_err();
        assert!(matches!(err, KirkError::Framework(_)));
        assert!(err.to_string().contains("Test must have a command"));
    }

    #[test]
    fn full_command_joins_args() {
        let test = Test::new("ls-test", "ls")
            .unwrap()
            .with_args(vec!["-l".to_owned(), "-a".to_owned()]);
        assert_eq!(test.full_command(), "ls -l -a");
    }

    #[test]
    fn full_command_without_args() {
        let test = Test::new("ls-test", "ls").unwrap();
        assert_eq!(test.full_command(), "ls");
    }

    #[test]
    fn force_parallel_marks_parallelizable() {
        let mut test = Test::new("ls-test", "ls").unwrap();
        assert!(!test.parallelizable());
        test.force_parallel();
        assert!(test.parallelizable());
    }

    #[test]
    fn suite_set_name_rejects_empty() {
        let mut suite = Suite::new("suite0", Vec::new());
        assert!(suite.set_name("").is_err());
        assert_eq!(suite.name(), "suite0");
        suite.set_name("suite1").unwrap();
        assert_eq!(suite.name(), "suite1");
    }

    #[test]
    fn suite_holds_tests() {
        let test = Test::new("ls-test", "ls").unwrap();
        let suite = Suite::new("suite0", vec![test.clone()]);
        assert_eq!(suite.tests(), &[test]);
    }
}
