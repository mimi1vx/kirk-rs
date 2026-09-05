//! `Plugin::clone_box` renames, mirroring `Plugin.clone` in `plugin.py`.

use std::collections::HashMap;

use kirk_core::KirkError;
use kirk_plugin::Plugin;

struct FakePlugin {
    name: String,
    configured: bool,
}

impl Plugin for FakePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::from([("opt".to_string(), "an option".to_string())])
    }

    fn setup(&mut self, _cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        self.configured = true;
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_string(),
            configured: self.configured,
        })
    }
}

#[test]
fn clone_renames() {
    let original = FakePlugin {
        name: "original".to_string(),
        configured: false,
    };
    let cloned = original.clone_box("newchan");

    assert_eq!(cloned.name(), "newchan");
    assert_eq!(original.name(), "original");
    assert!(cloned.config_help().contains_key("opt"));
}

#[test]
fn clone_preserves_setup_flag() {
    let mut original = FakePlugin {
        name: "original".to_string(),
        configured: false,
    };
    original.setup(&HashMap::new()).expect("setup succeeds");
    let cloned = original.clone_box("copy");

    assert_eq!(cloned.name(), "copy");
}
