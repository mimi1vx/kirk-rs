use kirk_core::KirkError;

fn may_fail_sut(fail: bool) -> Result<String, KirkError> {
    if fail {
        Err(KirkError::Sut(String::from("sut broken")))
    } else {
        Ok(String::from("ok"))
    }
}

#[test]
fn each_variant_display_is_non_empty() {
    let variants = [
        KirkError::Plugin(String::from("p")),
        KirkError::Communication(String::from("c")),
        KirkError::Sut(String::from("s")),
        KirkError::KernelPanic(String::from("kp")),
        KirkError::KernelTainted(String::from("kt")),
        KirkError::KernelTimeout(String::from("kto")),
        KirkError::Framework(String::from("f")),
        KirkError::Exporter(String::from("e")),
        KirkError::Ltx(String::from("l")),
        KirkError::Scheduler(String::from("sched")),
        KirkError::Session(String::from("sess")),
    ];
    for err in variants {
        assert!(
            !err.to_string().is_empty(),
            "{err:?} Display must be non-empty"
        );
    }
}

#[test]
fn catch_sut_error_specifically() {
    let err = may_fail_sut(true).unwrap_err();
    assert!(matches!(err, KirkError::Sut(_)));

    let value = may_fail_sut(false).unwrap();
    assert_eq!(value, "ok");
}

#[test]
fn question_mark_propagates_kirk_error() {
    fn inner() -> Result<String, KirkError> {
        let value = may_fail_sut(false)?;
        Ok(value)
    }

    fn failing() -> Result<String, KirkError> {
        let _ = may_fail_sut(true)?;
        Ok(String::from("unreachable"))
    }

    assert_eq!(inner().unwrap(), "ok");
    assert!(matches!(failing().unwrap_err(), KirkError::Sut(_)));
}
