//! With the feature absent the body is the entire expansion: labels and timer
//! expressions are not evaluated and no extra async future is constructed.
#[cfg(not(feature = "observability"))]
#[macro_export]
macro_rules! observe_fuse {
    ($op:expr, $body:block) => {
        $body
    };
}
#[cfg(feature = "observability")]
#[macro_export]
macro_rules! observe_fuse {
    ($op:expr, $body:block) => {{
        let timer = $crate::observability::fuse_timer($op);
        let result = (async { $body }).await;
        if let Some(timer) = timer {
            timer.finish(result.as_ref().err().map(|error| i32::from(*error)));
        }
        result
    }};
}
#[cfg(not(feature = "observability"))]
#[macro_export]
macro_rules! observe_metadata {
    ($op:expr, $backend:expr, $body:block) => {
        $body
    };
}
#[cfg(feature = "observability")]
#[macro_export]
macro_rules! observe_metadata {
    ($op:expr, $backend:expr, $body:block) => {{
        let timer = $crate::observability::metadata_timer($op, $backend);
        let result = (async { $body }).await;
        drop(timer);
        result
    }};
}

#[cfg(all(test, not(feature = "observability")))]
mod tests {
    #[test]
    fn disabled_instrumentation_does_not_evaluate_arguments() {
        assert_eq!(crate::observe_fuse!(panic!("label evaluated"), { 7 }), 7);
        assert_eq!(
            crate::observe_metadata!(panic!("op evaluated"), panic!("backend evaluated"), { 9 }),
            9
        );
    }
}
