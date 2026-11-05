/// Read the TiKV PD endpoints, or return `None` so callers can skip an unconfigured test.
pub fn tikv_endpoints() -> Option<Vec<String>> {
    let raw = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS").ok()?;
    let list: Vec<String> = raw
        .split(',')
        .map(str::to_owned)
        .filter(|endpoint| !endpoint.is_empty())
        .collect();
    if list.is_empty() { None } else { Some(list) }
}

#[cfg(test)]
mod tests {
    use super::tikv_endpoints;
    use std::process::Command;

    #[test]
    fn tikv_endpoints_skip_without_configuration_and_parse_configured_endpoints() {
        const CASE: &str = "BREWFS_TEST_TIKV_ENDPOINTS_HELPER_CASE";
        const ENDPOINTS: &str = "BREWFS_TEST_TIKV_PD_ENDPOINTS";
        match std::env::var(CASE).ok().as_deref() {
            Some("unset" | "empty" | "commas") => assert_eq!(tikv_endpoints(), None),
            Some("configured") => assert_eq!(
                tikv_endpoints(),
                Some(vec!["127.0.0.1:2379".into(), "127.0.0.2:2379".into()])
            ),
            Some(case) => panic!("unexpected helper test case: {case}"),
            None => {
                // Each child owns its environment; parallel tests and configured TiKV
                // fixtures in the parent never observe temporary endpoint changes.
                for (case, value) in [
                    ("unset", None),
                    ("empty", Some("")),
                    ("commas", Some(",,,")),
                    ("configured", Some(",127.0.0.1:2379,,127.0.0.2:2379,")),
                ] {
                    let mut child = Command::new(std::env::current_exe().unwrap());
                    child
                        .args([
                            "tikv_endpoints_skip_without_configuration_and_parse_configured_endpoints",
                            "--nocapture",
                            "--test-threads=1",
                        ])
                        .env(CASE, case)
                        .env_remove(ENDPOINTS);
                    if let Some(value) = value {
                        child.env(ENDPOINTS, value);
                    }
                    let output = child.output().unwrap();
                    assert!(
                        output.status.success(),
                        "{case}: {}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        }
    }
}
