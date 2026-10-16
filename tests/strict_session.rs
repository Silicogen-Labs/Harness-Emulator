use silicogen_terminal_runtime::{
    config::LaunchOptions,
    isolation::{IsolationBackend, IsolationSpec, NetworkPolicy},
    protocol::{LaunchSpec, RpcRequest, WaitPredicate},
    service::SessionService,
    workspace::{WorkspaceMode, WorkspaceSpec},
};
use std::{collections::BTreeMap, fs};

#[test]
fn configured_strict_session_keeps_host_auth_readonly_and_snapshot_writable() {
    let tmp = tempfile::tempdir().unwrap();
    let origin = tmp.path().join("origin");
    fs::create_dir(&origin).unwrap();
    fs::write(origin.join("auth.json"), "synthetic-original").unwrap();
    let root = tmp.path().join("runtime");
    let service = SessionService::new(&root).unwrap();
    let options = LaunchOptions {
        disposable: true,
        workspace: Some(WorkspaceSpec {
            source: origin.to_string_lossy().into_owned(),
            mode: WorkspaceMode::Copy,
            ..Default::default()
        }),
        isolation: IsolationSpec {
            backend: IsolationBackend::Bubblewrap,
            network: NetworkPolicy::None,
        },
        ..Default::default()
    };
    let script = format!(
        "if printf changed > '{}'; then printf BREACH; exit 9; fi; printf changed > auth.json; printf STRICT_READY",
        origin.join("auth.json").display()
    );
    let spec = LaunchSpec {
        name: Some("strict-integration".into()),
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: "${workspace}".into(),
        env: BTreeMap::new(),
        cols: 100,
        rows: 30,
        record_output: false,
    };
    let result = service.handle(RpcRequest::LaunchConfigured { spec, options });
    if let Err(error) = &result {
        assert!(
            error.to_string().contains("unavailable"),
            "unexpected strict failure: {error:#}"
        );
        eprintln!("STRICT BACKEND UNAVAILABLE: {error:#}");
        return;
    }
    let launched = result.unwrap().0;
    let id = launched["session_id"].as_str().unwrap();
    let waited = service
        .handle(RpcRequest::Wait {
            session_id: id.into(),
            predicate: WaitPredicate::TextContains {
                text: "STRICT_READY".into(),
            },
            timeout_ms: 5000,
        })
        .unwrap()
        .0;
    assert_eq!(waited["matched"], true);
    assert!(
        !waited["snapshot"]["text"]
            .as_str()
            .unwrap()
            .contains("BREACH")
    );
    assert_eq!(
        fs::read_to_string(origin.join("auth.json")).unwrap(),
        "synthetic-original"
    );
    let snapshot = launched["capabilities"]["launch"]["workspace"]["root"]
        .as_str()
        .unwrap();
    assert_eq!(
        fs::read_to_string(std::path::Path::new(snapshot).join("auth.json")).unwrap(),
        "changed"
    );
    service.shutdown().unwrap();
}
