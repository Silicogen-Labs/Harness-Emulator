use std::collections::BTreeMap;
use std::fs;

use config::{LaunchOptions, StateMode, StateSeed};
use launch::prepare_launch;
use profiles::{ProfileEnvValue, create_profile, list_profiles, load_profile, store_profile};
use protocol::LaunchSpec;
use silicogen_terminal_runtime::{config, launch, profiles, protocol};

fn spec(env: BTreeMap<String, String>) -> LaunchSpec {
    LaunchSpec {
        name: None,
        program: "/usr/bin/true".into(),
        args: Vec::new(),
        cwd: "/".into(),
        env,
        cols: 80,
        rows: 24,
        record_output: false,
    }
}

#[test]
fn copied_auth_is_session_owned_and_sqlite_files_are_excluded() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let host = temp.path().join("fake-host-state");
    fs::create_dir_all(&host).unwrap();
    fs::write(host.join("auth.json"), "synthetic-auth-original").unwrap();
    fs::write(host.join("settings.json"), "synthetic-config-original").unwrap();
    fs::write(host.join("history.sqlite"), "sqlite-placeholder").unwrap();
    fs::write(host.join("history.sqlite-wal"), "wal-placeholder").unwrap();

    let options = LaunchOptions {
        state_mode: StateMode::CopyUser,
        seeds: vec![StateSeed {
            source: host.clone(),
            destination: "data/silicogen".into(),
            include_credentials: true,
        }],
        ..LaunchOptions::default()
    };
    let prepared = prepare_launch(
        &runtime,
        "session-copy-test",
        &spec(BTreeMap::new()),
        &options,
    )
    .unwrap();
    let second = prepare_launch(
        &runtime,
        "session-copy-test-2",
        &spec(BTreeMap::new()),
        &options,
    )
    .unwrap();
    let copied_auth = prepared.state_root.join("data/silicogen/auth.json");
    assert_eq!(
        fs::read_to_string(&copied_auth).unwrap(),
        "synthetic-auth-original"
    );
    fs::write(&copied_auth, "session-mutated-auth").unwrap();
    assert_eq!(
        fs::read_to_string(host.join("auth.json")).unwrap(),
        "synthetic-auth-original"
    );
    assert_eq!(
        fs::read_to_string(second.state_root.join("data/silicogen/auth.json")).unwrap(),
        "synthetic-auth-original"
    );
    assert_eq!(
        fs::read_to_string(host.join("settings.json")).unwrap(),
        "synthetic-config-original"
    );
    assert!(
        !prepared
            .state_root
            .join("data/silicogen/history.sqlite")
            .exists()
    );
    assert!(
        !prepared
            .state_root
            .join("data/silicogen/history.sqlite-wal")
            .exists()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            fs::metadata(host.join("auth.json")).unwrap().ino(),
            fs::metadata(copied_auth).unwrap().ino()
        );
    }
}

#[test]
fn named_profile_applies_allowlist_and_explicit_unset_without_diagnostics_values() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let mut profile = create_profile(&runtime, "safe-test", "test profile").unwrap();
    profile.env_allowlist = vec!["PATH".into()];
    profile
        .metadata
        .insert("private_note".into(), "synthetic-secret-value".into());
    profile.env.insert(
        "SILICOGEN_STARTUP_TRACE".into(),
        ProfileEnvValue::Literal { value: "1".into() },
    );
    profile.env.insert(
        "PROVIDER_API_KEY".into(),
        ProfileEnvValue::HostReference {
            variable: "PATH".into(),
            secret: true,
        },
    );
    store_profile(&runtime, &profile).unwrap();
    let listed = list_profiles(&runtime).unwrap();
    assert_eq!(
        listed[0].credential_reference_names,
        vec!["PROVIDER_API_KEY"]
    );
    let serialized_metadata = serde_json::to_string(&listed).unwrap();
    assert!(!serialized_metadata.contains("synthetic-secret-value"));

    let launch_spec = spec(BTreeMap::from([("APP_OVERRIDE".into(), "visible".into())]));
    let prepared = prepare_launch(
        &runtime,
        "session-profile-test",
        &launch_spec,
        &LaunchOptions {
            state_mode: StateMode::Profile,
            profile: Some("safe-test".into()),
            env_unset: vec!["NO_COLOR".into()],
            ..LaunchOptions::default()
        },
    )
    .unwrap();
    assert_eq!(prepared.environment["SILICOGEN_STARTUP_TRACE"], "1");
    assert!(prepared.environment.contains_key("PATH"));
    assert_eq!(prepared.environment["APP_OVERRIDE"], "visible");
    assert!(!prepared.environment.contains_key("NO_COLOR"));
    assert_eq!(
        prepared.environment["PROVIDER_API_KEY"],
        std::env::var("PATH").unwrap()
    );
    assert_eq!(
        prepared.environment["HOME"],
        prepared.state_root.join("home").to_string_lossy()
    );
    let diagnostic_text = prepared.diagnostics.to_string();
    assert!(!diagnostic_text.contains(&std::env::var("PATH").unwrap()));
    assert!(diagnostic_text.contains("profile_secret_reference"));
    assert_eq!(
        load_profile(&runtime, "safe-test").unwrap().name,
        "safe-test"
    );
}

#[test]
fn managed_state_environment_override_is_actionable_error() {
    let temp = tempfile::tempdir().unwrap();
    let result = prepare_launch(
        temp.path(),
        "session-invalid-state",
        &spec(BTreeMap::from([("HOME".into(), "/host/home".into())])),
        &LaunchOptions::default(),
    );
    let error = result.unwrap_err().to_string();
    assert!(error.contains("HOME is managed"));
    assert!(error.contains("profile seed"));
}

#[test]
fn prepared_session_root_and_workspace_are_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let session = runtime.join("sessions/session-prepared-first");
    fs::create_dir_all(session.join("runs")).unwrap();
    fs::create_dir_all(session.join("workspace")).unwrap();
    fs::write(session.join("workspace/marker.txt"), "main-prepared").unwrap();

    let prepared = prepare_launch(
        &runtime,
        "session-prepared-first",
        &spec(BTreeMap::new()),
        &LaunchOptions::default(),
    )
    .unwrap();
    assert_eq!(prepared.state_root, session.join("state"));
    assert_eq!(
        fs::read_to_string(session.join("workspace/marker.txt")).unwrap(),
        "main-prepared"
    );
    assert!(session.join("runs").is_dir());
}

#[test]
fn legacy_launch_keeps_daemon_environment_and_shell_is_resolved_explicitly() {
    let temp = tempfile::tempdir().unwrap();
    let mut launch_spec = spec(BTreeMap::new());
    launch_spec.program.clear();
    let options = LaunchOptions {
        shell: Some(config::ShellSpec {
            executable: "/bin/sh".into(),
            script: "printf '%s' \"$HOME\"".into(),
            login: false,
        }),
        ..LaunchOptions::default()
    };
    let prepared =
        prepare_launch(temp.path(), "session-shell-test", &launch_spec, &options).unwrap();
    assert_eq!(prepared.spec.program, "/bin/sh");
    assert_eq!(prepared.spec.args[0], "-c");
    assert_eq!(prepared.spec.args[1], "printf '%s' \"$HOME\"");
    assert_eq!(
        prepared.environment.get("PATH"),
        std::env::var("PATH").ok().as_ref()
    );

    let invalid = LaunchOptions {
        shell: options.shell,
        ..LaunchOptions::default()
    };
    launch_spec.args = vec!["arg".into()];
    assert!(
        prepare_launch(temp.path(), "session-shell-invalid", &launch_spec, &invalid)
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive")
    );
}

#[test]
fn silicogen_template_has_no_credentials_or_model_choice() {
    let profile = profiles::silicogen_test_profile_template();
    assert_eq!(profile.name, "silicogen-test");
    assert!(profile.seeds.is_empty());
    assert!(
        !profile
            .env
            .keys()
            .any(|name| name.contains("KEY") || name.contains("TOKEN"))
    );
    assert!(profile.metadata.contains_key("state_template"));
    assert!(profile.metadata.contains_key("provider_setup"));
}
