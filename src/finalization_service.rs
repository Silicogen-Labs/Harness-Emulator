//! Outcome finalization orchestration, isolated from unrelated agent operations.
use super::{AutomationService, encode, read_json, remove_historical_credentials};
use crate::{
    artifacts::ArtifactSourceKind, protocol::RpcRequest, retention::RetentionClass,
    service::SessionService, session::Session,
};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

impl AutomationService {
    pub(crate) fn finalize(
        &self,
        service: &SessionService,
        args: crate::finalization::FinalizeArgs,
    ) -> Result<Value> {
        use crate::finalization::{FinalizationRecord, Outcome};
        let _serial = self
            .finalizations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = service.active_session(&args.session_id).ok();
        let summary = if let Some(session) = &session {
            serde_json::to_value(session.summary())?
        } else {
            service
                .handle(RpcRequest::Status {
                    session_id: args.session_id.clone(),
                })?
                .0
        };
        if summary["run_id"] != args.expected_run_id {
            bail!("stale_run: finalization targets an earlier application run");
        }
        self.stop_diagnostics_owner(&args.session_id, &args.expected_run_id)?;
        let session_dir = self.root.join("sessions").join(&args.session_id);
        if let Err(error) = crate::profiles::ensure_no_symlink_components(&session_dir) {
            return self.reject_unreadable_journal(session.as_deref(), &args, error);
        }
        let record_path = session_dir.join("finalization.json");
        let (record_exists, loaded_record) = match std::fs::symlink_metadata(&record_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, None),
            Err(error) => {
                return self.reject_unreadable_journal(session.as_deref(), &args, error.into());
            }
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return self.reject_unreadable_journal(
                    session.as_deref(),
                    &args,
                    anyhow::anyhow!("finalization journal is not a regular file"),
                );
            }
            Ok(_) => {
                let parsed = crate::profiles::ensure_no_symlink_components(&record_path)
                    .and_then(|()| std::fs::read(&record_path).map_err(Into::into))
                    .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into));
                match parsed {
                    Ok(record) => (true, Some(record)),
                    Err(error) => {
                        return self.reject_unreadable_journal(session.as_deref(), &args, error);
                    }
                }
            }
        };
        let mut record: FinalizationRecord =
            loaded_record.unwrap_or_else(|| FinalizationRecord::new(&args));
        record.check(&args)?;
        if matches!(record.disposal.as_str(), "retained" | "disposed") {
            if let Some(session) = &session {
                session.stop_for_finalization(&args)?;
            }
            self.restore_final_evidence(&record)?;
            return encode(record);
        }
        if let Some(session) = &session {
            self.stop_and_persist_journal(session, &args, &record, &record_path)?;
        } else if !record_exists {
            self.persist_finalization_record(&args.session_id, &record_path, &record)?;
        }

        let manifest: Value = match read_json(&session_dir.join("launch.json")) {
            Ok(manifest) if manifest.is_object() && manifest["options"].is_object() => manifest,
            _ => {
                return self.fail_finalization_metadata(
                    &args,
                    &record_path,
                    record,
                    "launch metadata is unreadable or invalid",
                );
            }
        };
        let diagnostics: Value = match read_json(&session_dir.join("launch-diagnostics.json")) {
            Ok(diagnostics) if diagnostics.is_object() => diagnostics,
            _ => {
                return self.fail_finalization_metadata(
                    &args,
                    &record_path,
                    record,
                    "launch diagnostics are unreadable or invalid",
                );
            }
        };
        let dispose = args.outcome == Outcome::Passed
            && (manifest["options"]["disposable"] == true
                || diagnostics["test_policy"]["dispose_on_pass"] == true);
        if let Some(session) = &session {
            if record
                .evidence
                .as_ref()
                .is_none_or(|evidence| evidence["capture_failed"] == true)
            {
                let _lease = session.retain_evidence()?;
                let capture = self
                    .check_capacity(service, 2 * 1024 * 1024, 0)
                    .and_then(|()| self.capture_final_evidence(session, &session_dir));
                match capture {
                    Ok(evidence) => record.evidence = Some(evidence),
                    Err(_) => {
                        record.evidence = Some(json!({
                            "session_id": args.session_id,
                            "run_id": args.expected_run_id,
                            "sanitized": false,
                            "sanitization": "not_certified",
                            "complete": false,
                            "capture_failed": true,
                            "warning": "final evidence capture exceeded available artifact or retention quota"
                        }));
                        record.disposal = "failed".into();
                        record.reason = Some("process stopped and outcome journaled; bounded evidence capture failed, copies retained with failure retention; retry identical finalization after releasing quota".into());
                        self.persist_finalization_record(&args.session_id, &record_path, &record)?;
                        self.set_outcome_retention(&args.session_id, Outcome::Failed)?;
                        return encode(record);
                    }
                }
                self.persist_finalization_record(&args.session_id, &record_path, &record)?;
            }
        } else {
            if !record_exists {
                self.persist_finalization_record(&args.session_id, &record_path, &record)?;
            }
            if record
                .evidence
                .as_ref()
                .is_none_or(|evidence| evidence["capture_failed"] == true)
            {
                let capture = self
                    .check_capacity(service, 2 * 1024 * 1024, 0)
                    .and_then(|()| {
                        self.capture_closed_final_evidence(
                            &args.session_id,
                            &args.expected_run_id,
                            &session_dir,
                        )
                    });
                match capture {
                    Ok(evidence) => {
                        record.evidence = Some(evidence);
                        self.persist_finalization_record(&args.session_id, &record_path, &record)?;
                    }
                    Err(_) => {
                        record.evidence = Some(json!({
                            "session_id": args.session_id,
                            "run_id": args.expected_run_id,
                            "sanitized": false,
                            "sanitization": "not_certified",
                            "complete": false,
                            "capture_failed": true,
                            "warning": "bounded closed-session evidence is unavailable or exceeded quota"
                        }));
                        record.disposal = "failed".into();
                        record.reason = Some("closed-session evidence is unavailable or incomplete; private copies retained with failure retention".into());
                        self.persist_finalization_record(&args.session_id, &record_path, &record)?;
                        self.set_outcome_retention(&args.session_id, Outcome::Failed)?;
                        return encode(record);
                    }
                }
            }
        }
        if args.outcome == Outcome::Passed
            && record
                .evidence
                .as_ref()
                .is_none_or(|evidence| evidence["complete"] != true)
        {
            record.disposal = "failed".into();
            record.reason = Some(
                "passed outcome cannot dispose private copies because required final evidence is incomplete".into(),
            );
            self.persist_finalization_record(&args.session_id, &record_path, &record)?;
            self.set_outcome_retention(&args.session_id, Outcome::Failed)?;
            return encode(record);
        }
        let protected = self.protected_jobs();
        if protected.iter().any(|path| path == &session_dir) {
            record.disposal = "pending".into();
            record.reason =
                Some("active scenario still owns session evidence; retry after it finishes".into());
        } else {
            let reclamation = service.begin_reclamation(&args.session_id);
            match reclamation {
                Err(_) => {
                    record.disposal = "pending".into();
                    record.reason = Some(
                        "viewer or evidence lease still holds the session; retry after release"
                            .into(),
                    );
                }
                Ok(_gate) => {
                    // _gate excludes new viewer/evidence access until deletion and journal
                    // update finish. Dropping it reopens read-only access to retained evidence.
                    let removal = (|| -> Result<()> {
                        if dispose {
                            let workspace_relative = manifest["options"]["workspace"]
                                .is_object()
                                .then(|| PathBuf::from("workspaces").join(&args.session_id));
                            self.retention
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .dispose_test_allocation(
                                    &PathBuf::from("sessions")
                                        .join(&args.session_id)
                                        .join("state"),
                                    workspace_relative.as_deref(),
                                )?;
                        } else if let Some(session) = &session {
                            session.remove_seed_credentials()?;
                        } else {
                            remove_historical_credentials(&session_dir, &manifest)?;
                        }
                        Ok(())
                    })();
                    if removal.is_ok() {
                        record.disposal = if dispose { "disposed" } else { "retained" }.into();
                        record.reason = None;
                    } else {
                        record.disposal = "failed".into();
                        record.reason = Some("private copy disposal or credential removal failed; evidence retained, retry identical finalization".into());
                    }
                    self.set_outcome_retention(
                        &args.session_id,
                        if record.disposal == "failed" {
                            Outcome::Failed
                        } else {
                            args.outcome
                        },
                    )?;
                    self.persist_finalization_record(&args.session_id, &record_path, &record)?;
                    return encode(record);
                }
            }
        }
        self.persist_finalization_record(&args.session_id, &record_path, &record)?;
        encode(record)
    }

    fn stop_and_persist_journal(
        &self,
        session: &Session,
        args: &crate::finalization::FinalizeArgs,
        record: &crate::finalization::FinalizationRecord,
        path: &Path,
    ) -> Result<()> {
        self.stop_and_persist_journal_with(session, args, record, path, |path, record| {
            crate::session::atomic_json_write(path, record)
        })
    }

    fn stop_and_persist_journal_with<F>(
        &self,
        session: &Session,
        args: &crate::finalization::FinalizeArgs,
        record: &crate::finalization::FinalizationRecord,
        path: &Path,
        persist: F,
    ) -> Result<()>
    where
        F: FnOnce(&Path, &crate::finalization::FinalizationRecord) -> Result<()>,
    {
        let was_running = session.summary().state == "running";
        if let Err(error) = session.stop_for_finalization(args) {
            if was_running && session.summary().state == "closed" {
                self.set_outcome_retention(&args.session_id, crate::finalization::Outcome::Failed)?;
            }
            return Err(error).context("authorized finalization stop did not complete cleanly");
        }
        if let Err(error) = persist(path, record) {
            self.set_outcome_retention(&args.session_id, crate::finalization::Outcome::Failed)?;
            return Err(error).context(
                "finalization journal write failed after the owned process stopped; private copies retained with failure retention",
            );
        }
        Ok(())
    }

    fn persist_finalization_record(
        &self,
        session_id: &str,
        path: &Path,
        record: &crate::finalization::FinalizationRecord,
    ) -> Result<()> {
        if let Err(error) = crate::session::atomic_json_write(path, record) {
            self.set_outcome_retention(session_id, crate::finalization::Outcome::Failed)?;
            return Err(error).context(
                "finalization journal update failed; private copies retained with failure retention",
            );
        }
        Ok(())
    }

    fn reject_unreadable_journal(
        &self,
        session: Option<&Session>,
        args: &crate::finalization::FinalizeArgs,
        journal_error: anyhow::Error,
    ) -> Result<Value> {
        if let Some(session) = session {
            let was_running = session.summary().state == "running";
            if let Err(stop_error) = session.stop_for_finalization(args) {
                if was_running && session.summary().state == "closed" {
                    self.set_outcome_retention(
                        &args.session_id,
                        crate::finalization::Outcome::Failed,
                    )?;
                }
                return Err(stop_error).context(
                    "could not safely stop workload; existing journal was left untouched",
                );
            }
        }
        self.set_outcome_retention(&args.session_id, crate::finalization::Outcome::Failed)?;
        Err(journal_error).context(
            "existing finalization journal is unreadable and was left untouched; private copies retained",
        )
    }

    fn fail_finalization_metadata(
        &self,
        args: &crate::finalization::FinalizeArgs,
        record_path: &Path,
        mut record: crate::finalization::FinalizationRecord,
        warning: &str,
    ) -> Result<Value> {
        record.evidence = Some(json!({
            "session_id":args.session_id,
            "run_id":args.expected_run_id,
            "sanitized":false,
            "sanitization":"not_certified",
            "complete":false,
            "capture_failed":true,
            "warning":warning
        }));
        record.disposal = "failed".into();
        record.reason = Some(
            "required launch metadata could not be trusted; private copies retained with failure retention".into(),
        );
        self.persist_finalization_record(&args.session_id, record_path, &record)?;
        self.set_outcome_retention(&args.session_id, crate::finalization::Outcome::Failed)?;
        encode(record)
    }

    fn set_outcome_retention(&self, id: &str, outcome: crate::finalization::Outcome) -> Result<()> {
        let mut registry = self
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let class = if outcome == crate::finalization::Outcome::Passed {
            RetentionClass::Successful
        } else {
            RetentionClass::Failed
        };
        let entries: Vec<_> = registry
            .entries()
            .iter()
            .filter(|entry| {
                ["sessions", "artifacts", "workspaces"]
                    .iter()
                    .any(|category| entry.relative_path == PathBuf::from(category).join(id))
            })
            .map(|entry| (entry.id.clone(), entry.pinned))
            .collect();
        for (id, pinned) in entries {
            registry.set_retention_at(&id, class, pinned, crate::engine::now_ms())?;
        }
        Ok(())
    }

    pub(super) fn restore_final_evidence(
        &self,
        record: &crate::finalization::FinalizationRecord,
    ) -> Result<()> {
        let Some(evidence) = &record.evidence else {
            return Ok(());
        };
        if let Some(id) = evidence["artifact_id"].as_str()
            && self.artifacts.metadata(&record.session_id, id).is_err()
        {
            let directory = self
                .root
                .join("artifacts")
                .join(&record.session_id)
                .join(&record.run_id);
            let root = self
                .artifacts
                .register_session_evidence(&record.session_id, &directory)?;
            self.artifacts.register_evidence_file_with_id(
                &record.session_id,
                &root,
                Path::new("final-evidence.json"),
                id,
                ArtifactSourceKind::Other,
                "application/json",
                Some(record.run_id.clone()),
                Some("outcome retention".into()),
            )?;
        }
        if !evidence["screenshot"].is_null() {
            self.register_capture(&serde_json::from_value(evidence["screenshot"].clone())?)?;
        }
        Ok(())
    }

    fn capture_closed_final_evidence(
        &self,
        session_id: &str,
        expected_run_id: &str,
        session_dir: &Path,
    ) -> Result<Value> {
        const CLOSED_PNG_MAX: u64 = 384 * 1024;
        let closed_path = session_dir.join("closed-evidence.json");
        crate::profiles::ensure_no_symlink_components(&closed_path)?;
        let closed = read_json(&closed_path)?;
        if closed["version"] != 1
            || closed["session_id"] != session_id
            || closed["run_id"] != expected_run_id
        {
            bail!("closed_evidence_identity_mismatch: snapshot does not belong to this run")
        }
        if closed["complete"] != true || closed["screenshot"].is_null() {
            bail!("closed_evidence_incomplete: required bounded close capture is unavailable")
        }

        let screenshot: crate::protocol::ScreenshotArtifact =
            serde_json::from_value(closed["screenshot"].clone())?;
        if screenshot.session_id != session_id || screenshot.run_id != expected_run_id {
            bail!("closed_evidence_identity_mismatch: screenshot belongs to another run")
        }
        let screenshot_path = PathBuf::from(&screenshot.path);
        let relative = screenshot_path
            .strip_prefix(session_dir)
            .context("closed screenshot escaped its session directory")?;
        crate::artifacts::validate_relative(relative)?;
        crate::profiles::ensure_no_symlink_components(&screenshot_path)?;
        let png = std::fs::read(&screenshot_path)?;
        if png.len() as u64 > CLOSED_PNG_MAX {
            bail!("closed_evidence_size: screenshot exceeds its bounded size")
        }
        let digest = Sha256::digest(&png);
        let actual_sha256 = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if actual_sha256 != screenshot.sha256 {
            bail!("closed_evidence_hash_mismatch: screenshot changed after close")
        }

        let screenshot_artifact = self.register_file(
            session_id,
            &screenshot_path,
            ArtifactSourceKind::Screenshot,
            Some(expected_run_id.to_owned()),
        )?;
        let mut screenshot_value = serde_json::to_value(screenshot)?;
        screenshot_value["artifact_id"] = json!(screenshot_artifact);
        let changes = closed["changed_files"].clone();
        let assertions_complete = closed["assertions_complete"] == true;
        let complete =
            closed["complete"] == true && assertions_complete && changes["complete"] == true;
        let evidence = json!({
            "session_id":session_id,
            "run_id":expected_run_id,
            "frame_id":closed["frame_id"],
            "text":closed["text"],
            "actions":closed["actions"],
            "history_truncated":closed["history_truncated"],
            "sanitized":true,
            "source_frame_id":closed["frame_id"],
            "raw_recording_enabled":closed["raw_recording_enabled"],
            "raw_bytes_in_bundle":false,
            "assertions":closed["assertions"],
            "assertions_complete":assertions_complete,
            "changed_files":changes,
            "log_tail":closed["log_tail"],
            "log_tail_available":closed["log_tail_available"],
            "sanitization":"known selected credentials and explicit secret environment values; not a general secret detector",
            "complete":complete
        });
        if serde_json::to_vec(&evidence)?.len() > 1024 * 1024 {
            bail!("finalization_evidence_limit: bounded closed evidence exceeds 1 MiB")
        }
        let directory = self
            .root
            .join("artifacts")
            .join(session_id)
            .join(expected_run_id);
        let text_path = directory.join("final-evidence.json");
        crate::session::atomic_json_write(&text_path, &evidence)?;
        let artifact = self.register_file(
            session_id,
            &text_path,
            ArtifactSourceKind::Other,
            Some(expected_run_id.to_owned()),
        )?;
        Ok(json!({
            "artifact_id":artifact,
            "screenshot":screenshot_value,
            "sanitized":true,
            "source_frame_id":closed["frame_id"],
            "complete":complete
        }))
    }

    fn capture_final_evidence(&self, session: &Session, session_dir: &Path) -> Result<Value> {
        use std::io::{Read, Seek, SeekFrom};
        if session.capabilities()["launch"]["credential_directory_seed_count"]
            .as_u64()
            .unwrap_or_default()
            > 0
        {
            bail!(
                "legacy_credential_directory: use explicit selected credential files; broad directory imports cannot be certified as sanitized evidence"
            );
        }
        let redactor = session.final_evidence_redactor()?;
        let frame = session.sanitized_final_frame(&redactor);
        let screenshot = session.screenshot_snapshot(&frame)?;
        self.register_capture(&screenshot)?;
        let directory = self
            .root
            .join("artifacts")
            .join(&session.id)
            .join(&frame.run_id);
        let text_path = directory.join("final-evidence.json");
        let history = session.history();
        let actions: Vec<_> = history.events.iter().filter(|event| event.kind != "pty_output").map(|event| json!({
            "sequence":event.sequence,"at_ms":event.at_ms,"kind":event.kind,"byte_count":event.byte_count,
            "sha256":event.sha256,"detail":event.detail.as_ref().map(|detail| redactor.text(detail))
        })).collect();
        let capabilities = session.capabilities();
        let workspace = capabilities["launch"]["workspace"]["root"].as_str();
        let changes = if let Some(root) = workspace {
            crate::finalization::changed_files(
                &read_json(&session_dir.join("workspace-baseline.json"))?,
                &crate::finalization::workspace_manifest(Path::new(root))?,
            )
        } else {
            json!({"files":{},"complete":true})
        };
        let relative_log = capabilities["launch"]["app_log_root"]
            .as_str()
            .unwrap_or("data/silicogen/log");
        let log = session_dir.join("state").join(relative_log).join("dev.log");
        let log_tail = (|| -> Result<String> {
            crate::profiles::ensure_no_symlink_components(&log)?;
            let mut file = std::fs::File::open(&log)?;
            let length = file.metadata()?.len();
            file.seek(SeekFrom::Start(length.saturating_sub(16384)))?;
            let mut bytes = Vec::new();
            file.take(16384).read_to_end(&mut bytes)?;
            Ok(redactor.text(&String::from_utf8_lossy(&bytes)))
        })();
        let (assertions, assertions_complete) = crate::finalization::load_sanitized_assertions(
            &session_dir.join("assertions.json"),
            &redactor,
        );
        let evidence = json!({"session_id":session.id,"run_id":frame.run_id,"frame_id":frame.frame_id,
            "text":frame.text,"actions":actions,"history_truncated":history.truncated,
            "sanitized":true,"source_frame_id":frame.frame_id,
            "raw_recording_enabled":history.raw_recording_enabled,"raw_bytes_in_bundle":false,
            "assertions":assertions,"assertions_complete":assertions_complete,
            "changed_files":changes,"log_tail":log_tail.as_ref().ok(),
            "log_tail_available":log_tail.is_ok(),"sanitization":"known selected credentials and explicit secret environment values; not a general secret detector",
            "complete":frame.capture_complete && !history.truncated && assertions_complete && changes["complete"] == true});
        let encoded = serde_json::to_vec(&evidence)?;
        if encoded.len() > 1024 * 1024 {
            bail!("finalization_evidence_limit: bounded evidence exceeds 1 MiB");
        }
        let _budget = session.evidence_budget_guard(encoded.len() as u64)?;
        crate::session::atomic_json_write(&text_path, &evidence)?;
        let artifact = self.register_file(
            &session.id,
            &text_path,
            ArtifactSourceKind::Other,
            Some(frame.run_id),
        )?;
        Ok(
            json!({"artifact_id":artifact,"screenshot":screenshot,"sanitized":true,"source_frame_id":frame.frame_id,"complete":evidence["complete"]}),
        )
    }
}

#[cfg(test)]
mod journal_failure_tests {
    use super::*;
    use crate::{
        config::LaunchOptions,
        finalization::{FinalizationRecord, FinalizeArgs, Outcome},
        protocol::{LaunchSpec, RpcRequest},
        retention::{RetentionClass, RetentionRegistry},
    };
    use std::{collections::BTreeMap, fs};

    #[test]
    fn initial_journal_write_failure_stops_authorized_work_and_retains_copies() {
        let runtime_parent =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/finalization-journal-tests");
        fs::create_dir_all(&runtime_parent).unwrap();
        let temp = tempfile::tempdir_in(runtime_parent).unwrap();
        let runtime = temp.path().join("runtime");
        let service = SessionService::new(&runtime).unwrap();
        let launched = service
            .handle(RpcRequest::LaunchConfigured {
                spec: LaunchSpec {
                    name: Some("journal-write-failure".into()),
                    program: "/bin/sh".into(),
                    args: vec!["-c".into(), "printf READY; sleep 60".into()],
                    cwd: temp.path().display().to_string(),
                    env: BTreeMap::new(),
                    cols: 80,
                    rows: 24,
                    record_output: false,
                },
                options: LaunchOptions::default(),
            })
            .unwrap()
            .0;
        let session_id = launched["session_id"].as_str().unwrap();
        let session = service.active_session(session_id).unwrap();
        let args = FinalizeArgs {
            session_id: session_id.into(),
            expected_run_id: launched["run_id"].as_str().unwrap().into(),
            outcome: Outcome::Passed,
            actor_id: None,
        };
        let record = FinalizationRecord::new(&args);
        let journal_path = runtime
            .join("sessions")
            .join(session_id)
            .join("finalization.json");
        let error = service
            .agent
            .stop_and_persist_journal_with(&session, &args, &record, &journal_path, |_, _| {
                Err(anyhow::anyhow!("synthetic initial journal write failure"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("journal write failed after"));
        assert_eq!(session.summary().state, "closed");
        assert!(
            runtime
                .join("sessions")
                .join(session_id)
                .join("state")
                .is_dir()
        );
        assert!(!journal_path.exists());
        let registry = RetentionRegistry::open(&runtime).unwrap();
        let session_owner = registry
            .entries()
            .iter()
            .find(|entry| entry.relative_path == PathBuf::from("sessions").join(session_id))
            .unwrap();
        assert_eq!(session_owner.retention, RetentionClass::Failed);
        service.shutdown().unwrap();
    }
}
