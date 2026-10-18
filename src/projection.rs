//! Revision-2 observation projections. Legacy RPC payloads are unchanged.
use crate::{
    protocol::CellRegionSnapshot,
    validation::{RequestError, range},
};
use anyhow::Result;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
pub const CONTRACT_REVISION: u16 = 2;
pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    Summary,
    Text,
    Diff,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrameIdentity {
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
#[schemars(extend("examples"=[{"mode":"summary","max_bytes":1024},{"mode":"diff","baseline":{"session_id":"session-id","run_id":"run-id","frame_id":"fr-00000001"},"max_bytes":4096},{"mode":"full","styles":true}]))]
pub struct ResponseOptions {
    pub mode: Option<ResponseMode>,
    /// Compact JSON payload budget; MCP adds at most 512 bytes of bounded presentation metadata.
    #[schemars(range(min = 256, max = 4194304))]
    pub max_bytes: Option<usize>,
    pub styles: Option<bool>,
    /// Required for diff. Identities are run-scoped, not globally unique frame strings.
    pub baseline: Option<FrameIdentity>,
}

impl ResponseOptions {
    pub fn validate(&self, default_mode: ResponseMode) -> Result<ResponseMode> {
        let mode = self.mode.unwrap_or(default_mode);
        if let Some(max) = self.max_bytes {
            range(
                "response.max_bytes",
                max as u64,
                256,
                MAX_RESPONSE_BYTES as u64,
            )?;
        }
        if mode == ResponseMode::Diff && self.baseline.is_none() {
            return Err(RequestError::new(
                "response.baseline",
                "session/run/frame identity for diff",
            )
            .into());
        }
        if mode != ResponseMode::Diff && self.baseline.is_some() {
            return Err(RequestError::new("response.baseline", "only supported for diff").into());
        }
        if mode == ResponseMode::Summary && self.styles == Some(true) {
            return Err(RequestError::new(
                "response.styles",
                "styles are available in text, diff or full mode",
            )
            .into());
        }
        Ok(mode)
    }
    fn budget(&self, mode: ResponseMode, is_wait: bool) -> usize {
        self.max_bytes.unwrap_or(if mode == ResponseMode::Full {
            MAX_RESPONSE_BYTES
        } else if is_wait || mode == ResponseMode::Summary {
            1024
        } else {
            16384
        })
    }
}

/// Project locator/fresh-wait payloads without ever falling back to a raw grid.
pub fn bounded_matches(mut value: Value, options: ResponseOptions, fresh: bool) -> Result<Value> {
    let mode = options.validate(ResponseMode::Summary)?;
    if mode == ResponseMode::Diff {
        return Err(RequestError::new("response.mode", "summary, text, or full")
            .code("unsupported_response")
            .into());
    }
    let budget = options.max_bytes.unwrap_or(match mode {
        ResponseMode::Full => MAX_RESPONSE_BYTES,
        ResponseMode::Text => 16384,
        _ => 1024,
    });
    let original = if fresh {
        value["matches"]["matches"].as_array()
    } else {
        value["matches"].as_array()
    }
    .map_or(0, Vec::len);
    let identity = if fresh {
        &value["matches"]["matches"]
    } else {
        &value["matches"]
    }
    .as_array()
    .and_then(|a| a.first())
    .map(|m| m["observation"].clone());
    if mode == ResponseMode::Summary {
        let any_match = if fresh {
            value["matches"]["any_match"].clone()
        } else {
            value["any_match"].clone()
        };
        let first_match = if fresh {
            value["matches"]["matches"].as_array()
        } else {
            value["matches"].as_array()
        }
        .and_then(|matches| matches.first())
        .map(|item| {
            json!({
                "rect": item["rect"],
                "snippet": truncate(item["snippet"].as_str().unwrap_or(""), 256),
                "observation": item["observation"]
            })
        });
        if fresh {
            value["matches"] = json!({"any_match":any_match,"match_count":original,"observation":identity,"matches":first_match.into_iter().collect::<Vec<_>>(),"truncated":original > 1 || value["matches"]["truncated"] == true});
        } else {
            value["matches"] = first_match.into_iter().collect::<Vec<_>>().into();
            value["match_count"] = json!(original);
            value["truncated"] = json!(original > 1 || value["truncated"] == true);
        }
    } else {
        let items = if fresh {
            &mut value["matches"]["matches"]
        } else {
            &mut value["matches"]
        };
        for item in items.as_array_mut().into_iter().flatten() {
            let snippet = item["snippet"].as_str().unwrap_or("").to_owned();
            if snippet.len() > 16384 {
                let mut boundary = 16384;
                while !snippet.is_char_boundary(boundary) {
                    boundary -= 1;
                }
                item["snippet"] = json!(&snippet[..boundary]);
            }
        }
    }
    let mut truncated = original
        > if fresh {
            value["matches"]["matches"].as_array()
        } else {
            value["matches"].as_array()
        }
        .map_or(0, Vec::len);
    loop {
        let response =
            json!({"contract_revision":2,"mode":mode,"max_bytes":budget,"truncated":truncated});
        value["response"] = response;
        if serde_json::to_vec(&value)?.len() <= budget {
            break;
        }
        if mode == ResponseMode::Full {
            return Err(RequestError::new(
                "response.max_bytes",
                "full locator payload within its byte budget",
            )
            .code("response_budget_exceeded")
            .into());
        }
        if fresh
            && let Some(items) = value["matches"]["matches"].as_array_mut()
            && items.pop().is_some()
        {
            truncated = true;
            value["matches"]["truncated"] = json!(true);
            value["matches"]["omitted_matches"] =
                json!(value["matches"]["omitted_matches"].as_u64().unwrap_or(0) + 1);
            continue;
        } else if !fresh
            && let Some(items) = value["matches"].as_array_mut()
            && items.pop().is_some()
        {
            truncated = true;
            value["truncated"] = json!(true);
            value["omitted_matches"] = json!(value["omitted_matches"].as_u64().unwrap_or(0) + 1);
            continue;
        }
        return Err(RequestError::new(
            "response.max_bytes",
            "required match identity metadata within its byte budget",
        )
        .code("response_budget_exceeded")
        .into());
    }
    Ok(value)
}

struct CachedFrame {
    frame: Value,
    at: Instant,
    bytes: usize,
}
#[derive(Default)]
pub struct ObservationCache {
    frames: VecDeque<CachedFrame>,
    bytes: usize,
}
impl ObservationCache {
    fn expire(&mut self, now: Instant) {
        self.frames
            .retain(|frame| now.saturating_duration_since(frame.at) < Duration::from_secs(60));
        self.bytes = self.frames.iter().map(|frame| frame.bytes).sum();
    }
    fn remember(&mut self, frame: &Value, now: Instant) -> bool {
        let bytes = serde_json::to_vec(frame).map_or(usize::MAX, |v| v.len());
        if bytes > 1024 * 1024 {
            return false;
        }
        let session = &frame["session_id"];
        self.frames.retain(|old| {
            !(old.frame["session_id"] == *session
                && old.frame["run_id"] == frame["run_id"]
                && old.frame["frame_id"] == frame["frame_id"])
        });
        while self
            .frames
            .iter()
            .filter(|old| old.frame["session_id"] == *session)
            .count()
            >= 8
        {
            let index = self
                .frames
                .iter()
                .position(|old| old.frame["session_id"] == *session)
                .unwrap();
            self.frames.remove(index);
        }
        self.bytes = self.frames.iter().map(|old| old.bytes).sum();
        while self.frames.len() >= 32 || self.bytes.saturating_add(bytes) > 8 * 1024 * 1024 {
            if let Some(old) = self.frames.pop_front() {
                self.bytes = self.bytes.saturating_sub(old.bytes);
            } else {
                break;
            }
        }
        self.frames.push_back(CachedFrame {
            frame: frame.clone(),
            at: now,
            bytes,
        });
        self.bytes += bytes;
        true
    }
    pub fn project(
        &mut self,
        value: Value,
        options: ResponseOptions,
        is_wait: bool,
        query: Option<(&str, bool)>,
        now: Instant,
    ) -> Result<Value> {
        self.expire(now);
        let mode = options.validate(if is_wait {
            ResponseMode::Summary
        } else {
            ResponseMode::Text
        })?;
        let budget = options.budget(mode, is_wait);
        let frame = if is_wait {
            value.get("snapshot").cloned()
        } else {
            Some(value.clone())
        };
        let Some(frame) = frame.filter(|frame| frame["text"].is_string()) else {
            if mode == ResponseMode::Diff {
                return Err(
                    RequestError::new("response.baseline", "current observable frame")
                        .code("baseline_unavailable")
                        .into(),
                );
            }
            let mut out = value;
            out["response"] = metadata(mode, budget, false);
            return fit(out, budget, mode);
        };
        let mut out = if mode == ResponseMode::Full {
            value.clone()
        } else if is_wait {
            let mut out = value.clone();
            let object = out.as_object_mut().unwrap();
            object.remove("snapshot");
            for field in [
                "matched_markers",
                "missing_markers",
                "busy_markers",
                "crash_markers",
            ] {
                object.remove(field);
            }
            out
        } else {
            json!({"status":"observed"})
        };
        if mode != ResponseMode::Full {
            for field in [
                "session_id",
                "run_id",
                "frame_id",
                "output_sequence",
                "cols",
                "rows",
                "process_state",
                "observation_revision",
                "viewport_revision",
            ] {
                out[field] = frame[field].clone();
            }
        }
        let styles = options.styles.unwrap_or(mode == ResponseMode::Full);
        if mode == ResponseMode::Text {
            out["text"] = frame["text"].clone();
            out["cursor"] = frame["cursor"].clone();
            out["modes"] = frame["modes"].clone();
            if styles {
                out["base_style"] = frame["base_style"].clone();
                out["styled_runs"] = frame["styled_runs"].clone();
            }
        } else if mode == ResponseMode::Diff {
            let id = options.baseline.as_ref().unwrap();
            if id.session_id != frame["session_id"] || id.run_id != frame["run_id"] {
                return Err(RequestError::new(
                    "response.baseline",
                    "same session and application run",
                )
                .code("baseline_incompatible")
                .into());
            }
            let old = self
                .frames
                .iter()
                .find(|old| {
                    old.frame["session_id"] == id.session_id
                        && old.frame["run_id"] == id.run_id
                        && old.frame["frame_id"] == id.frame_id
                })
                .ok_or_else(|| {
                    RequestError::new(
                        "response.baseline",
                        "a retained observation (last 8/session, at most 60 seconds)",
                    )
                    .code("baseline_unavailable")
                })?;
            if old.frame["cols"] != frame["cols"] || old.frame["rows"] != frame["rows"] {
                return Err(
                    RequestError::new("response.baseline", "same terminal dimensions")
                        .code("baseline_incompatible")
                        .into(),
                );
            }
            let before: Vec<_> = old.frame["text"].as_str().unwrap().lines().collect();
            let after: Vec<_> = frame["text"].as_str().unwrap().lines().collect();
            let changed: Vec<_> = after
                .iter()
                .enumerate()
                .filter(|(row, line)| before.get(*row) != Some(line))
                .map(|(row, text)| json!({"row":row,"text":text}))
                .collect();
            out["baseline"] = serde_json::to_value(id)?;
            out["changed_rows"] = json!(changed.len());
            out["changes"] = json!(changed);
            if styles {
                out["base_style"] = frame["base_style"].clone();
                out["styled_runs"] = frame["styled_runs"].clone();
                out["style_change"] = json!(
                    frame["styled_runs"] != old.frame["styled_runs"]
                        || frame["base_style"] != old.frame["base_style"]
                );
            }
        } else if mode == ResponseMode::Full && !styles {
            let target = if is_wait {
                &mut out["snapshot"]
            } else {
                &mut out
            };
            if let Some(object) = target.as_object_mut() {
                object.remove("styled_runs");
                object.remove("base_style");
                object.remove("palette");
            }
        }
        if is_wait
            && value["matched"] == true
            && mode == ResponseMode::Summary
            && let Some((pattern, is_regex)) = query
        {
            let regex = is_regex.then(|| regex::Regex::new(pattern).ok()).flatten();
            if let Some((row, line)) =
                frame["text"]
                    .as_str()
                    .unwrap()
                    .lines()
                    .enumerate()
                    .find(|(_, line)| {
                        if let Some(re) = &regex {
                            re.is_match(line)
                        } else {
                            line.contains(pattern)
                        }
                    })
            {
                out["match"] = json!({"row":row,"text":truncate(line,120)});
            }
        }
        out["response"] = metadata(mode, budget, styles);
        let mut out = fit(out, budget, mode)?;
        out["response"]["baseline_retained"] = json!(self.remember(&frame, now));
        fit(out, budget, mode)
    }
}

pub fn cells(region: CellRegionSnapshot, options: ResponseOptions) -> Result<Value> {
    let mode = options.validate(ResponseMode::Text)?;
    if mode == ResponseMode::Diff {
        return Err(
            RequestError::new("response.mode", "use terminal_observe for frame diffs")
                .code("unsupported_response")
                .into(),
        );
    }
    let budget = options.max_bytes.unwrap_or(if mode == ResponseMode::Full {
        MAX_RESPONSE_BYTES
    } else {
        4096
    });
    let (styles, grid) = crate::frame_stream::compact_cells(&region.cells)
        .ok_or_else(|| RequestError::new("response.styles", "encodable compact terminal styles"))?;
    let include_styles = options.styles.unwrap_or(mode != ResponseMode::Summary);
    let grid: Vec<Vec<Value>> = grid
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    if include_styles {
                        json!([cell.0, cell.1, cell.2, cell.3, cell.4])
                    } else {
                        json!([cell.0, cell.1])
                    }
                })
                .collect()
        })
        .collect();
    let mut out = json!({"session_id":region.session_id,"run_id":region.run_id,"frame_id":region.frame_id,"row":region.row,"col":region.col,"rows":region.rows,"cols":region.cols,"cells":grid,"response":metadata(mode,budget,include_styles)});
    if mode == ResponseMode::Summary {
        out.as_object_mut().unwrap().remove("cells");
    } else if include_styles {
        out["styles"] = json!(styles);
    }
    fit(out, budget, mode)
}

fn metadata(mode: ResponseMode, budget: usize, styles: bool) -> Value {
    json!({"contract_revision":CONTRACT_REVISION,"mode":mode,"max_bytes":budget,"styles":styles,"truncated":false})
}
pub fn named_diff(value: Value, options: ResponseOptions) -> Result<Value> {
    let mode = options.validate(ResponseMode::Summary)?;
    validate_named_diff(&options)?;
    let budget = options.budget(mode, false);
    let mut out = if mode == ResponseMode::Full {
        value.clone()
    } else {
        let mut out = json!({});
        for key in [
            "passed",
            "changed_cell_count",
            "actual_session_id",
            "actual_run_id",
            "actual_frame_id",
            "expected_baseline_id",
            "cols",
            "rows",
            "pixel_comparison",
        ] {
            out[key] = value[key].clone();
        }
        out
    };
    if mode == ResponseMode::Text {
        out["changes"]=json!(value["changed_cells"].as_array().into_iter().flatten().map(|cell|json!({"row":cell["row"],"col":cell["col"],"expected":cell["expected"]["text"],"actual":cell["actual"]["text"]})).collect::<Vec<_>>());
    }
    if mode == ResponseMode::Full && options.styles == Some(false) {
        for cell in out["changed_cells"].as_array_mut().into_iter().flatten() {
            for side in ["expected", "actual"] {
                cell[side] = json!({"text":cell[side]["text"],"width":cell[side]["width"]});
            }
        }
    }
    out["response"] = metadata(
        mode,
        budget,
        options.styles.unwrap_or(mode == ResponseMode::Full),
    );
    fit(out, budget, mode)
}
pub fn validate_named_diff(options: &ResponseOptions) -> Result<()> {
    let mode = options.mode.unwrap_or(ResponseMode::Summary);
    if mode == ResponseMode::Diff {
        return Err(RequestError::new("response.mode","named-frame comparisons support summary, text or full; use observe for observation-baseline diffs").code("unsupported_response").into());
    }
    if mode == ResponseMode::Text && options.styles == Some(true) {
        return Err(RequestError::new(
            "response.styles",
            "full mode for named-frame style differences",
        )
        .code("unsupported_response")
        .into());
    }
    Ok(())
}
pub(crate) fn truncate(text: &str, max: usize) -> &str {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
fn fit(mut value: Value, budget: usize, mode: ResponseMode) -> Result<Value> {
    let mut omitted = 0;
    loop {
        if serde_json::to_vec(&value)?.len() <= budget {
            return Ok(value);
        }
        if mode == ResponseMode::Full {
            return Err(RequestError::new(
                "response.max_bytes",
                "enough bytes for the explicitly requested full response",
            )
            .code("response_budget_exceeded")
            .into());
        }
        value["response"]["truncated"] = json!(true);
        if let Some(runs) = value
            .get_mut("styled_runs")
            .and_then(Value::as_array_mut)
            .filter(|v| !v.is_empty())
        {
            runs.pop();
            omitted += 1;
        } else if let Some(changes) = value
            .get_mut("changes")
            .and_then(Value::as_array_mut)
            .filter(|v| !v.is_empty())
        {
            changes.pop();
            omitted += 1;
        } else if let Some(cells) = value
            .get_mut("cells")
            .and_then(Value::as_array_mut)
            .filter(|v| !v.is_empty())
        {
            cells.pop();
            omitted += 1;
        } else if let Some(text) = value
            .get("text")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        {
            let shorter = truncate(text, text.len() / 2).to_owned();
            omitted += text.len() - shorter.len();
            value["text"] = json!(shorter);
        } else if value.get("match").is_some() {
            value.as_object_mut().unwrap().remove("match");
            omitted += 1;
        } else {
            return Err(RequestError::new(
                "response.max_bytes",
                "enough bytes for required identity/status metadata",
            )
            .code("response_budget_exceeded")
            .into());
        }
        value["response"]["omitted_units"] = json!(omitted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(id: usize) -> Value {
        json!({"session_id":"s","run_id":"r","frame_id":format!("f-{id}"),"cols":10,"rows":1,"text":format!("frame {id}"),"styled_runs":[],"base_style":{}})
    }
    fn diff_options(id: usize) -> ResponseOptions {
        ResponseOptions {
            mode: Some(ResponseMode::Diff),
            baseline: Some(FrameIdentity {
                session_id: "s".into(),
                run_id: "r".into(),
                frame_id: format!("f-{id}"),
            }),
            ..Default::default()
        }
    }
    #[test]
    fn expired_and_evicted_baselines_return_typed_errors() {
        let now = Instant::now();
        let mut cache = ObservationCache::default();
        cache
            .project(frame(0), Default::default(), false, None, now)
            .unwrap();
        let expired = cache
            .project(
                frame(1),
                diff_options(0),
                false,
                None,
                now + Duration::from_secs(61),
            )
            .unwrap_err();
        assert_eq!(
            expired.downcast_ref::<RequestError>().unwrap().code,
            "baseline_unavailable"
        );
        for id in 0..9 {
            cache
                .project(
                    frame(id),
                    Default::default(),
                    false,
                    None,
                    now + Duration::from_secs(62),
                )
                .unwrap();
        }
        let evicted = cache
            .project(
                frame(10),
                diff_options(0),
                false,
                None,
                now + Duration::from_secs(62),
            )
            .unwrap_err();
        assert_eq!(
            evicted.downcast_ref::<RequestError>().unwrap().code,
            "baseline_unavailable"
        );
        assert!(
            cache
                .project(
                    frame(10),
                    diff_options(8),
                    false,
                    None,
                    now + Duration::from_secs(62)
                )
                .is_ok()
        );
    }

    #[test]
    fn summary_locator_keeps_one_bounded_location_and_utf8_snippet() {
        let observation = json!({
            "session_id":"session",
            "run_id":"run",
            "frame_id":"frame-1",
            "observation_revision":7,
            "viewport_revision":2,
            "cols":80,
            "rows":24
        });
        let long_snippet = "界".repeat(200);
        let input = json!({
            "matches":[
                {"rect":{"row_start":4,"col_start":3,"row_end":4,"col_end":8},"cell_rects":[],"snippet":long_snippet,"observation":observation},
                {"rect":{"row_start":8,"col_start":0,"row_end":8,"col_end":4},"cell_rects":[],"snippet":"second","observation":observation}
            ],
            "any_match":true,
            "truncated":false,
            "omitted_matches":0
        });
        let projected = bounded_matches(input, ResponseOptions::default(), false).unwrap();
        assert_eq!(projected["match_count"], 2);
        assert_eq!(projected["truncated"], true);
        assert_eq!(projected["matches"].as_array().unwrap().len(), 1);
        assert_eq!(projected["matches"][0]["rect"]["row_start"], 4);
        assert!(projected["matches"][0]["snippet"].as_str().unwrap().len() <= 256);
        assert_eq!(
            projected["matches"][0]["observation"]["frame_id"],
            "frame-1"
        );
        assert!(serde_json::to_vec(&projected).unwrap().len() <= 1024);
    }
}
