use silicogen_terminal_runtime::{engine::EngineState, protocol};
mod screenshot {
    pub use silicogen_terminal_runtime::screenshot::*;
}
#[path = "../src/crop_evidence.rs"]
mod crop_evidence;
#[path = "../src/locators.rs"]
mod locators;
use crop_evidence::*;
use locators::{CellRect, ObservationIdentity};

fn source() -> (protocol::FrameSnapshot, ObservationIdentity) {
    let mut engine = EngineState::new(3, 2).unwrap();
    engine
        .process_output(b"\x1b[48;2;255;0;0m \x1b[48;2;0;255;0m \x1b[48;2;0;0;255m \x1b[0m\r\nABC");
    let mut frame = engine.snapshot("s-private-label", "r", "running");
    frame.cursor.visible = false;
    let identity = ObservationIdentity {
        session_id: frame.session_id.clone(),
        run_id: frame.run_id.clone(),
        frame_id: frame.frame_id.clone(),
        observation_revision: 3,
        viewport_revision: 4,
        cols: 3,
        rows: 2,
    };
    (frame, identity)
}
fn request() -> CropRequest {
    CropRequest {
        region: CellRect {
            row_start: 0,
            col_start: 1,
            row_end: 1,
            col_end: 2,
        },
        scale: 1,
    }
}

#[test]
fn crop_png_has_exact_cell_dimensions_content_and_leaves_authoritative_state_unchanged() {
    let (frame, identity) = source();
    let before = serde_json::to_vec(&frame).unwrap();
    let cells = serde_json::to_vec(&frame.cells).unwrap();
    for scale in 1..=4 {
        let crop = render_crop(&frame, &identity, &CropRequest { scale, ..request() }).unwrap();
        let pixmap = resvg::tiny_skia::Pixmap::decode_png(&crop.png).unwrap();
        assert_eq!(
            (pixmap.width(), pixmap.height()),
            (10 * u32::from(scale), 20 * u32::from(scale))
        );
        assert!(
            pixmap
                .data()
                .chunks_exact(4)
                .all(|pixel| pixel == [0, 255, 0, 255]),
            "labels, border, or adjacent cells leaked into crop"
        );
        assert_eq!(crop.metadata.source, identity);
        assert_eq!(crop.metadata.width_px, pixmap.width());
    }
    assert_eq!(serde_json::to_vec(&frame).unwrap(), before);
    assert_eq!(serde_json::to_vec(&frame.cells).unwrap(), cells);
}

#[test]
fn invalid_regions_scales_identities_and_malformed_underline_are_rejected() {
    let (mut frame, identity) = source();
    for scale in [0, 5, 255] {
        assert!(render_crop(&frame, &identity, &CropRequest { scale, ..request() }).is_err());
    }
    for region in [
        CellRect {
            row_start: 0,
            col_start: 2,
            row_end: 1,
            col_end: 4,
        },
        CellRect {
            row_start: 1,
            col_start: 0,
            row_end: 1,
            col_end: 1,
        },
    ] {
        assert!(render_crop(&frame, &identity, &CropRequest { region, scale: 1 }).is_err());
    }
    let mut wrong = identity.clone();
    wrong.run_id = "other".into();
    assert!(render_crop(&frame, &wrong, &request()).is_err());
    frame.cells[0][1].underline = "single\"/><rect fill=\"red".into();
    assert!(
        render_crop(&frame, &identity, &request())
            .unwrap_err()
            .to_string()
            .contains("underline")
    );
}

#[test]
fn clipping_a_wide_glyph_is_reported_without_expanding_the_region() {
    let (mut frame, identity) = source();
    frame.cells[0][0].text = "界".into();
    frame.cells[0][0].width = 2;
    frame.cells[0][1].text.clear();
    frame.cells[0][1].width = 0;
    let crop = render_crop(&frame, &identity, &request()).unwrap();
    assert!(crop.metadata.wide_glyph_clipped);
    assert_eq!(crop.metadata.width_px, 10);
    assert_eq!(crop.metadata.region, request().region);
}

#[test]
fn oversized_pixel_requests_and_metadata_mismatches_fail_before_unbounded_decode() {
    let (frame, identity) = source();
    let mut engine = EngineState::new(300, 120).unwrap();
    engine.process_output(b"bounded");
    let large = engine.snapshot("s", "r", "running");
    let large_id = ObservationIdentity {
        session_id: "s".into(),
        run_id: "r".into(),
        frame_id: large.frame_id.clone(),
        observation_revision: 1,
        viewport_revision: 1,
        cols: 300,
        rows: 120,
    };
    let large_request = CropRequest {
        region: CellRect {
            row_start: 0,
            col_start: 0,
            row_end: 120,
            col_end: 300,
        },
        scale: 4,
    };
    assert!(
        render_crop(&large, &large_id, &large_request)
            .unwrap_err()
            .to_string()
            .contains("pixel budget")
    );
    let first = render_crop(&frame, &identity, &request()).unwrap();
    let mut forged = render_crop(&frame, &identity, &request()).unwrap();
    forged.metadata.png_sha256 = "not-the-png-hash".into();
    assert!(
        compare_crop_pixels(
            &first,
            &forged,
            PixelTolerance {
                max_channel_delta: 0,
                max_different_pixels: 0
            }
        )
        .is_err()
    );
}

#[test]
fn pixel_comparison_requires_matching_profiles_and_explicit_tolerance() {
    let (frame, identity) = source();
    let first = render_crop(&frame, &identity, &request()).unwrap();
    let second = render_crop(&frame, &identity, &request()).unwrap();
    let exact = PixelTolerance {
        max_channel_delta: 0,
        max_different_pixels: 0,
    };
    assert!(compare_crop_pixels(&first, &second, exact).unwrap().matched);
    let mut changed = frame.clone();
    changed.cells[0][1].resolved_background = "#00fe00".into();
    let changed = render_crop(&changed, &identity, &request()).unwrap();
    assert!(
        !compare_crop_pixels(&first, &changed, exact)
            .unwrap()
            .matched
    );
    assert!(
        compare_crop_pixels(
            &first,
            &changed,
            PixelTolerance {
                max_channel_delta: 1,
                max_different_pixels: 0
            }
        )
        .unwrap()
        .matched
    );
    let mut wrong = second;
    wrong.metadata.font_sha256 = "other-font".into();
    assert!(
        compare_crop_pixels(&first, &wrong, exact)
            .unwrap_err()
            .to_string()
            .contains("incompatible")
    );
    let mut wrong_profile = render_crop(&frame, &identity, &request()).unwrap();
    wrong_profile.metadata.render_profile = "other-render-profile".into();
    assert!(
        compare_crop_pixels(&first, &wrong_profile, exact)
            .unwrap_err()
            .to_string()
            .contains("incompatible")
    );
}
