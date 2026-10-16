use crate::protocol::{MouseButton, MouseInput};
use anyhow::{Result, bail};
use serde::Serialize;
use std::time::{Duration, Instant};
use ulid::Ulid;

pub const BROWSER_CONTROL_LEASE: Duration = Duration::from_secs(30);
pub type ReleasedMouseButton = (MouseButton, u16, u16);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOwner {
    Automation,
    Browser,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlStatus {
    pub owner: ControlOwner,
    pub generation: u64,
    pub view_id: Option<String>,
    pub connection_id: Option<String>,
    pub expires_in_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BrowserAuthority {
    pub view_id: String,
    pub connection_id: String,
    pub lease_id: String,
    pub generation: u64,
    pub expected_run_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BrowserLease {
    pub view_id: String,
    pub connection_id: String,
    pub lease_id: String,
    pub generation: u64,
    pub run_id: String,
    pub duration_ms: u64,
}

#[derive(Debug)]
struct BrowserOwner {
    view_id: String,
    connection_id: String,
    lease_id: String,
    run_id: String,
    generation: u64,
    deadline: Instant,
}

#[derive(Debug)]
enum Owner {
    Automation,
    Browser(BrowserOwner),
}

#[derive(Debug)]
pub struct ControlState {
    owner: Owner,
    generation: u64,
    mouse_position: (u16, u16),
    pressed_buttons: Vec<MouseButton>,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            owner: Owner::Automation,
            generation: 0,
            mouse_position: (0, 0),
            pressed_buttons: Vec::new(),
        }
    }
}

impl ControlState {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn status(&self) -> ControlStatus {
        match &self.owner {
            Owner::Automation => ControlStatus {
                owner: ControlOwner::Automation,
                generation: self.generation,
                view_id: None,
                connection_id: None,
                expires_in_ms: None,
            },
            Owner::Browser(owner) => ControlStatus {
                owner: ControlOwner::Browser,
                generation: self.generation,
                view_id: Some(owner.view_id.clone()),
                connection_id: Some(owner.connection_id.clone()),
                expires_in_ms: Some(
                    owner
                        .deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                ),
            },
        }
    }

    pub fn authorize_automation(&self) -> Result<()> {
        if let Owner::Browser(owner) = &self.owner {
            if owner.deadline <= Instant::now() {
                bail!("control_lease_expired: browser lease must be reconciled before input")
            }
            bail!(
                "control_held_by_browser: view {} owns terminal input",
                owner.view_id
            )
        }
        Ok(())
    }

    pub fn take_browser_control(
        &mut self,
        view_id: &str,
        connection_id: &str,
        run_id: &str,
        permitted: bool,
    ) -> Result<BrowserLease> {
        if !permitted {
            bail!("control_not_permitted: this viewing grant is read-only")
        }
        if let Owner::Browser(owner) = &self.owner {
            if owner.deadline <= Instant::now() {
                bail!("control_lease_expired: browser lease must be reconciled before takeover")
            }
            bail!(
                "control_busy: browser connection {} already owns input",
                owner.connection_id
            )
        }
        self.generation = self.generation.saturating_add(1);
        let lease_id = Ulid::generate().to_string();
        let owner = BrowserOwner {
            view_id: view_id.into(),
            connection_id: connection_id.into(),
            lease_id: lease_id.clone(),
            run_id: run_id.into(),
            generation: self.generation,
            deadline: Instant::now() + BROWSER_CONTROL_LEASE,
        };
        self.owner = Owner::Browser(owner);
        Ok(BrowserLease {
            view_id: view_id.into(),
            connection_id: connection_id.into(),
            lease_id,
            generation: self.generation,
            run_id: run_id.into(),
            duration_ms: BROWSER_CONTROL_LEASE.as_millis() as u64,
        })
    }

    pub fn renew_browser_control(&mut self, authority: &BrowserAuthority) -> Result<bool> {
        let Owner::Browser(owner) = &mut self.owner else {
            bail!("control_lease_expired: browser no longer owns terminal input")
        };
        Self::validate_authority(owner, authority)?;
        if owner.deadline <= Instant::now() {
            bail!("control_lease_expired: browser control lease expired")
        }
        owner.deadline = Instant::now() + BROWSER_CONTROL_LEASE;
        Ok(false)
    }

    pub fn authorize_browser(
        &mut self,
        authority: &BrowserAuthority,
        current_run_id: &str,
    ) -> Result<bool> {
        let Owner::Browser(owner) = &self.owner else {
            bail!("control_lease_expired: automation currently owns terminal input")
        };
        Self::validate_authority(owner, authority)?;
        if owner.run_id != current_run_id || authority.expected_run_id != current_run_id {
            bail!("stale_run: browser control belongs to a different application run")
        }
        if owner.deadline <= Instant::now() {
            bail!("control_lease_expired: browser control lease expired")
        }
        Ok(false)
    }

    pub fn release_browser_control(
        &mut self,
        authority: &BrowserAuthority,
    ) -> Result<(bool, Vec<ReleasedMouseButton>)> {
        let Owner::Browser(owner) = &self.owner else {
            bail!("control_lease_expired: automation currently owns terminal input")
        };
        Self::validate_authority(owner, authority)?;
        if owner.deadline <= Instant::now() {
            bail!("control_lease_expired: browser control lease expired")
        }
        let position = self.mouse_position;
        let releases = self
            .pressed_buttons
            .drain(..)
            .map(|button| (button, position.0, position.1))
            .collect();
        self.owner = Owner::Automation;
        self.generation = self.generation.saturating_add(1);
        Ok((true, releases))
    }

    pub fn release_expired_control(&mut self) -> Option<Vec<ReleasedMouseButton>> {
        if !self.expire() {
            return None;
        }
        let position = self.mouse_position;
        Some(self.drain_pressed_buttons(position))
    }

    pub fn record_browser_mouse(&mut self, input: &MouseInput) {
        match input {
            MouseInput::Move { row, col }
            | MouseInput::Click { row, col, .. }
            | MouseInput::Down { row, col, .. }
            | MouseInput::Wheel { row, col, .. } => self.mouse_position = (*row, *col),
            MouseInput::Up { row, col } => self.mouse_position = (*row, *col),
        }
        match input {
            MouseInput::Down { button, .. } => {
                if !self.pressed_buttons.contains(button) {
                    self.pressed_buttons.push(*button);
                }
            }
            MouseInput::Up { .. } | MouseInput::Click { .. } => self.pressed_buttons.clear(),
            MouseInput::Move { .. } | MouseInput::Wheel { .. } => {}
        }
    }

    pub fn release_for_admin(&mut self) -> Vec<ReleasedMouseButton> {
        let position = self.mouse_position;
        let releases = self
            .pressed_buttons
            .drain(..)
            .map(|button| (button, position.0, position.1))
            .collect();
        if matches!(self.owner, Owner::Browser(_)) {
            self.owner = Owner::Automation;
            self.generation = self.generation.saturating_add(1);
        }
        releases
    }

    pub fn release_pressed_mouse(&mut self) -> Vec<ReleasedMouseButton> {
        self.drain_pressed_buttons(self.mouse_position)
    }

    fn validate_authority(owner: &BrowserOwner, authority: &BrowserAuthority) -> Result<()> {
        if owner.view_id != authority.view_id
            || owner.connection_id != authority.connection_id
            || owner.lease_id != authority.lease_id
            || owner.generation != authority.generation
        {
            bail!("control_generation_changed: browser no longer owns terminal input")
        }
        Ok(())
    }

    fn expire(&mut self) -> bool {
        if matches!(&self.owner, Owner::Browser(owner) if owner.deadline <= Instant::now()) {
            self.owner = Owner::Automation;
            self.generation = self.generation.saturating_add(1);
            true
        } else {
            false
        }
    }

    fn drain_pressed_buttons(&mut self, position: (u16, u16)) -> Vec<ReleasedMouseButton> {
        self.pressed_buttons
            .drain(..)
            .map(|button| (button, position.0, position.1))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(lease: &BrowserLease) -> BrowserAuthority {
        BrowserAuthority {
            view_id: lease.view_id.clone(),
            connection_id: lease.connection_id.clone(),
            lease_id: lease.lease_id.clone(),
            generation: lease.generation,
            expected_run_id: lease.run_id.clone(),
        }
    }

    #[test]
    fn browser_control_is_exclusive_and_generation_scoped() {
        let mut state = ControlState::default();
        let lease = state
            .take_browser_control("view-a", "connection-a", "run-a", true)
            .unwrap();
        assert!(state.authorize_automation().is_err());
        assert!(state.authorize_browser(&authority(&lease), "run-a").is_ok());
        assert!(
            state
                .authorize_browser(&authority(&lease), "run-b")
                .unwrap_err()
                .to_string()
                .contains("stale_run")
        );
        assert!(
            state
                .release_browser_control(&BrowserAuthority {
                    generation: lease.generation.saturating_sub(1),
                    ..authority(&lease)
                })
                .is_err()
        );
        state.release_browser_control(&authority(&lease)).unwrap();
        assert!(state.authorize_automation().is_ok());
    }

    #[test]
    fn read_only_view_cannot_take_control() {
        let mut state = ControlState::default();
        let error = state
            .take_browser_control("view-a", "connection-a", "run-a", false)
            .unwrap_err();
        assert!(error.to_string().contains("control_not_permitted"));
        assert!(matches!(state.status().owner, ControlOwner::Automation));
    }

    #[test]
    fn stale_browser_connection_cannot_renew_or_release_a_replacement() {
        let mut state = ControlState::default();
        let old = state
            .take_browser_control("view-a", "connection-a", "run-a", true)
            .unwrap();
        state.release_browser_control(&authority(&old)).unwrap();
        let replacement = state
            .take_browser_control("view-b", "connection-b", "run-a", true)
            .unwrap();
        assert!(state.renew_browser_control(&authority(&old)).is_err());
        assert!(state.release_browser_control(&authority(&old)).is_err());
        assert!(
            state
                .authorize_browser(&authority(&replacement), "run-a")
                .is_ok()
        );
    }
}
