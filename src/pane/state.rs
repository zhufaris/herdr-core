use std::fmt;

use crate::terminal::TerminalId;

pub const PANE_TOKEN_SPACE: u32 = 36_u32.pow(4);

/// A short, human-friendly pane locator. This is not a security credential or
/// a replacement for the pane's public API id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PaneToken([u8; 4]);

impl PaneToken {
    pub fn parse(value: &str) -> Option<Self> {
        let bytes: [u8; 4] = value.as_bytes().try_into().ok()?;
        bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
            .then_some(Self(bytes))
    }

    pub(crate) fn from_index(mut index: u32) -> Self {
        debug_assert!(index < PANE_TOKEN_SPACE);
        let mut bytes = [b'0'; 4];
        for byte in bytes.iter_mut().rev() {
            let digit = (index % 36) as u8;
            *byte = if digit < 10 {
                b'0' + digit
            } else {
                b'a' + digit - 10
            };
            index /= 36;
        }
        Self(bytes)
    }

    pub fn as_str(&self) -> &str {
        // Construction only permits ASCII base36 bytes.
        std::str::from_utf8(&self.0).expect("pane token must be valid ASCII")
    }

    #[cfg(test)]
    pub(crate) fn alloc_for_test() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        Self::from_index(NEXT.fetch_add(1, Ordering::Relaxed) % PANE_TOKEN_SPACE)
    }
}

impl fmt::Display for PaneToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Viewport state for a pane.
///
/// Terminal identity, cwd, labels, and agent metadata live in TerminalState.
pub struct PaneState {
    pub attached_terminal_id: TerminalId,
    pub token: PaneToken,
    /// Whether the user has seen this pane since its last state change to Idle.
    /// False = "Done" (agent finished while user was in another workspace).
    pub seen: bool,
    /// Whether unmodified right-click gestures should be forwarded to the pane application.
    pub right_click_passthrough: bool,
}

impl PaneState {
    pub fn new(attached_terminal_id: TerminalId, token: PaneToken) -> Self {
        Self {
            attached_terminal_id,
            token,
            seen: true,
            right_click_passthrough: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pane_token_accepts_exact_lowercase_base36() {
        assert_eq!(
            PaneToken::parse("0az9").map(|token| token.to_string()),
            Some("0az9".into())
        );
        for invalid in ["abc", "abcde", "ABC1", "ab-1", "é123"] {
            assert_eq!(PaneToken::parse(invalid), None, "accepted {invalid:?}");
        }
    }

    #[test]
    fn pane_token_index_covers_base36_boundaries() {
        assert_eq!(PaneToken::from_index(0).as_str(), "0000");
        assert_eq!(PaneToken::from_index(35).as_str(), "000z");
        assert_eq!(PaneToken::from_index(36).as_str(), "0010");
        assert_eq!(PaneToken::from_index(PANE_TOKEN_SPACE - 1).as_str(), "zzzz");
    }
}
