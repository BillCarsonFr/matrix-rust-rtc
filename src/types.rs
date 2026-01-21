#[derive(Clone, Debug)]
pub enum FocusType {
    Livekit,
    Other(String),
}

pub type ApplicationId = String;

#[derive(Clone, Debug, PartialEq)]
pub enum ApplicationKind {
    /// "application": "m.call"
    Call,
    Other(String),
}

pub struct RtcSlot {
    pub kind: ApplicationKind,
    pub id: ApplicationId,
}

#[derive(Clone, Debug)]
pub enum Focus {
    LivekitFocus(LivekitFocus),
}

#[derive(Clone, Debug)]
pub struct LivekitFocus {
    pub livekit_alias: String,
    pub livekit_service_url: String,
}

impl LivekitFocus {
    pub fn focus_type(&self) -> FocusType {
        FocusType::Livekit
    }
}
