use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use crate::replicas::Replicas;

pub struct MasterData {
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
}

#[derive(Clone)]
pub struct ServerState {
    pub cache: Arc<Mutex<HashMap<String, String>>>,
    pub master_data: Arc<Option<MasterData>>,
    pub replicas: Arc<Replicas>,
    pub pending_writes: Arc<AtomicBool>,
    // watched keys -> number of watchers
    pub watched_keys: Arc<Mutex<HashMap<String, u64>>>,
}

pub struct ClientState {
    pub multi_queue: Option<VecDeque<Vec<Option<String>>>>,
    pub currently_watching: HashSet<String>,
}
