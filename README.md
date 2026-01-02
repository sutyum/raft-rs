# raft

**The Truth Arbiter — Distributed Consensus**

This crate implements the Raft consensus algorithm. It depends on `env` for time/randomness and `wal` for log persistence.

## Why This Exists

Imagine three servers tracking account balances:

```
Server A: balance = $100
Server B: balance = $100  
Server C: balance = $100
```

A client sends "withdraw $50" to Server A. Before A can tell B and C:

1. Network partition isolates A
2. Client retries, sends "withdraw $50" to Server B
3. Partition heals

What's the balance now?
- Server A thinks: $50
- Server B thinks: $50
- Server C thinks: $100

**This is the consensus problem.** How do multiple servers agree on a single sequence of operations?

Raft solves this by:
1. Electing a **leader** that handles all writes
2. Leader **replicates** writes to followers
3. Writes are **committed** only when majority acknowledges
4. A new leader is elected if the old one fails

---

## The Core Insight

Raft reduces consensus to **log replication**.

```
Server A (Leader):   [op1][op2][op3][op4][op5]
                      ✓    ✓    ✓    ✓    ?
                      
Server B (Follower): [op1][op2][op3][op4]
                      ✓    ✓    ✓    ✓
                      
Server C (Follower): [op1][op2][op3]
                      ✓    ✓    ✓
```

- All servers apply the SAME operations in the SAME order
- An operation is **committed** when replicated to majority
- Committed operations are NEVER lost, even if leader crashes

The state machine (your application) just processes the committed log:

```rust
fn apply_committed(&mut self) {
    while self.last_applied < self.commit_index {
        self.last_applied += 1;
        let entry = &self.log[self.last_applied];
        self.state_machine.apply(entry.command);
    }
}
```

---

## The Architecture

### Pure State Machine Design

**Critical:** Raft is implemented as a pure function with no I/O.

```rust
impl Raft {
    /// The ONLY entry point. Pure function.
    pub fn step(&mut self, input: Input) -> Vec<Output>;
}
```

This design is non-negotiable because:
1. **Testability:** Simulation can control all inputs
2. **Determinism:** Same inputs = same outputs
3. **Separation:** Logic is separate from I/O

### Input/Output Types

```rust
/// Messages from the outside world
pub enum Input {
    /// Time has passed (call periodically)
    Tick,
    
    /// Network message received
    Message(Message),
    
    /// Client wants to propose a command
    ClientPropose { id: ClientId, command: Vec<u8> },
}

/// Actions for the runtime to perform
pub enum Output {
    /// Send message to another node
    Send { to: NodeId, message: Message },
    
    /// Persist log entries to WAL (MUST complete before responding)
    Persist { entries: Vec<Entry> },
    
    /// Apply committed entries to state machine
    Apply { entries: Vec<Entry> },
    
    /// Respond to client
    ClientResponse { id: ClientId, result: ClientResult },
}

pub enum Message {
    RequestVote { term: u64, candidate_id: NodeId, last_log_index: u64, last_log_term: u64 },
    RequestVoteResponse { term: u64, vote_granted: bool },
    
    AppendEntries { 
        term: u64, 
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResponse { term: u64, success: bool, match_index: u64 },
}
```

---

## The Raft State

```rust
pub struct Raft {
    // === Identity ===
    id: NodeId,
    peers: Vec<NodeId>,
    
    // === Persistent State (must survive restarts) ===
    current_term: u64,      // Latest term seen
    voted_for: Option<NodeId>,  // Who we voted for in current term
    log: Vec<Entry>,        // The replicated log
    
    // === Volatile State ===
    commit_index: u64,      // Highest committed entry
    last_applied: u64,      // Highest applied to state machine
    
    // === Leader State (reinitialized on election) ===
    role: Role,
    next_index: HashMap<NodeId, u64>,   // Next entry to send to each peer
    match_index: HashMap<NodeId, u64>,  // Highest entry known replicated
    
    // === Timing ===
    election_timeout: u64,  // Ticks until we start election
    heartbeat_timeout: u64, // Ticks until leader sends heartbeat
}

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}
```

---

## The Algorithm

### Leader Election

```
1. Start as Follower with random election timeout (150-300ms)

2. If election timeout elapses without hearing from leader:
   - Increment current_term
   - Transition to Candidate
   - Vote for self
   - Send RequestVote to all peers
   - Reset election timeout

3. As Candidate:
   - If receive votes from majority → become Leader
   - If receive AppendEntries from valid leader → become Follower
   - If election timeout elapses → start new election

4. As Leader:
   - Send periodic heartbeats (empty AppendEntries)
   - Handle client requests
   - Replicate log entries
```

### Log Replication

```
Leader receives client command:
1. Append entry to local log
2. Send AppendEntries to all followers

Follower receives AppendEntries:
1. If term < currentTerm → reject
2. If log doesn't contain entry at prevLogIndex with prevLogTerm → reject
3. Delete conflicting entries
4. Append new entries
5. Update commitIndex
6. Respond success

Leader receives AppendEntriesResponse:
1. If success → update nextIndex and matchIndex for follower
2. If majority have replicated entry → commit it
3. If reject → decrement nextIndex, retry
```

### Safety Rules

**Election Safety:** At most one leader per term.
- Each node votes for at most one candidate per term
- Candidate needs majority to win

**Leader Completeness:** Committed entries survive leader changes.
- Candidate's log must be "at least as up-to-date" to get votes
- "Up-to-date" = higher term in last entry, or same term with longer log

**Log Matching:** If two logs have entry with same index and term, all preceding entries are identical.
- Leader includes prevLogIndex/prevLogTerm in AppendEntries
- Follower rejects if doesn't match

---

## The Simulation Layer

The Raft implementation is pure. We need a simulation harness:

```rust
pub struct RaftSimulator {
    env: SimEnv,
    nodes: Vec<Raft>,
    network: SimNetwork,
    pending_outputs: Vec<(NodeId, Output)>,
}

impl RaftSimulator {
    pub fn new(env: SimEnv, node_count: usize) -> Self;
    
    /// Advance simulation by one step
    pub fn step(&mut self);
    
    /// Run until predicate is true or timeout
    pub fn run_until<F: Fn(&Self) -> bool>(&mut self, predicate: F, max_ticks: u64);
    
    /// Run until a leader is elected
    pub fn run_until_leader(&mut self) -> NodeId;
    
    /// Create network partition
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]);
    
    /// Heal all partitions
    pub fn heal_network(&mut self);
    
    /// Submit client request
    pub fn client_request(&mut self, to: NodeId, command: &[u8]) -> ClientId;
    
    /// Check if all nodes have consistent logs
    pub fn assert_logs_consistent(&self);
}

pub struct SimNetwork {
    partitions: Vec<HashSet<NodeId>>,
    in_flight: VecDeque<(NodeId, NodeId, Message, u64)>,  // (from, to, msg, deliver_at)
    latency_range: (u64, u64),
    drop_probability: f64,
}
```

---

## Exam Questions

### Exam 1: Basic Leader Election

```rust
#[test]
fn basic_election() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env, 3);
    
    // Initially no leader
    assert!(sim.current_leader().is_none());
    
    // Run until leader elected
    let leader = sim.run_until_leader();
    
    // Verify exactly one leader
    let leaders: Vec<_> = sim.nodes.iter()
        .filter(|n| n.role() == Role::Leader)
        .collect();
    assert_eq!(leaders.len(), 1);
    assert_eq!(leaders[0].id(), leader);
    
    // All nodes agree on current term
    let terms: HashSet<_> = sim.nodes.iter()
        .map(|n| n.current_term())
        .collect();
    assert_eq!(terms.len(), 1);
}
```

### Exam 2: Leader Failure and Re-election

```rust
#[test]
fn leader_failure() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env, 5);
    
    // Elect initial leader
    let leader1 = sim.run_until_leader();
    let term1 = sim.node(leader1).current_term();
    
    // Kill the leader
    sim.crash_node(leader1);
    
    // New leader should be elected
    sim.run_for(Duration::from_secs(5));
    let leader2 = sim.current_leader().expect("should have new leader");
    
    assert_ne!(leader1, leader2);
    assert!(sim.node(leader2).current_term() > term1);
}
```

### Exam 3: Log Replication

```rust
#[test]
fn log_replication() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env, 3);
    
    let leader = sim.run_until_leader();
    
    // Submit commands
    for i in 0..10 {
        sim.client_request(leader, format!("cmd_{}", i).as_bytes());
    }
    
    // Wait for replication
    sim.run_for(Duration::from_secs(5));
    
    // All nodes should have same log
    sim.assert_logs_consistent();
    
    // All commands should be committed
    assert_eq!(sim.node(leader).commit_index(), 10);
}
```

### Exam 4: Network Partition Safety

```rust
#[test]
fn partition_safety() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env, 5);  // Nodes 0-4
    
    let leader = sim.run_until_leader();
    
    // Submit and commit some data
    sim.client_request(leader, b"before_partition");
    sim.run_for(Duration::from_secs(1));
    
    // Create partition: {leader, node1} vs {node2, node3, node4}
    let minority = vec![leader, (leader + 1) % 5];
    let majority: Vec<_> = (0..5).filter(|n| !minority.contains(n)).collect();
    sim.partition(&minority, &majority);
    
    // Try to write to old leader (minority partition)
    let request_id = sim.client_request(leader, b"minority_write");
    sim.run_for(Duration::from_secs(2));
    
    // This write should NOT be committed (can't reach majority)
    assert!(sim.request_status(request_id).is_pending());
    
    // Majority partition should elect new leader
    sim.run_for(Duration::from_secs(5));
    let new_leader = sim.current_leader_in_partition(&majority)
        .expect("majority should elect leader");
    
    // Write to new leader (should succeed)
    sim.client_request(new_leader, b"majority_write");
    sim.run_for(Duration::from_secs(1));
    
    // Heal partition
    sim.heal_network();
    sim.run_for(Duration::from_secs(5));
    
    // All nodes should converge
    sim.assert_logs_consistent();
    
    // The minority write should NOT be in the final log
    for node in &sim.nodes {
        let log_contents: Vec<&[u8]> = node.log().iter()
            .map(|e| e.command.as_slice())
            .collect();
        
        assert!(log_contents.contains(&b"before_partition".as_slice()));
        assert!(log_contents.contains(&b"majority_write".as_slice()));
        assert!(!log_contents.contains(&b"minority_write".as_slice()));
    }
}
```

### Exam 5: Log Consistency After Partition Heal

```rust
#[test]
fn log_consistency_after_partition() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env, 5);
    
    let leader = sim.run_until_leader();
    
    // Commit entries 1-5
    for i in 1..=5 {
        sim.client_request(leader, format!("entry_{}", i).as_bytes());
    }
    sim.run_for(Duration::from_secs(1));
    
    // Partition leader from all others
    sim.partition(&[leader], &[0, 1, 2, 3, 4].iter()
        .filter(|&&n| n != leader)
        .copied()
        .collect::<Vec<_>>());
    
    // Leader appends uncommitted entries (can't replicate)
    for i in 6..=10 {
        sim.client_request(leader, format!("leader_only_{}", i).as_bytes());
    }
    sim.run_for(Duration::from_secs(1));
    
    // Majority elects new leader, commits new entries
    sim.run_for(Duration::from_secs(5));
    let new_leader = sim.current_leader().unwrap();
    for i in 6..=10 {
        sim.client_request(new_leader, format!("new_leader_{}", i).as_bytes());
    }
    sim.run_for(Duration::from_secs(1));
    
    // Heal partition
    sim.heal_network();
    sim.run_for(Duration::from_secs(5));
    
    // OLD LEADER MUST TRUNCATE ITS UNCOMMITTED ENTRIES
    // AND ADOPT NEW LEADER'S LOG
    sim.assert_logs_consistent();
    
    // Verify old leader has new leader's entries
    let old_leader_log: Vec<String> = sim.node(leader).log().iter()
        .filter_map(|e| String::from_utf8(e.command.clone()).ok())
        .collect();
    
    assert!(old_leader_log.iter().any(|s| s.contains("new_leader")));
    assert!(!old_leader_log.iter().any(|s| s.contains("leader_only")));
}
```

### Exam 6: Persistence and Recovery

```rust
#[test]
fn persistence_and_recovery() {
    let env = SimEnv::new(42);
    let mut sim = RaftSimulator::new(env.clone(), 3);
    
    let leader = sim.run_until_leader();
    
    // Commit some entries
    for i in 0..10 {
        sim.client_request(leader, format!("cmd_{}", i).as_bytes());
    }
    sim.run_for(Duration::from_secs(2));
    
    // Record state
    let term = sim.node(leader).current_term();
    let log_len = sim.node(leader).log().len();
    
    // Crash and restart all nodes
    for i in 0..3 {
        sim.crash_node(i);
    }
    
    env.crash();  // Lose all unflushed data
    
    // Restart nodes (they recover from WAL)
    for i in 0..3 {
        sim.restart_node(i);
    }
    sim.run_for(Duration::from_secs(5));
    
    // All committed data should survive
    sim.assert_logs_consistent();
    
    // Log should be intact
    for node in &sim.nodes {
        assert!(node.log().len() >= log_len - 1);  // Might lose last uncommitted
        assert!(node.current_term() >= term);
    }
}
```

### Exam 7: No Split Brain

```rust
#[test]
fn no_split_brain() {
    // Run many random scenarios
    for seed in 0..1000 {
        let env = SimEnv::new(seed);
        let mut sim = RaftSimulator::new(env, 5);
        
        // Inject random chaos
        for _ in 0..100 {
            match sim.random_action() {
                Action::Tick => sim.step(),
                Action::Partition(a, b) => sim.partition(&a, &b),
                Action::Heal => sim.heal_network(),
                Action::Crash(n) => sim.crash_node(n),
                Action::Restart(n) => sim.restart_node(n),
                Action::ClientRequest(n) => { sim.client_request(n, b"data"); }
            }
        }
        
        // At any point in time, at most one leader per term
        let leaders_by_term = sim.history_of_leaders();
        for (term, leaders) in leaders_by_term {
            assert!(
                leaders.len() <= 1,
                "Split brain! Term {} had leaders: {:?} (seed={})",
                term, leaders, seed
            );
        }
    }
}
```

---

## Common Traps

### Trap 1: Responding Before Persisting

**Wrong:**
```rust
fn handle_append_entries(&mut self, msg: AppendEntries) -> Vec<Output> {
    self.log.extend(msg.entries);
    vec![
        Output::Send { to: msg.leader_id, message: Response { success: true } },
        Output::Persist { entries: msg.entries },  // Oops, response already sent
    ]
}
```

**Why:** If we crash before persist completes, leader thinks we have entries we don't.

**Right:**
```rust
fn handle_append_entries(&mut self, msg: AppendEntries) -> Vec<Output> {
    self.log.extend(msg.entries.clone());
    self.pending_response = Some((msg.leader_id, Response { success: true }));
    vec![Output::Persist { entries: msg.entries }]
    // Response sent by runtime AFTER persist completes
}
```

### Trap 2: Wrong "Up-to-date" Comparison

**Wrong:**
```rust
fn log_is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
    last_index >= self.log.last_index()  // Only compares length
}
```

**Why:** A longer log with older term is NOT more up-to-date.

**Right:**
```rust
fn log_is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
    let my_last_term = self.log.last().map(|e| e.term).unwrap_or(0);
    let my_last_index = self.log.len() as u64;
    
    // First compare terms, then length
    if last_term != my_last_term {
        last_term > my_last_term
    } else {
        last_index >= my_last_index
    }
}
```

### Trap 3: Not Resetting Election Timeout

**Wrong:**
```rust
fn handle_append_entries(&mut self, msg: AppendEntries) -> Vec<Output> {
    if msg.term >= self.current_term {
        // Process entries...
        // Forgot to reset election timeout!
    }
}
```

**Why:** Follower will time out and start election even with active leader.

**Right:**
```rust
fn handle_append_entries(&mut self, msg: AppendEntries) -> Vec<Output> {
    if msg.term >= self.current_term {
        self.reset_election_timeout();
        // Process entries...
    }
}
```

### Trap 4: Committing Entries from Previous Terms

**Wrong:**
```rust
fn maybe_commit(&mut self) {
    // Find highest index replicated to majority
    let mut indices: Vec<_> = self.match_index.values().copied().collect();
    indices.push(self.log.last_index());
    indices.sort();
    let majority_index = indices[indices.len() / 2];
    
    self.commit_index = majority_index;  // Always commit
}
```

**Why:** Raft Figure 8 scenario. Committing old-term entries can lead to inconsistency.

**Right:**
```rust
fn maybe_commit(&mut self) {
    let mut indices: Vec<_> = self.match_index.values().copied().collect();
    indices.push(self.log.last_index());
    indices.sort();
    let majority_index = indices[indices.len() / 2];
    
    // ONLY commit if entry is from current term
    if self.log[majority_index].term == self.current_term {
        self.commit_index = majority_index;
    }
}
```

### Trap 5: Single-Step Membership Changes

**Wrong:**
```rust
fn add_server(&mut self, new_node: NodeId) {
    self.peers.push(new_node);  // Immediate change
}
```

**Why:** Can create overlapping majorities → split brain.

**Right:** Use joint consensus (two-phase):
```rust
fn add_server(&mut self, new_node: NodeId) {
    // Phase 1: Commit Cold,new (both old and new config must agree)
    // Phase 2: Commit Cnew (only new config)
}
```

This is complex. Consider skipping membership changes for learning, or only allowing single-server changes with careful guards.

---

## Files to Implement

```
raft/
├── Cargo.toml
├── README.md              # This file
└── src/
    ├── lib.rs             # Public API, Raft struct
    ├── state.rs           # RaftState, persistent/volatile state
    ├── log.rs             # Log entries, indexing
    ├── election.rs        # Leader election logic
    ├── replication.rs     # Log replication logic  
    ├── message.rs         # Message types
    └── sim/
        ├── mod.rs         # RaftSimulator
        ├── network.rs     # SimNetwork with partitions
        └── harness.rs     # Test harness utilities
```

---

## Dependencies

```toml
[package]
name = "raft"
version.workspace = true
edition.workspace = true

[dependencies]
env = { path = "../env" }
wal = { path = "../wal" }
thiserror = { workspace = true }

[dev-dependencies]
rand = { workspace = true }
proptest = { workspace = true }
```

---

## Reference Materials

Read the Raft paper carefully: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)

Pay special attention to:
- Figure 2: Summary of Raft algorithm
- Figure 8: Why you can't commit old-term entries
- Section 5.4.1: Election restriction
- Section 6: Cluster membership changes

Use the [Raft Visualization](https://raft.github.io/) to build intuition.

---

## Success Criteria

You have completed this phase when:

1. All exam questions pass
2. `cargo test -p raft` passes
3. No split brain under any combination of partitions (verified by simulation)
4. Log consistency maintained through leader failures (verified by simulation)
5. You can explain why Raft can't commit entries from previous terms
6. You can explain the "up-to-date" comparison for voting
7. You can explain why election timeouts must be randomized

**Time estimate:** 6-8 weeks

**Previous phase:** [lsm](../lsm/README.md)

---

## What's Next?

After completing all four phases, you have built a distributed key-value store from scratch:

```rust
// The final integration (not part of this curriculum, but possible)
struct DistributedKV {
    raft: Raft,
    lsm: Lsm,
}

impl DistributedKV {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        // 1. Propose to Raft
        let entry = self.raft.propose(PutCommand { key, value });
        
        // 2. Wait for commit
        while !self.raft.is_committed(entry) {
            self.raft.step(Input::Tick);
        }
        
        // 3. Apply to LSM
        self.lsm.put(key, value);
    }
}
```

You now understand:
- How data gets from application to disk (LSM)
- How writes survive crashes (WAL)
- How multiple servers agree on state (Raft)
- How to test all of this deterministically (Env + Simulation)
