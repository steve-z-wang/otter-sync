//! A clockless network: one queue. Delay is "not delivered this step"; reorder,
//! duplicate and drop are queue operations. Every message names its client.
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    Push {
        client: usize,
        bytes: Vec<u8>,
    },
    Receipt {
        client: usize,
        sequence: u64,
        bytes: Vec<u8>,
    },
    /// One pull for every scope the client subscribes to.
    Pull {
        client: usize,
        bytes: Vec<u8>,
    },
    Page {
        client: usize,
        bytes: Vec<u8>,
    },
    /// One bounded historical page request of a Scope's interval, carrying the
    /// run it belongs to so a late answer can be fenced against the ledger
    /// ([#151](https://github.com/zanminwang/axton/issues/151)).
    Load {
        client: usize,
        task: axton_client::BootstrapTask,
        bytes: Vec<u8>,
    },
    /// The page that request was answered with, still carrying its correlation.
    LoadPage {
        client: usize,
        task: axton_client::BootstrapTask,
        bytes: Vec<u8>,
    },
    PushFailed {
        client: usize,
        sequence: u64,
        error: String,
    },
}

impl Message {
    pub fn client(&self) -> usize {
        match self {
            Message::Push { client, .. }
            | Message::Receipt { client, .. }
            | Message::Pull { client, .. }
            | Message::Page { client, .. }
            | Message::Load { client, .. }
            | Message::LoadPage { client, .. }
            | Message::PushFailed { client, .. } => *client,
        }
    }
}

#[derive(Default)]
pub struct Network {
    queue: VecDeque<Message>,
}

impl Network {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn send(&mut self, m: Message) {
        self.queue.push_back(m);
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn pop(&mut self) -> Option<Message> {
        self.queue.pop_front()
    }
    pub fn drop_front(&mut self) -> bool {
        self.queue.pop_front().is_some()
    }
    pub fn duplicate_front(&mut self) -> bool {
        match self.queue.front().cloned() {
            Some(m) => {
                self.queue.push_back(m);
                true
            }
            None => false,
        }
    }
    pub fn hold_front(&mut self) -> bool {
        match self.queue.pop_front() {
            Some(m) => {
                self.queue.push_back(m);
                true
            }
            None => false,
        }
    }
    pub fn swap(&mut self, i: usize, j: usize) -> bool {
        if i < self.queue.len() && j < self.queue.len() {
            self.queue.swap(i, j);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(n: u8) -> Message {
        Message::Page {
            client: 0,
            bytes: vec![n],
        }
    }

    #[test]
    fn queue_operations() {
        let mut net = Network::new();
        assert!(net.pop().is_none());
        assert!(!net.drop_front());
        net.send(page(1));
        net.send(page(2));
        net.send(page(3));
        assert!(net.swap(0, 2));
        assert_eq!(net.pop(), Some(page(3)));
        assert!(net.hold_front());
        assert_eq!(net.pop(), Some(page(1)));
        assert!(net.duplicate_front());
        assert_eq!(net.len(), 2);
        assert!(net.drop_front());
        assert_eq!(net.pop(), Some(page(2)));
        assert!(net.is_empty());
        assert!(!net.swap(0, 1));
    }
}
