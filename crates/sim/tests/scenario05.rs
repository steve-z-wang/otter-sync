use axton_sim::scenario05::run_recovery05;
#[test]
fn receipt_and_authority_permutations_reopen_every_persisted_boundary() {
    for (authority_first, private) in [(false, false), (true, false), (false, true)] {
        let directory = tempfile::tempdir().unwrap();
        let trace = run_recovery05(&directory.path().join("db"), authority_first, private);
        assert!(trace.exact_retry, "frozen bytes changed after reopen");
        assert!(
            trace.unfinished_after_receipt,
            "content or receipt alone fabricated coverage"
        );
        assert!(trace.durable_completion, "completion lost original waiter");
        assert_eq!(
            trace.visible["text"],
            if private { "later direct" } else { "canonical" },
            "private receipt preserves local owner; later Stream authority wins"
        );
        assert_eq!(trace.boundaries.len(), 5);
    }
}
