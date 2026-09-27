//! Every backend must behave the same: the same conditional-write semantics as S3, and plain
//! string-prefix listing.
//!
//! The S3 tests run only when `CTM_TEST_S3_URL` (for example `s3://ctm-test/run1`) is set, with
//! `CTM_TEST_S3_ENDPOINT` for an S3-compatible server and credentials in the usual AWS variables.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ctm_store::faulty::Faults;
use ctm_store::{Backend, ETag, Error, FaultyBackend, FileBackend, MemBackend, PutMode};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

async fn conformance(be: Arc<dyn Backend>) {
    // Missing keys.
    assert!(matches!(be.get("a/missing").await, Err(Error::NotFound(_))));
    assert_eq!(be.head("a/missing").await.unwrap(), None);
    be.delete("a/missing").await.unwrap();

    // Overwrite, get, head.
    let e1 = be.put("a/x", b("one"), PutMode::Overwrite).await.unwrap();
    let (body, e) = be.get("a/x").await.unwrap();
    assert_eq!((body, &e), (b("one"), &e1));
    assert_eq!(be.head("a/x").await.unwrap(), Some(e1.clone()));

    // Create-only.
    be.put("a/new", b("n"), PutMode::CreateOnly).await.unwrap();
    assert!(matches!(
        be.put("a/new", b("n2"), PutMode::CreateOnly).await,
        Err(Error::PreconditionFailed(_))
    ));
    assert_eq!(be.get("a/new").await.unwrap().0, b("n"));

    // If-Match: current ETag succeeds, stale or missing fails.
    let e2 = be
        .put("a/x", b("two"), PutMode::IfMatch(e1.clone()))
        .await
        .unwrap();
    assert_ne!(e2, e1);
    assert!(matches!(
        be.put("a/x", b("three"), PutMode::IfMatch(e1.clone()))
            .await,
        Err(Error::PreconditionFailed(_))
    ));
    assert!(matches!(
        be.put("a/nope", b("z"), PutMode::IfMatch(e2.clone())).await,
        Err(Error::PreconditionFailed(_))
    ));
    assert_eq!(be.get("a/x").await.unwrap(), (b("two"), e2));

    // Listing is by plain string prefix, sorted, and relative to the repo.
    for k in [
        "meta/7f3a01",
        "meta/7f3a02",
        "meta/7f3b00",
        "meta/80",
        "refs/branches/main",
    ] {
        be.put(k, b(k), PutMode::Overwrite).await.unwrap();
    }
    assert_eq!(
        be.list("meta/7f3a").await.unwrap(),
        ["meta/7f3a01", "meta/7f3a02"]
    );
    assert_eq!(
        be.list("meta/").await.unwrap(),
        ["meta/7f3a01", "meta/7f3a02", "meta/7f3b00", "meta/80"]
    );
    assert_eq!(be.list("refs/").await.unwrap(), ["refs/branches/main"]);
    assert!(be.list("nothing/").await.unwrap().is_empty());

    // Ranges.
    be.put("packs/p", b("0123456789"), PutMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(be.get_range("packs/p", 2..5).await.unwrap(), b("234"));
    assert_eq!(
        be.get_range("packs/p", 0..10).await.unwrap(),
        b("0123456789")
    );
    assert_eq!(be.get_range("packs/p", 7..7).await.unwrap(), b(""));
    assert!(matches!(
        be.get_range("packs/missing", 0..1).await,
        Err(Error::NotFound(_))
    ));
    be.delete("packs/p").await.unwrap();

    // Delete.
    be.delete("meta/80").await.unwrap();
    assert_eq!(be.head("meta/80").await.unwrap(), None);

    // Races: exactly one conditional writer wins.
    let base = be
        .put("race/cas", b("0"), PutMode::Overwrite)
        .await
        .unwrap();
    let wins = race(&be, |_| PutMode::IfMatch(base.clone()), "race/cas").await;
    assert_eq!(wins, 1, "CAS race");
    let wins = race(&be, |_| PutMode::CreateOnly, "race/create").await;
    assert_eq!(wins, 1, "create race");

    ctm_store::probe(be.as_ref()).await.unwrap();
}

async fn race(be: &Arc<dyn Backend>, mode: impl Fn(usize) -> PutMode, key: &str) -> usize {
    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let be = be.clone();
            let mode = mode(i);
            let key = key.to_string();
            tokio::spawn(async move { be.put(&key, b(&format!("w{i}")), mode).await })
        })
        .collect();
    let mut wins = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => wins += 1,
            Err(Error::PreconditionFailed(_)) => {}
            Err(e) => panic!("unexpected error in race: {e}"),
        }
    }
    wins
}

#[tokio::test]
async fn mem_backend_conforms() {
    conformance(Arc::new(MemBackend::new())).await;
}

#[tokio::test]
async fn file_backend_conforms() {
    let dir = tempfile::tempdir().unwrap();
    conformance(Arc::new(FileBackend::new(dir.path()))).await;
}

#[tokio::test]
async fn file_backend_hides_its_bookkeeping() {
    let dir = tempfile::tempdir().unwrap();
    let be = FileBackend::new(dir.path());
    let e = be.put("refs/x", b("1"), PutMode::CreateOnly).await.unwrap();
    be.put("refs/x", b("2"), PutMode::IfMatch(e)).await.unwrap();
    assert_eq!(be.list("").await.unwrap(), ["refs/x"]);
}

#[tokio::test]
async fn file_backend_is_shared_between_instances() {
    // Two processes on one machine are two instances over the same directory.
    let dir = tempfile::tempdir().unwrap();
    let a: Arc<dyn Backend> = Arc::new(FileBackend::new(dir.path()));
    let e = a.put("refs/b", b("0"), PutMode::Overwrite).await.unwrap();
    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let be = FileBackend::new(dir.path());
            let e = e.clone();
            tokio::spawn(async move {
                be.put("refs/b", b(&format!("w{i}")), PutMode::IfMatch(e))
                    .await
            })
        })
        .collect();
    let mut wins = 0;
    for t in tasks {
        if t.await.unwrap().is_ok() {
            wins += 1;
        }
    }
    assert_eq!(wins, 1);
}

#[tokio::test]
async fn s3_backend_conforms() {
    let Ok(url) = std::env::var("CTM_TEST_S3_URL") else {
        eprintln!("skipped: CTM_TEST_S3_URL is not set");
        return;
    };
    let endpoint = std::env::var("CTM_TEST_S3_ENDPOINT").ok();
    // A fresh prefix per run, so leftovers from earlier runs don't matter.
    let url = format!("{url}/{}", std::process::id());
    conformance(ctm_store::open(&url, endpoint.as_deref()).unwrap()).await;
}

#[test]
fn open_parses_repo_urls() {
    let dir = tempfile::tempdir().unwrap();
    let file_url = format!("file://{}", dir.path().display());
    assert!(ctm_store::open(&file_url, None).is_ok());
    assert!(ctm_store::open("s3://bucket/some/prefix", None).is_ok());
    assert!(ctm_store::open("s3://bucket", Some("http://localhost:9000")).is_ok());
    for bad in [
        "r2://bucket/p",
        "s3://",
        "http://x/y",
        "file://relative",
        "bucket/p",
    ] {
        assert!(
            matches!(ctm_store::open(bad, None), Err(Error::BadUrl(_))),
            "{bad} accepted"
        );
    }
}

/// A backend that ignores preconditions, like a service without conditional writes.
struct NoConditions(MemBackend);

#[async_trait]
impl Backend for NoConditions {
    async fn get(&self, key: &str) -> ctm_store::Result<(Bytes, ETag)> {
        self.0.get(key).await
    }
    async fn head(&self, key: &str) -> ctm_store::Result<Option<ETag>> {
        self.0.head(key).await
    }
    async fn put(&self, key: &str, body: Bytes, _: PutMode) -> ctm_store::Result<ETag> {
        self.0.put(key, body, PutMode::Overwrite).await
    }
    async fn list(&self, prefix: &str) -> ctm_store::Result<Vec<String>> {
        self.0.list(prefix).await
    }
    async fn delete(&self, key: &str) -> ctm_store::Result<()> {
        self.0.delete(key).await
    }
}

#[tokio::test]
async fn probe_refuses_a_backend_without_conditional_writes() {
    let be = NoConditions(MemBackend::new());
    assert!(matches!(
        ctm_store::probe(&be).await,
        Err(Error::ProbeFailed(_))
    ));
}

#[tokio::test]
async fn probe_cleans_up() {
    let be = MemBackend::new();
    ctm_store::probe(&be).await.unwrap();
    assert!(be.list("").await.unwrap().is_empty());
}

#[tokio::test]
async fn faulty_backend_crashes_after_n_puts_and_heals() {
    let be = FaultyBackend::new(
        MemBackend::new(),
        Faults {
            crash_after_puts: Some(2),
            ..Faults::default()
        },
    );
    be.put("a", b("1"), PutMode::Overwrite).await.unwrap();
    be.put("b", b("2"), PutMode::Overwrite).await.unwrap();
    assert!(be.put("c", b("3"), PutMode::Overwrite).await.is_err());
    // After the crash, nothing works, not even reads.
    assert!(be.get("a").await.is_err());
    be.heal();
    assert_eq!(be.get("a").await.unwrap().0, b("1"));
    assert_eq!(be.head("c").await.unwrap(), None);
}

#[tokio::test]
async fn a_crash_can_land_the_put_it_interrupts() {
    let be = FaultyBackend::new(
        MemBackend::new(),
        Faults {
            crash_after_puts: Some(1),
            crash_lands_put: true,
            ..Faults::default()
        },
    );
    be.put("a", b("1"), PutMode::Overwrite).await.unwrap();
    assert!(be.put("b", b("2"), PutMode::Overwrite).await.is_err());
    assert!(be.put("c", b("3"), PutMode::Overwrite).await.is_err());
    be.heal();
    assert_eq!(
        be.get("b").await.unwrap().0,
        b("2"),
        "the crashing put landed"
    );
    assert_eq!(be.head("c").await.unwrap(), None, "later puts did not");
}

#[tokio::test]
async fn faulty_backend_errors_are_deterministic() {
    let run = || async {
        let be = FaultyBackend::new(
            MemBackend::new(),
            Faults {
                error_rate: 0.5,
                seed: 7,
                ..Faults::default()
            },
        );
        let mut pattern = Vec::new();
        for i in 0..64 {
            pattern.push(
                be.put(&format!("k{i}"), b("v"), PutMode::Overwrite)
                    .await
                    .is_ok(),
            );
        }
        pattern
    };
    let a = run().await;
    assert_eq!(a, run().await);
    assert!(a.iter().any(|ok| *ok) && a.iter().any(|ok| !*ok));
}

#[tokio::test]
async fn faulty_backend_adds_latency() {
    let be = FaultyBackend::new(
        MemBackend::new(),
        Faults {
            latency: Duration::from_millis(30),
            ..Faults::default()
        },
    );
    let t = std::time::Instant::now();
    be.head("x").await.unwrap();
    assert!(t.elapsed() >= Duration::from_millis(30));
}
