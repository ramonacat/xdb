mod predictable;
mod random_single;
mod random_threaded;

use clap::Parser;
use clap::Subcommand;
use rand::Rng;
use std::{
    thread::{self},
    time::Duration,
};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use xdb::bplustree::TreeKey;
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::find;
use xdb::bplustree::algorithms::insert::insert;
use xdb::bplustree::debug::assert_properties;

use arbitrary::{Arbitrary, Unstructured};
use xdb::{
    bplustree::{Tree, TreeError, TreeTransaction},
    debug::BigKey,
    storage::{StorageError, in_memory::InMemoryStorage},
};

type KeyType = BigKey<u16, 1024>;

// TODO make these CLI options?
const RUN_LENGTH: Duration = Duration::from_secs(60);
const THREAD_COUNT: usize = 16;

fn final_checks<T: TreeKey + for<'a> Arbitrary<'a>>(tree: &Tree<InMemoryStorage, T>) {
    let mut trx = tree.transaction().unwrap();
    assert_properties(&mut trx);
    trx.rollback().unwrap();
}

#[derive(Debug)]
struct Value(Vec<u8>);

impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut buffer = vec![0u8; u.int_in_range(1..=128)?];

        u.fill_buffer(&mut buffer)?;

        Ok(Value(buffer))
    }
}

#[derive(Debug, Arbitrary)]
#[arbitrary(bound = "T: TreeKey + for<'a> Arbitrary<'a>")]
// TODO add a `Sleep` command, it would allow us to stress-test locking code
enum Command<T: TreeKey + for<'a> Arbitrary<'a>> {
    Insert(T, Value),
    Delete(T),
    Read(T),
}

impl<T: TreeKey + for<'a> Arbitrary<'a>> Command<T> {
    fn run(&self, transaction: &mut TreeTransaction<InMemoryStorage, T>) -> Result<(), TreeError> {
        match self {
            Command::Insert(key, value) => insert(transaction, *key, &value.0).map(|_| ())?,
            Command::Delete(key) => {
                delete(transaction, *key).map(|_| ())?;
            }
            Command::Read(key) => find(transaction, *key).map(|_| ())?,
        }

        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
#[arbitrary(bound = "T: TreeKey + for<'a> Arbitrary<'a>")]
struct TransactionCommands<T: TreeKey + for<'a> Arbitrary<'a>> {
    commands: Vec<Command<T>>,
    commit: bool,
}

impl<T: TreeKey + for<'a> Arbitrary<'a>> TransactionCommands<T> {
    // TODO allow providing probabilities for each type of command (so we can e.g. create a read
    // heavy test)
    fn new_random<TRng: Rng>(rng: &mut TRng) -> Self {
        let mut buffer = [0u8; 1024];
        rng.fill(&mut buffer);
        let mut unstructured = Unstructured::new(&buffer);

        TransactionCommands::arbitrary(&mut unstructured).unwrap()
    }

    fn run(&self, mut transaction: TreeTransaction<InMemoryStorage, T>) -> Result<(), TreeError> {
        for command in &self.commands {
            command.run(&mut transaction)?;
        }

        if self.commit {
            transaction.commit()?;
        } else {
            transaction.rollback()?;
        }

        Ok(())
    }
}

// TODO we should differentiate between deadlocks and optimistic concurrency failures (and only
// handle the latter here)
fn retry_on_deadlock<T: TreeKey + for<'a> Arbitrary<'a>>(
    tree: &Tree<InMemoryStorage, T>,
    callable: impl Fn(TreeTransaction<InMemoryStorage, T>) -> Result<(), TreeError>,
) -> Result<(), TreeError> {
    for i in 0..128 {
        let transaction = tree.transaction().unwrap();

        match callable(transaction) {
            Ok(ok) => return Ok(ok),
            Err(TreeError::StorageError(StorageError::Deadlock(_))) => {}
            error @ Err(_) => return error,
        };
        thread::sleep(Duration::from_millis(2u64.pow(i / 16)));
    }

    let transaction = tree.transaction().unwrap();
    callable(transaction)
}

#[derive(Subcommand)]
enum TestName {
    MultiThreadedRandom,
    SingleThreadedRandom,

    SingleThreadedPredictable,
    MultiThreadedPredictable,
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    test: TestName,
}

// TODO Add a separate "mod X" testing mode, where every thread operates (in a single, long
// transaction) only on keys that are (n%THREAD_COUNT)+thread_id, and verifies that it does not
// see anything from other threads.
// TODO Add a testing mode that creates (and removes) a lot of predicatable data, and ensure the data is correct.
fn main() {
    // TODO create a script for running a docker container with jaeger and make it possible to send
    // telemetry there
    if !cfg!(miri) {
        FmtSubscriber::builder()
            .with_thread_names(true)
            .with_env_filter(EnvFilter::from_default_env())
            .pretty()
            .with_writer(std::fs::File::create("log.txt").unwrap())
            .init();
    }

    let cli = Cli::parse();

    match &cli.test {
        TestName::MultiThreadedRandom => random_threaded::run(),
        TestName::SingleThreadedRandom => random_single::run(),

        TestName::SingleThreadedPredictable => predictable::single::run(),
        TestName::MultiThreadedPredictable => predictable::threaded::run(),
    }
}
