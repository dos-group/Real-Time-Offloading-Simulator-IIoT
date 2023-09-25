use std::env;
use tokio::time::{sleep_until, Instant, Duration};

#[tokio::main]
async fn main() {
    let start_time = Instant::now();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        panic!(
            "Usage: {} execution_cpu_time_ms return_data_bytes",
            &args[0]
        );
    }
    let exec_time = Duration::from_millis(args[1].parse().unwrap());
    let return_data_bytes: usize = args[2].parse().unwrap();
    let output = (0..return_data_bytes).map(|_| "X").collect::<String>();

    sleep_until(start_time + exec_time).await;
    println!("{}", output);
}
