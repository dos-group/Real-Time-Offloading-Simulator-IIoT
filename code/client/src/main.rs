use bincode;
use env_logger::Builder;
use local_ip_address::local_ip;
use log::{debug, warn, LevelFilter};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal, Uniform};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddrV4;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use utils::{log_metric, LogMetricType, Message, Task, TaskResult};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let mut log_builder = Builder::new();
    log_builder
        .filter_level(LevelFilter::Off)
        .parse_default_env()
        .format_timestamp_micros()
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 7 {
        panic!(
            "Usage: {} scheduler_addr exec_time_ms slack_time_mean_ms \
            return_data_bytes interval_lambda rng_seed",
            &args[0]
        );
    }
    let scheduler_addr: SocketAddrV4 = args[1].parse().unwrap();
    let actual_exec_time = Duration::from_millis(args[2].parse().unwrap());
    let slack_time_mean_ms: f64 = args[3].parse().unwrap();
    assert!(slack_time_mean_ms > 0.0);
    let worst_case_exec_time = actual_exec_time;
    let return_data_bytes: usize = args[4].parse().unwrap();
    let interval_lambda: f64 = args[5].parse().unwrap();
    assert!(interval_lambda >= 0.0);
    let rng_seed: u64 = args[6].parse().unwrap();
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(rng_seed);

    let ipv4 = local_ip().unwrap().to_string();
    let listener = TcpListener::bind(format!("{}:0", ipv4)).await.unwrap();

    let rpc_id = 1;
    let client_id = Uuid::new_v4();
    let client_addr: SocketAddrV4 =
        listener.local_addr().unwrap().to_string().parse().unwrap();

    debug!("Connecting to scheduler at {}...", scheduler_addr);

    let connection_start_time = SystemTime::now();
    let mut stream = TcpStream::connect(scheduler_addr).await.unwrap();
    let connection_setup_time = connection_start_time.elapsed().unwrap();

    let msg = Message::ClientHello(client_id);
    let msg_s = bincode::serialize(&msg).unwrap();
    stream.write(&msg_s).await.unwrap();

    let mut buffer = [0 as u8; 1024];
    stream.read(&mut buffer).await.unwrap();
    let message: Message = bincode::deserialize(&buffer).unwrap();

    match message {
        Message::SchedulerHello => {
            debug!("Connected to scheduler at {}", scheduler_addr)
        }
        _ => panic!("Connection failed"),
    }
    let slack_time_variance_ms = slack_time_mean_ms / 3.;
    debug!(
        "Slack time mean: {} variance: {}",
        slack_time_mean_ms, slack_time_variance_ms
    );
    let normal =
        Normal::new(slack_time_mean_ms, slack_time_variance_ms).unwrap();
    let _uniform = Uniform::new(10.0, 1000.0);

    loop {
        let empty_map: HashMap<String, String> = HashMap::new();
        let parameters = HashMap::from([(
            String::from("return_data_bytes"),
            return_data_bytes.to_string(),
        )]);

        let mut slack_time_ms = normal.sample(&mut rand::thread_rng());
        if slack_time_ms < 0.0 {
            slack_time_ms = -slack_time_ms;
        }
        // let slack_time_ms = _uniform.sample(&mut rand::thread_rng());
        debug!("Slack time ms: {}", slack_time_ms);
        let initial_time_to_deadline =
            actual_exec_time + Duration::from_secs_f64(slack_time_ms / 1000.0);
        let deadline = SystemTime::now() + initial_time_to_deadline;

        let task = Task::new(
            rpc_id,
            client_id,
            client_addr,
            deadline,
            initial_time_to_deadline,
            connection_setup_time,
            worst_case_exec_time,
            actual_exec_time,
            parameters,
        );
        let task_id = task.id();

        let current_time = SystemTime::now();
        log_metric(
            client_id,
            LogMetricType::ClientTaskCreated,
            task_id,
            &HashMap::from([
                (
                    "deadline",
                    task.deadline()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64()
                        .to_string(),
                ),
                (
                    "time_to_deadline_secs",
                    task.deadline()
                        .duration_since(current_time)
                        .unwrap()
                        .as_secs_f64()
                        .to_string(),
                ),
                (
                    "exec_time_secs",
                    task.actual_exec_time().as_secs_f64().to_string(),
                ),
                (
                    "slack_time_secs",
                    task.slack_time(current_time).as_secs_f64().to_string(),
                ),
                (
                    "return_data_bytes",
                    task.parameters()
                        .get("return_data_bytes")
                        .unwrap()
                        .to_string(),
                ),
            ]),
        );

        let msg = Message::Task(task.clone());
        let msg_s = bincode::serialize(&msg).unwrap();
        stream.write(&msg_s).await.unwrap();

        let current_time = SystemTime::now();
        log_metric(
            client_id,
            LogMetricType::ClientTaskSubmitted,
            task_id,
            &HashMap::from([
                (
                    "deadline",
                    task.deadline()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64()
                        .to_string(),
                ),
                (
                    "time_to_deadline_secs",
                    task.deadline()
                        .duration_since(current_time)
                        .unwrap()
                        .as_secs_f64()
                        .to_string(),
                ),
                (
                    "exec_time_secs",
                    task.actual_exec_time().as_secs_f64().to_string(),
                ),
                (
                    "slack_time_secs",
                    task.slack_time(current_time).as_secs_f64().to_string(),
                ),
                (
                    "return_data_bytes",
                    task.parameters()
                        .get("return_data_bytes")
                        .unwrap()
                        .to_string(),
                ),
            ]),
        );

        let mut buffer = [0 as u8; 1024];
        stream.read(&mut buffer).await.unwrap();
        let message: Message = bincode::deserialize(&buffer).unwrap();

        match message {
            Message::TaskAccepted => {
                log_metric(
                    client_id,
                    LogMetricType::ClientTaskAccepted,
                    task_id,
                    &empty_map,
                );

                let (mut res_stream, _addr) = match timeout(
                    initial_time_to_deadline * 10,
                    listener.accept(),
                )
                .await
                {
                    Ok(res) => res.unwrap(),
                    Err(_) => {
                        log_metric(
                            client_id,
                            LogMetricType::ClientTaskTimeout,
                            task_id,
                            &empty_map,
                        );
                        continue;
                    }
                };

                let mut buffer = vec![0; return_data_bytes + 50];
                res_stream.read(&mut buffer).await.unwrap();
                let mut recv_time = SystemTime::now();
                let mut task_result: TaskResult =
                    bincode::deserialize(&buffer).unwrap();

                while task_result.task_id() != task_id {
                    warn!(
                        "Received task result for {}, but waiting for {} \
                        (most likely a late result of a timed out task)",
                        task_result.task_id(),
                        task_id
                    );
                    let (mut res_stream, _addr) =
                        listener.accept().await.unwrap();
                    let mut buffer = [0; 1024];
                    res_stream.read(&mut buffer).await.unwrap();
                    recv_time = SystemTime::now();
                    task_result = bincode::deserialize(&buffer).unwrap();
                }

                if task_result.exec_time() > actual_exec_time {
                    let exec_time_plus =
                        task_result.exec_time() - actual_exec_time;
                    debug!(
                        "Task {} exec time was {:?} longer than expected, \
                        subtract diff from receive time",
                        task_id, exec_time_plus
                    );
                    recv_time = recv_time - exec_time_plus;
                } else {
                    let exec_time_minus =
                        actual_exec_time - task_result.exec_time();
                    debug!(
                        "Task {} exec time was {:?} shorter than expected, \
                        adding diff to receive time",
                        task_id, exec_time_minus
                    );
                    recv_time = recv_time + exec_time_minus;
                }

                match deadline.duration_since(recv_time) {
                    Ok(time_to_deadline) => log_metric(
                        client_id,
                        LogMetricType::ClientTaskResultReceivedWithinDeadline,
                        task_id,
                        &HashMap::from([
                            (
                                "deadline",
                                task.deadline()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs_f64()
                                    .to_string(),
                            ),
                            (
                                "exec_time_secs",
                                task_result
                                    .exec_time()
                                    .as_secs_f64()
                                    .to_string(),
                            ),
                            (
                                "time_to_deadline_secs",
                                time_to_deadline.as_secs_f64().to_string(),
                            ),
                            (
                                "return_data_bytes",
                                task_result.value().len().to_string(),
                            ),
                            (
                                "initial_slack_time_secs",
                                (slack_time_ms / 1000.0).to_string(),
                            ),
                        ]),
                    ),
                    Err(_) => log_metric(
                        client_id,
                        LogMetricType::ClientTaskResultReceivedAfterDeadline,
                        task_id,
                        &HashMap::from([
                            (
                                "deadline",
                                task.deadline()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs_f64()
                                    .to_string(),
                            ),
                            (
                                "exec_time_secs",
                                task_result
                                    .exec_time()
                                    .as_secs_f64()
                                    .to_string(),
                            ),
                            (
                                "time_to_deadline_secs",
                                format!(
                                    "-{}",
                                    recv_time
                                        .duration_since(deadline)
                                        .unwrap()
                                        .as_secs_f64()
                                        .to_string()
                                ),
                            ),
                            (
                                "return_data_bytes",
                                task_result.value().len().to_string(),
                            ),
                            (
                                "initial_slack_time_secs",
                                (slack_time_ms / 1000.0).to_string(),
                            ),
                        ]),
                    ),
                }
            }
            Message::TaskRejected(err_msg) => {
                log_metric(
                    client_id,
                    LogMetricType::ClientTaskRejected,
                    task_id,
                    &HashMap::from([("error_message", err_msg)]),
                );
                let time_to_deadline = task.time_to_deadline();
                debug!(
                    "Sleeping {:?} until deadline of rejected task",
                    time_to_deadline
                );
                sleep(time_to_deadline).await;
            }
            _ => {
                panic!("Received unexpected message: {:#?}", message);
            }
        }
        if interval_lambda > 0.0 {
            let interval = Duration::from_secs_f64(get_poisson_interval(
                interval_lambda,
                &mut rng,
            ));
            if interval > task.actual_exec_time() {
                let interval = interval - task.actual_exec_time();
                debug!("Sleeping for {:?}...", interval);
                sleep(interval).await;
            } else {
                debug!("Sleeping for {:?}...", Duration::ZERO);
            }
        } else {
            break;
        }
    }
}

fn get_poisson_interval(lambda: f64, rng: &mut ChaCha8Rng) -> f64 {
    let p: f64 = rng.gen();
    let one_minus_p = 1.0 - p;
    let inter_arrival_time = -one_minus_p.ln() / lambda;
    inter_arrival_time
}
