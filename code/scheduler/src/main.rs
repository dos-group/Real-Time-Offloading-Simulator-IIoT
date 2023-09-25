mod global_schedule;
mod partitioned_schedule;
mod task_queue;

use bincode;
use core::panic;
use env_logger::Builder;
use local_ip_address::local_ip;
use log::{debug, error, info, warn, LevelFilter};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use utils::{log_metric, FinishedTask, LogMetricType, Message, Task};
use uuid::Uuid;

use global_schedule::GlobalSchedule;
use partitioned_schedule::{AllocationHeuristic, PartitionedSchedule};
use scheduler::{Schedule, Worker, ID};

#[tokio::main]
async fn main() {
    let mut log_builder = Builder::new();
    log_builder
        .filter_level(LevelFilter::Off)
        .parse_default_env()
        .format_timestamp_micros()
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        panic!(
            "Usage: {} port {{GS|PSFF|PSBF|PSWF}} delay_factor",
            &args[0]
        );
    }
    let port = &args[1];
    let schedule = Arc::new(Mutex::new(get_schedule(&args[2])));
    let delay_factor: f64 = args[3].parse().unwrap();
    assert!(delay_factor >= 0.0);

    let listener = TcpListener::bind(format!(
        "{}:{}",
        local_ip().unwrap().to_string(),
        port
    ))
    .await
    .unwrap();

    info!(
        "Listening at {:?} for workers and tasks",
        listener.local_addr().unwrap()
    );

    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        let schedule = schedule.clone();

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            socket.read(&mut buf).await.unwrap();
            if let Ok(message) = bincode::deserialize(&buf) {
                match message {
                    Message::WorkerHello(worker_id) => {
                        handle_worker(worker_id, socket, &schedule).await;
                    }
                    Message::ClientHello(client_id) => {
                        handle_client(
                            client_id,
                            socket,
                            &schedule,
                            delay_factor,
                        )
                        .await;
                    }
                    _ => {
                        warn!("Received unexpected: {:#?}", message);
                    }
                }
            } else {
                warn!("Received undecodable message from {}", addr);
            };
        });
    }
}

async fn handle_worker(
    worker_id: Uuid,
    mut socket: TcpStream,
    schedule: &Arc<Mutex<Box<dyn Schedule + Send>>>,
) {
    let msg = Message::SchedulerHello;
    let msg_s = bincode::serialize(&msg).unwrap();
    socket.write(&msg_s).await.unwrap();
    info!("New worker connected: {}", worker_id);

    let (mut reader, writer) = socket.into_split();
    let worker = Worker::new(worker_id, writer);
    schedule.lock().await.add_worker(worker).await;

    loop {
        let mut buf = [0; 1024];
        let n = reader.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        let message: Message = bincode::deserialize(&buf).unwrap();
        if let Message::TaskFinished(finished_task) = message {
            let finished_task_id = finished_task.task_id;
            schedule
                .lock()
                .await
                .finish_task(finished_task, worker_id)
                .await;
            log_metric(
                ID,
                LogMetricType::SchedulerTaskFinished,
                finished_task_id,
                &HashMap::from([("worker_id", worker_id)]),
            );
        } else {
            error!(
                "Received unexpected message: {:#?}\n\
                    Expected only TaskFinished at this point",
                message
            );
        };
    }
    schedule.lock().await.remove_worker(worker_id).await;
    info!("Worker {} disconnected", worker_id);
}

async fn handle_client(
    client_id: Uuid,
    socket: TcpStream,
    schedule: &Arc<Mutex<Box<dyn Schedule + Send>>>,
    delay_factor: f64,
) {
    let msg = Message::SchedulerHello;
    let msg_s = bincode::serialize(&msg).unwrap();
    let (mut reader, writer) = socket.into_split();
    let writer = Arc::new(Mutex::new(writer));
    writer.lock().await.write(&msg_s).await.unwrap();
    info!("New client connected: {}", client_id);

    loop {
        let mut buf = [0; 1024];
        if let Ok(n) = reader.read(&mut buf).await {
            if n == 0 {
                break;
            }
        } else {
            break;
        }
        let message: Message = bincode::deserialize(&buf).unwrap();
        if let Message::Task(task) = message {
            let writer = writer.clone();
            handle_task(task, &writer, &schedule, delay_factor).await;
        } else {
            panic!(
                "Received unexpected message: {:#?}\n\
                    Expected only a Task from a client",
                message
            );
        };
    }
    info!("Client {} disconnected", client_id);
}

async fn handle_task(
    mut task: Task,
    socket: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    schedule: &Arc<Mutex<Box<dyn Schedule + Send>>>,
    delay_factor: f64,
) {
    let empty_map: HashMap<String, String> = HashMap::new();
    let task_id = task.id();
    let current_time = SystemTime::now();
    let mut time_to_deadline = if let Ok(time_to_deadline) =
        task.deadline().duration_since(current_time)
    {
        log_metric(
            ID,
            LogMetricType::SchedulerTaskReceived,
            task_id,
            &HashMap::from([
                ("client_id", task.client_id().to_string()),
                (
                    "time_to_deadline_secs",
                    time_to_deadline.as_secs_f64().to_string(),
                ),
            ]),
        );
        time_to_deadline
    } else {
        let time_to_deadline =
            current_time.duration_since(task.deadline()).unwrap();
        log_metric(
            ID,
            LogMetricType::SchedulerTaskReceived,
            task_id,
            &HashMap::from([
                ("client_id", task.client_id().to_string()),
                (
                    "time_to_deadline_secs",
                    format!("-{}", time_to_deadline.as_secs_f64().to_string()),
                ),
            ]),
        );
        Duration::ZERO
    };

    // Adjusting the deadline based on the delay values the client
    // provided and the delay factor configuration
    let delay_duration = total_delay(&task, time_to_deadline);
    let delay_f64 = delay_duration.as_secs_f64();
    let delay = Duration::from_secs_f64(delay_f64 * delay_factor);
    task.deadline_subtract_delay(delay);
    if time_to_deadline > delay {
        time_to_deadline -= delay;
    } else {
        time_to_deadline = Duration::ZERO;
    }
    debug!(
        "task id: {:?} total delay: {:?} delay aware time to deadline: {:?}",
        task.id(),
        delay,
        time_to_deadline
    );

    let resp;
    if time_to_deadline < task.actual_exec_time() {
        log_metric(
            ID,
            LogMetricType::SchedulerTaskRejected,
            task_id,
            &empty_map,
        );
        resp = Message::TaskRejected(String::from(
            "Task arrived with insufficient time for execution",
        ));
    } else {
        resp = match schedule.lock().await.add_task(task).await {
            Ok((worker_id, worker_queue_len)) => {
                log_metric(
                    ID,
                    LogMetricType::SchedulerTaskAccepted,
                    task_id,
                    &HashMap::from([
                        ("worker_id", worker_id.to_string()),
                        ("worker_queue_len", worker_queue_len.to_string()),
                    ]),
                );
                Message::TaskAccepted
            }
            Err(err_msg) => {
                log_metric(
                    ID,
                    LogMetricType::SchedulerTaskRejected,
                    task_id,
                    &empty_map,
                );
                Message::TaskRejected(err_msg)
            }
        };
        check_overdue(&schedule).await;
    }
    let resp_s = bincode::serialize(&resp).unwrap();
    socket.lock().await.write(&resp_s).await.unwrap();
}

async fn check_overdue(schedule: &Arc<Mutex<Box<dyn Schedule + Send>>>) {
    // Ugly workaround for the bug that I cannot find
    // Everytime a new task comes in we check if there is some overdue task
    // for which we did not receive as TaskFinished message from the worker
    // Why this message gets lost sometimes I absolutely cannot tell
    debug!("Checking for dangling task");
    let overdue_tasks = schedule.lock().await.get_overdue_tasks();
    for (worker_id, overdue_task) in overdue_tasks {
        warn!(
            "Task {} overdue! ({:?})",
            overdue_task.id(),
            overdue_task.current_exec_time()
        );
        let finished_overdue_task = FinishedTask {
            task_id: overdue_task.id(),
            expected_exec_time: overdue_task.actual_exec_time(),
            real_exec_time: overdue_task.current_exec_time(),
            timestamp: SystemTime::now(),
        };
        schedule
            .lock()
            .await
            .finish_task(finished_overdue_task, worker_id)
            .await;
        warn!("Force-finished overdue task {}", overdue_task.id());
        log_metric(
            ID,
            LogMetricType::SchedulerTaskFinished,
            overdue_task.id(),
            &HashMap::from([("worker_id", worker_id)]),
        );
    }
}

fn total_delay(task: &Task, time_to_deadline: Duration) -> Duration {
    let mut delay = task.initial_time_to_deadline() - time_to_deadline;
    debug!(
        "task id: {} \
        initial time to deadline: {:?} \
        connection setup time: {:?} \
        execution time: {:?} \
        delay {:?}",
        task.id(),
        task.initial_time_to_deadline(),
        task.connection_setup_time(),
        task.actual_exec_time(),
        delay
    );

    if task.connection_setup_time() > task.actual_exec_time() {
        let connection_setup_execution_time_diff =
            task.connection_setup_time() - task.actual_exec_time();
        delay += connection_setup_execution_time_diff;
        debug!(
            "task id: {:?} connection setup execution time diff: {:?}",
            task.id(),
            connection_setup_execution_time_diff,
        );
    }
    delay
}

fn get_schedule(param: &str) -> Box<dyn Schedule + Send> {
    match param {
        "GS" => {
            info!(
                "Using global schedule with single ready queue \
                for all workers"
            );
            Box::new(GlobalSchedule::new())
        }
        "PSFF" => {
            info!(
                "Using partitioned schedule with a separate ready queue \
                for each worker and first fit task allocation"
            );
            Box::new(PartitionedSchedule::new(AllocationHeuristic::FF))
        }
        "PSBF" => {
            info!(
                "Using partitioned schedule with a separate ready queue \
                for each worker and best fit task allocation"
            );
            Box::new(PartitionedSchedule::new(AllocationHeuristic::BF))
        }
        "PSWF" => {
            info!(
                "Using partitioned schedule with a separate ready queue \
                for each worker and worst fit task allocation"
            );
            Box::new(PartitionedSchedule::new(AllocationHeuristic::WF))
        }
        _ => panic!(
            "Invalid ready queue type: {} \n\
            Must be one of\n\
            GS (global schedule),\n\
            PSFF (partitioned schedule with first fit task allocation)\n\
            PSBF (partitioned schedule with best fit task allocation)\n\
            PSWF (partitioned schedule with worst fit task allocation)",
            param
        ),
    }
}
