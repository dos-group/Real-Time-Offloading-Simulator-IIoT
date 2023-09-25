use bincode;
use env_logger::Builder;
use log::{debug, error, info, warn, LevelFilter};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddrV4;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use utils::{
    log_metric, FinishedTask, LogMetricType, Message, Task, TaskResult,
};
use uuid::Uuid;

#[derive(Debug)]
enum TaskStatus {
    RUNNING,
    PREEMPTED,
}

#[derive(Debug)]
struct RunningTask {
    pid: u32,
    task: Task,
    task_status: TaskStatus,
    preemptions: u32,
}
impl RunningTask {
    pub fn new(mut task: Task, task_bin_path: String) -> (Self, Child) {
        // let exec_time = (task.actual_exec_time().as_secs_f64() * 0.9) * 1000.0;
        let exec_time = task.actual_exec_time().as_millis();
        let proc = Command::new(&task_bin_path)
            .stdout(Stdio::piped())
            .arg(exec_time.to_string())
            .arg(
                task.parameters()
                    .get("return_data_bytes")
                    .unwrap_or(&String::from("1000")),
            )
            .spawn()
            .expect("Failed to spawn process");

        task.start();
        (
            RunningTask {
                pid: proc.id().unwrap(),
                task,
                task_status: TaskStatus::RUNNING,
                preemptions: 0,
            },
            proc,
        )
    }
    pub fn preemptions(&self) -> u32 {
        self.preemptions
    }
    pub async fn preempt(&mut self) -> Result<(), String> {
        if let TaskStatus::PREEMPTED = self.task_status {
            panic!("Preempting already preempted task {}", self.task.id());
        }
        let return_value = Command::new("kill")
            .stdout(Stdio::piped())
            .arg("-STOP")
            .arg(self.pid.to_string())
            .spawn()
            .expect("Failed to stop process")
            .wait()
            .await
            .expect("Failed to stop process");

        if return_value.success() {
            self.task.preempt();
            self.task_status = TaskStatus::PREEMPTED;
            self.preemptions += 1;
            Ok(())
        } else {
            let msg = format!(
                "Tried to preempt already finished task {}",
                self.task.id()
            );
            Err(msg)
        }
    }
    pub async fn resume(&mut self) -> Result<(), String> {
        if let TaskStatus::RUNNING = self.task_status {
            panic!("Resuming already running task {}", self.task.id());
        }

        let return_value = Command::new("kill")
            .stdout(Stdio::piped())
            .arg("-CONT")
            .arg(self.pid.to_string())
            .spawn()
            .expect("Failed to resume process")
            .wait()
            .await
            .expect("Failed to resume process");

        if return_value.success() {
            self.task.resume();
            self.task_status = TaskStatus::RUNNING;
            Ok(())
        } else {
            let msg = format!(
                "Tried to resume already finished task {}",
                self.task.id()
            );
            Err(msg)
        }
    }
}

type TaskList = Arc<Mutex<Vec<RunningTask>>>;

#[tokio::main]
async fn main() {
    let worker_id = Uuid::new_v4();
    let mut log_builder = Builder::new();
    log_builder
        .filter_level(LevelFilter::Off)
        .parse_default_env()
        .format_timestamp_micros()
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        panic!("Usage: {} scheduler_addr task_bin_path", &args[0]);
    }
    let scheduler_addr: SocketAddrV4 = args[1].parse().unwrap();
    let task_bin_path: String = args[2].parse().unwrap();

    info!("Starting worker {}", worker_id);

    let task_list: TaskList = Arc::new(Mutex::new(Vec::new()));
    let mut stream = TcpStream::connect(scheduler_addr).await.unwrap();
    let msg = Message::WorkerHello(worker_id);
    let msg_s = bincode::serialize(&msg).unwrap();
    stream.write(&msg_s).await.unwrap();

    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));

    loop {
        let task_list = task_list.clone();
        let stream_to_scheduler = writer.clone();
        let task_bin_path_cloned = task_bin_path.clone();

        let mut buffer = [0; 1024];
        reader.read(&mut buffer).await.unwrap();
        let message: Message = bincode::deserialize(&buffer).unwrap();

        match message {
            Message::Task(task) => {
                tokio::spawn(async move {
                    handle_task(
                        worker_id,
                        task,
                        &task_list,
                        task_bin_path_cloned,
                        &stream_to_scheduler,
                    )
                    .await;
                });
            }
            Message::SchedulerHello => {
                info!("Connected to scheduler, waiting for tasks");
            }
            Message::Empty => {
                error!("Received empty message, exiting");
                break;
            }
            _ => {
                warn!("Received unexpected message: {:#?}", message);
            }
        }
    }
}

async fn handle_task(
    worker_id: Uuid,
    task: Task,
    task_list: &TaskList,
    task_bin_path: String,
    stream_to_scheduler: &Arc<Mutex<OwnedWriteHalf>>,
) {
    let task_id = task.id();
    let client_addr = task.client_addr();
    let current_time = SystemTime::now();

    if task.slack_time.unwrap() < task.slack_time(current_time) {
        warn!(
            "Task {:?} has larger slack time at worker than at scheduler: {:?}",
            task_id, task.slack_time(current_time)
        )
    }
    let scheduler_worker_delay =
        task.slack_time.unwrap() - task.slack_time(current_time);
    debug!(
        "Task {:?} took {:?} to arrive at worker",
        task_id, scheduler_worker_delay
    );
    let deadline = task.deadline();
    let expected_exec_time = task.actual_exec_time();
    let time_to_deadline = match deadline.duration_since(current_time) {
        Ok(time_to_deadline) => time_to_deadline,
        Err(_) => Duration::ZERO,
    };
    log_metric(
        worker_id,
        LogMetricType::WorkerTaskReceived,
        task_id,
        &HashMap::from([
            ("client_id", task.client_id().to_string()),
            (
                "time_to_deadline_secs",
                time_to_deadline.as_secs_f64().to_string(),
            ),
            (
                "scheduler_worker_delay_secs",
                scheduler_worker_delay.as_secs_f64().to_string(),
            ),
        ]),
    );
    let ((task_result, real_exec_time, preempted_task_id), stream_to_client) = tokio::join!(
        // The worker starts to process the task and already establishes
        // a connection to the client to minimize latency when the
        // task result is available
        run_task(worker_id, task, &task_list, task_bin_path,),
        connect_to_client(client_addr)
    );

    if let Ok(stream_to_client) = stream_to_client {
        tokio::join!(
            send_task_finished_to_scheduler(
                worker_id,
                &stream_to_scheduler,
                task_id,
                expected_exec_time,
                real_exec_time,
            ),
            send_result_to_client(
                worker_id,
                stream_to_client,
                task_result,
                client_addr,
                task_id,
                deadline
            ),
            finish_task(
                worker_id,
                task_id,
                preempted_task_id,
                &task_list,
                expected_exec_time,
                real_exec_time
            ),
        );
    } else {
        warn!("Could not reach client at {:?}", client_addr);
        tokio::join!(
            send_task_finished_to_scheduler(
                worker_id,
                &stream_to_scheduler,
                task_id,
                expected_exec_time,
                real_exec_time,
            ),
            finish_task(
                worker_id,
                task_id,
                preempted_task_id,
                &task_list,
                expected_exec_time,
                real_exec_time
            ),
        );
    }
}

async fn run_task(
    worker_id: Uuid,
    task: Task,
    task_list: &TaskList,
    task_bin_path: String,
) -> (TaskResult, Duration, Option<Uuid>) {
    let task_id = task.id();
    let exec_time = task.actual_exec_time();
    let proc;
    let mut preempted_task_id: Option<Uuid> = None;
    let start_time = SystemTime::now();
    {
        let mut task_list = task_list.lock().await;
        if let Some(running_task) = task_list.last_mut() {
            match running_task.preempt().await {
                Ok(()) => {
                    preempted_task_id = Some(running_task.task.id());
                    let exec_time = running_task.task.current_exec_time();
                    let preemption_overhead =
                        SystemTime::now().duration_since(start_time).unwrap();
                    log_metric(
                        worker_id,
                        LogMetricType::WorkerTaskPreempted,
                        running_task.task.id(),
                        &HashMap::from([
                            ("new_task", task_id.to_string()),
                            (
                                "exec_time_secs",
                                exec_time.as_secs_f64().to_string(),
                            ),
                            (
                                "preemption_overhead_secs",
                                preemption_overhead.as_secs_f64().to_string(),
                            ),
                        ]),
                    );
                }
                Err(msg) => debug!("{}", msg),
            }
        }
        debug!("Task queue: {:#?}", task_list);
        let start_time = SystemTime::now();
        let (running_task, proc_handle) =
            RunningTask::new(task, task_bin_path);
        let start_overhead =
            SystemTime::now().duration_since(start_time).unwrap();
        log_metric(
            worker_id,
            LogMetricType::WorkerTaskCalculationStarted,
            task_id,
            &HashMap::from([
                ("exec_time_secs", exec_time.as_secs_f64().to_string()),
                (
                    "start_overhead_secs",
                    start_overhead.as_secs_f64().to_string(),
                ),
            ]),
        );
        proc = proc_handle;
        task_list.push(running_task);
    }
    let output = proc.wait_with_output().await.unwrap();
    let exec_time;
    {
        let mut task_list = task_list.lock().await;
        let task = task_list
            .iter_mut()
            .find(|rt| rt.task.id() == task_id)
            .unwrap();
        exec_time = task.task.finish();
        log_metric(
            worker_id,
            LogMetricType::WorkerTaskCalculationFinished,
            task_id,
            &HashMap::from([
                ("exec_time_secs", exec_time.as_secs_f64().to_string()),
                ("preemptions", task.preemptions().to_string()),
            ]),
        );
    }
    let output_str = String::from_utf8(output.stdout.clone()).unwrap();
    (
        TaskResult::new(task_id, output_str, exec_time),
        exec_time,
        preempted_task_id,
    )
}

async fn connect_to_client(
    client_addr: SocketAddrV4,
) -> Result<tokio::net::TcpStream, std::io::Error> {
    debug!("Connecting to client at {}...", client_addr);
    let result = TcpStream::connect(client_addr).await;
    debug!("Connected to client at {}", client_addr);
    result
}

async fn send_result_to_client(
    worker_id: Uuid,
    mut stream: TcpStream,
    task_result: TaskResult,
    client_addr: SocketAddrV4,
    task_id: Uuid,
    deadline: SystemTime,
) {
    let task_result_s = bincode::serialize(&task_result).unwrap();
    stream.write(&task_result_s).await.unwrap();

    let time_to_deadline = match deadline.duration_since(SystemTime::now()) {
        Ok(time_to_deadline) => time_to_deadline,
        Err(_) => Duration::ZERO,
    };
    log_metric(
        worker_id,
        LogMetricType::WorkerTaskResultSentToClient,
        task_id,
        &HashMap::from([
            ("client_addr", client_addr.to_string()),
            ("return_data_bytes", task_result.value().len().to_string()),
            (
                "time_to_deadline_secs",
                time_to_deadline.as_secs_f64().to_string(),
            ),
        ]),
    );
}

async fn send_task_finished_to_scheduler(
    worker_id: Uuid,
    stream_to_scheduler: &Arc<Mutex<OwnedWriteHalf>>,
    task_id: Uuid,
    expected_exec_time: Duration,
    real_exec_time: Duration,
) {
    let timestamp = SystemTime::now();
    let msg = Message::TaskFinished(FinishedTask {
        task_id,
        expected_exec_time,
        real_exec_time,
        timestamp,
    });
    let msg_s = bincode::serialize(&msg).unwrap();
    let empty_map: HashMap<String, String> = HashMap::new();

    stream_to_scheduler
        .lock()
        .await
        .write(&msg_s)
        .await
        .unwrap();

    log_metric(
        worker_id,
        LogMetricType::WorkerTaskFinishedSentToScheduler,
        task_id,
        &empty_map,
    );
}

async fn finish_task(
    worker_id: Uuid,
    task_id: Uuid,
    preempted_task_id: Option<Uuid>,
    task_list: &TaskList,
    expected_exec_time: Duration,
    real_exec_time: Duration,
) {
    let mut task_list = task_list.lock().await;
    // Remove pid from list of running processes
    task_list.retain(|t| t.task.id() != task_id);
    // And resume possibly preempted task
    if let Some(preempted_task_id) = preempted_task_id {
        debug!(
            "Resuming preempted task {} after finishing {}",
            preempted_task_id, task_id
        );
        let start_time = SystemTime::now();
        if let Some(preempted_task) = task_list
            .iter_mut()
            .find(|rt| rt.task.id() == preempted_task_id)
        {
            if real_exec_time > expected_exec_time {
                let exec_time_diff = real_exec_time - expected_exec_time;
                debug!(
                    "Task {:?} took {:?} longer than expected \
                    adding difference to preempted task",
                    task_id, exec_time_diff,
                );
                preempted_task.task.add_to_prev_exec_time(exec_time_diff);
            }
            let exec_time = preempted_task.task.current_exec_time();
            match preempted_task.resume().await {
                Ok(()) => {
                    let resume_overhead =
                        SystemTime::now().duration_since(start_time).unwrap();
                    log_metric(
                        worker_id,
                        LogMetricType::WorkerTaskResumed,
                        preempted_task.task.id(),
                        &HashMap::from([
                            (
                                "preemptions",
                                preempted_task.preemptions().to_string(),
                            ),
                            (
                                "exec_time_secs",
                                exec_time.as_secs_f64().to_string(),
                            ),
                            (
                                "resume_overhead_secs",
                                resume_overhead.as_secs_f64().to_string(),
                            ),
                        ]),
                    );
                }
                Err(msg) => debug!("{}", msg),
            }
        } else {
            warn!(
                "Preempted task {} not found in task list",
                preempted_task_id
            );
        }
    }
}
