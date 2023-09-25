use log::{trace, warn};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    // If no data is sent via a stream, the bincode deserializer
    // returns the first variant of the enum.
    // We catch that explicitly with the 'Empty' variant
    Empty,
    WorkerHello(Uuid),
    ClientHello(Uuid),
    SchedulerHello,
    Task(Task),
    TaskAccepted,
    TaskRejected(String),
    TaskResult(TaskResult),
    TaskFinished(FinishedTask),
}

#[derive(Debug)]
pub enum LogMetricType {
    ClientTaskCreated,
    ClientTaskSubmitted,
    ClientTaskAccepted,
    ClientTaskRejected,
    ClientTaskResultReceivedWithinDeadline,
    ClientTaskResultReceivedAfterDeadline,
    ClientTaskTimeout,
    SchedulerTaskReceived,
    SchedulerWorkerDensity,
    SchedulerTaskFinishedScheduling,
    SchedulerTaskAccepted,
    SchedulerTaskRejected,
    SchedulerTaskSentToWorker,
    SchedulerTaskPreempted,
    SchedulerTaskFinished,
    WorkerTaskReceived,
    WorkerTaskPreempted,
    WorkerTaskCalculationStarted,
    WorkerTaskCalculationFinished,
    WorkerTaskResultSentToClient,
    WorkerTaskResumed,
    WorkerTaskFinishedSentToScheduler,
}

pub fn log_metric(
    entity_id: Uuid,
    log_metric_type: LogMetricType,
    task_id: Uuid,
    data: &impl Serialize,
) {
    trace!(
        "METRIC {} {:?} {} {}",
        entity_id,
        log_metric_type,
        task_id,
        serde_json::to_string(&data).unwrap()
    );
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    id: Uuid,
    rpc_id: u8,
    client_id: Uuid,
    client_addr: SocketAddrV4,
    deadline: SystemTime,
    initial_time_to_deadline: Duration,
    connection_setup_time: Duration,
    worst_case_exec_time: Duration,
    actual_exec_time: Duration,
    prev_exec_time: Duration,
    start_time: Option<SystemTime>,
    preempt_time: Option<SystemTime>,
    resume_time: Option<SystemTime>,
    finish_time: Option<SystemTime>,
    parameters: HashMap<String, String>,
    pub slack_time: Option<Duration>,
}

impl Task {
    pub fn new(
        rpc_id: u8,
        client_id: Uuid,
        client_addr: SocketAddrV4,
        deadline: SystemTime,
        initial_time_to_deadline: Duration,
        connection_setup_time: Duration,
        worst_case_exec_time: Duration,
        actual_exec_time: Duration,
        parameters: HashMap<String, String>,
    ) -> Self {
        let id = Uuid::new_v4();
        let start_time = None;
        let preempt_time = None;
        let resume_time = None;
        let finish_time = None;
        let prev_exec_time = Duration::ZERO;
        Task {
            id,
            rpc_id,
            client_id,
            client_addr,
            deadline,
            initial_time_to_deadline,
            connection_setup_time,
            worst_case_exec_time,
            actual_exec_time,
            prev_exec_time,
            start_time,
            preempt_time,
            resume_time,
            finish_time,
            parameters,
            slack_time: None,
        }
    }
    pub fn start(&mut self) {
        if let Some(start_time) = self.start_time {
            warn!(
                "Task {} already has a start_time: {:#?}",
                self.id, start_time
            );
        } else {
            self.start_time = Some(SystemTime::now());
        }
    }
    pub fn preempt(&mut self) {
        let current_time = SystemTime::now();
        let exec_time;
        if let Some(resume_time) = self.resume_time {
            exec_time = current_time.duration_since(resume_time).unwrap();
        } else {
            let start_time = self.start_time.expect("Task has no start_time!");
            exec_time = current_time.duration_since(start_time).unwrap();
        }
        self.prev_exec_time += exec_time;
        self.preempt_time = Some(current_time);
    }
    pub fn resume(&mut self) {
        self.resume_time = Some(SystemTime::now());
    }
    pub fn finish(&mut self) -> Duration {
        let current_time = SystemTime::now();
        let exec_time;
        if let Some(resume_time) = self.resume_time {
            exec_time = current_time.duration_since(resume_time).unwrap();
        } else {
            let start_time = self.start_time.expect("Task has no start_time!");
            exec_time = current_time.duration_since(start_time).unwrap();
        }
        self.finish_time = Some(current_time);
        self.prev_exec_time + exec_time
    }
    pub fn time_to_deadline(&self) -> Duration {
        match self.deadline().duration_since(SystemTime::now()) {
            Ok(v) => v,
            Err(_) => Duration::ZERO,
        }
    }
    pub fn current_exec_time(&self) -> Duration {
        let current_time = SystemTime::now();
        let exec_time;

        if let None = self.start_time {
            // 0: Task not started
            exec_time = Duration::ZERO;
        } else if let (None, None) = (self.preempt_time, self.resume_time) {
            // 1: Task is running
            exec_time = current_time
                .duration_since(self.start_time.unwrap())
                .unwrap();
        } else if let (Some(_), None) = (self.preempt_time, self.resume_time) {
            // 2: Task is preempted first time
            exec_time = self.prev_exec_time;
        } else if let (Some(preempt_time), Some(resume_time)) =
            (self.preempt_time, self.resume_time)
        {
            if preempt_time < resume_time {
                // 3: Task is running again after being suspeneded
                exec_time = self.prev_exec_time
                    + current_time.duration_since(resume_time).unwrap();
            } else {
                // 4: Task is preempted again
                exec_time = self.prev_exec_time;
            }
        } else {
            panic!("This should not happen!");
        }
        exec_time
    }
    pub fn remaining_exec_time(&self) -> Duration {
        let current_exec_time = self.current_exec_time();
        if current_exec_time < self.actual_exec_time() {
            self.actual_exec_time() - current_exec_time
        } else {
            Duration::ZERO
        }
    }
    pub fn slack_time(&self, start_time: SystemTime) -> Duration {
        // slack_time = deadline - start_time - remaining_exec_time
        let remaining_exec_time = self.remaining_exec_time();
        let slack_time;
        if let Ok(time_to_deadline) = self.deadline.duration_since(start_time)
        {
            if remaining_exec_time > time_to_deadline {
                slack_time = Duration::ZERO;
            } else {
                slack_time = time_to_deadline - remaining_exec_time;
            }
        } else {
            slack_time = Duration::ZERO;
        }
        slack_time
    }
    pub fn density(&self, start_time: SystemTime) -> f64 {
        // density = exec_time / time_to_deadline
        if let Ok(time_to_deadline) = self.deadline.duration_since(start_time)
        {
            self.remaining_exec_time().as_secs_f64()
                / time_to_deadline.as_secs_f64()
        } else {
            1.0
        }
    }
    pub fn add_to_prev_exec_time(&mut self, exec_time: Duration) {
        self.prev_exec_time += exec_time;
    }
    pub fn sub_exec_time(&mut self, exec_time: Duration) {
        self.actual_exec_time -= exec_time;
    }
    pub fn add_exec_time(&mut self, exec_time: Duration) {
        self.actual_exec_time += exec_time;
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn client_id(&self) -> Uuid {
        self.client_id
    }
    pub fn client_addr(&self) -> SocketAddrV4 {
        self.client_addr
    }
    pub fn deadline(&self) -> SystemTime {
        self.deadline
    }
    pub fn deadline_subtract_delay(&mut self, delay: Duration) {
        self.deadline -= delay;
    }
    pub fn initial_time_to_deadline(&self) -> Duration {
        self.initial_time_to_deadline
    }
    pub fn connection_setup_time(&self) -> Duration {
        self.connection_setup_time
    }
    pub fn start_time(&self) -> Option<SystemTime> {
        self.start_time
    }
    pub fn actual_exec_time(&self) -> Duration {
        self.actual_exec_time
    }
    pub fn prev_exec_time(&self) -> Duration {
        self.prev_exec_time
    }
    pub fn parameters(&self) -> HashMap<String, String> {
        self.parameters.clone()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskResult {
    task_id: Uuid,
    value: String,
    exec_time: Duration,
}

impl TaskResult {
    pub fn new(
        task_id: Uuid,
        value: String,
        exec_time: Duration,
    ) -> TaskResult {
        TaskResult {
            task_id,
            value,
            exec_time,
        }
    }
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
    pub fn value(&self) -> String {
        self.value.clone()
    }
    pub fn exec_time(&self) -> Duration {
        self.exec_time
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinishedTask {
    pub task_id: Uuid,
    pub expected_exec_time: Duration,
    pub real_exec_time: Duration,
    pub timestamp: SystemTime,
}
