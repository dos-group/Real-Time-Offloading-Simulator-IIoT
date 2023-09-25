use async_trait::async_trait;
use bincode;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use utils::{log_metric, FinishedTask, LogMetricType, Message, Task};
use uuid::{uuid, Uuid};

// For the tests
pub mod task_queue;
// pub mod worker_schedule;

pub const ID: Uuid = uuid!("00000000-0000-0000-0000-000000000000");

#[derive(Debug)]
pub struct Worker {
    id: Uuid,
    stream: OwnedWriteHalf,
}
impl Worker {
    pub fn new(id: Uuid, stream: OwnedWriteHalf) -> Self {
        Worker { id, stream }
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub async fn send_task(&mut self, mut task: Task) {
        let task_id = task.id();
        let deadline = task.deadline();
        task.slack_time = Some(task.slack_time(SystemTime::now()));
        let msg = Message::Task(task);
        let msg_s = bincode::serialize(&msg).unwrap();
        self.stream.write(&msg_s).await.unwrap();

        let time_to_deadline = deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);

        log_metric(
            ID,
            LogMetricType::SchedulerTaskSentToWorker,
            task_id,
            &HashMap::from([
                ("worker_id", self.id().to_string()),
                (
                    "time_to_deadline",
                    time_to_deadline.as_secs_f64().to_string(),
                ),
            ]),
        );
    }
}

#[async_trait]
pub trait Schedule {
    async fn add_worker(&mut self, worker: Worker);

    async fn remove_worker(&mut self, worker_id: Uuid);

    async fn add_task(&mut self, task: Task) -> Result<(Uuid, usize), String>;

    async fn finish_task(
        &mut self,
        finished_task: FinishedTask,
        worker_id: Uuid,
    );

    fn get_overdue_tasks(&mut self) -> Vec<(Uuid, Task)> {
        Vec::new()
    }
}
