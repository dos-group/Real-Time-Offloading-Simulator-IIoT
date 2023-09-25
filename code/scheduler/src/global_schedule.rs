use crate::{
    task_queue::{SchedulingPolicy, TaskQueue},
    Schedule, Worker, ID,
};
use async_trait::async_trait;
use log::debug;
use std::collections::HashMap;
use utils::{log_metric, FinishedTask, LogMetricType, Task};
use uuid::Uuid;

#[derive(Debug)]
pub struct GlobalSchedule {
    workers: HashMap<Uuid, Worker>,
    worker_schedule: HashMap<Uuid, Vec<Task>>,
    task_queue: TaskQueue,
}

#[async_trait]
impl Schedule for GlobalSchedule {
    async fn add_worker(&mut self, worker: Worker) {
        let worker_id = worker.id();
        self.workers.insert(worker_id, worker);
        self.worker_schedule.insert(worker_id, Vec::new());
        debug!("Tasks in ready queue: {}", self.task_queue.len());
        if let Some(task) = self.task_queue.pop() {
            let mut task_cloned = task.clone();

            self.workers
                .get_mut(&worker_id)
                .unwrap()
                .send_task(task)
                .await;

            task_cloned.start();
            self.worker_schedule
                .get_mut(&worker_id)
                .unwrap()
                .push(task_cloned);
        }
    }
    async fn remove_worker(&mut self, worker_id: Uuid) {
        self.workers.remove(&worker_id).unwrap();
        self.worker_schedule.remove(&worker_id).unwrap();
        // TODO: Migrate tasks to remaining workers
    }
    async fn add_task(&mut self, task: Task) -> Result<(Uuid, usize), String> {
        if self.worker_schedule.len() == 0 {
            return Err(String::from("No worker available"));
        }
        let mut task_cloned = task.clone();
        let task_id = task.id();
        self.task_queue.add_task(task);
        self.task_queue.schedule(task_cloned.id());

        debug!("Tasks in queue: {}", self.task_queue.len());

        if let Some(highest_prio_task) = self.task_queue.highest_prio_task() {
            if highest_prio_task.id() == task_id {
                debug!(
                    "New task {} has highest priority in ready queue",
                    task_id
                );

                if let Some(worker_id) = self.find_idle_worker() {
                    debug!(
                        "Found idle worker {} for task {}",
                        worker_id, task_id
                    );
                    let task = self.task_queue.pop().unwrap();
                    self.workers
                        .get_mut(&worker_id)
                        .unwrap()
                        .send_task(task)
                        .await;

                    task_cloned.start();
                    self.worker_schedule
                        .get_mut(&worker_id)
                        .unwrap()
                        .push(task_cloned);
                } else {
                    debug!("Found no idle worker for task {}", task_id);
                    if let Some(worker_id) =
                        self.find_preempt_worker(task_cloned)
                    {
                        let task = self.task_queue.pop().unwrap();
                        let mut task_cloned = task.clone();
                        self.workers
                            .get_mut(&worker_id)
                            .unwrap()
                            .send_task(task)
                            .await;

                        task_cloned.start();
                        let preempt_task = self
                            .worker_schedule
                            .get_mut(&worker_id)
                            .unwrap()
                            .last_mut()
                            .unwrap();

                        preempt_task.preempt();
                        log_metric(
                            ID,
                            LogMetricType::SchedulerTaskPreempted,
                            preempt_task.id(),
                            &HashMap::from([("new_task", task_id)]),
                        );
                        self.worker_schedule
                            .get_mut(&worker_id)
                            .unwrap()
                            .push(task_cloned);
                    }
                }
            }
        }
        Ok((ID, self.task_queue.len()))
    }
    async fn finish_task(
        &mut self,
        finished_task: FinishedTask,
        worker_id: Uuid,
    ) {
        let worker_schedule =
            self.worker_schedule.get_mut(&worker_id).unwrap();
        let task = worker_schedule.pop().unwrap();

        // We expect the first task in a workers task queue to finish first
        assert_eq!(task.id(), finished_task.task_id);
        if worker_schedule.len() > 0 {
            worker_schedule.last_mut().unwrap().resume();
        } else if worker_schedule.len() == 0 && !self.task_queue.is_empty() {
            let task = self.task_queue.pop().unwrap();
            let mut task_cloned = task.clone();

            self.workers
                .get_mut(&worker_id)
                .unwrap()
                .send_task(task)
                .await;

            task_cloned.start();
            self.worker_schedule
                .get_mut(&worker_id)
                .unwrap()
                .push(task_cloned);
        }
    }
}

impl GlobalSchedule {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            worker_schedule: HashMap::new(),
            task_queue: TaskQueue::new(SchedulingPolicy::EDF),
        }
    }
    fn find_idle_worker(&self) -> Option<Uuid> {
        for (worker_id, schedule) in self.worker_schedule.iter() {
            if schedule.len() == 0 {
                return Some(worker_id.clone());
            }
        }
        None
    }
    fn find_preempt_worker(&self, task: Task) -> Option<Uuid> {
        let task_id = task.id();
        let mut tasks = TaskQueue::new(SchedulingPolicy::EDF);
        let mut task_worker_map = HashMap::new();

        for (worker_id, worker_tasks) in self.worker_schedule.iter() {
            let active_task = worker_tasks.last().unwrap();
            let active_task_id = active_task.id();
            task_worker_map.insert(active_task_id, worker_id);
            tasks.add_task(active_task.clone());
        }
        tasks.add_task(task);
        tasks.schedule(task_id);

        let lowest_prio_task_id = tasks.lowest_prio_task_id().unwrap();

        if lowest_prio_task_id == task_id {
            debug!("Task {} will not preempt any running task", task_id);
            None
        } else {
            let worker_id = task_worker_map.get(&lowest_prio_task_id).unwrap();
            debug!(
                "Task {} will preempt task {} on worker {}",
                task_id, lowest_prio_task_id, worker_id
            );
            Some(**worker_id)
        }
    }
}
