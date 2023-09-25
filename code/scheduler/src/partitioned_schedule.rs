use crate::{
    task_queue::{DeadlineMiss, SchedulingPolicy, TaskQueue},
    Schedule, Worker, ID,
};
use async_trait::async_trait;
use log::debug;
use std::collections::HashMap;
use std::result::Result;
use std::time::{Duration, SystemTime};
use utils::{log_metric, FinishedTask, LogMetricType, Task};
use uuid::Uuid;

#[derive(Debug)]
pub enum AllocationHeuristic {
    FF, // First Fit
    BF, // Best Fit
    WF, // Worst Fit
}

#[derive(Debug)]
pub struct PartitionedSchedule {
    workers: HashMap<Uuid, Worker>,
    worker_schedule: HashMap<Uuid, TaskQueue>,
    allocation: AllocationHeuristic,
}

#[async_trait]
impl Schedule for PartitionedSchedule {
    async fn add_worker(&mut self, worker: Worker) {
        let worker_id = worker.id();
        self.workers.insert(worker_id, worker);
        self.worker_schedule
            .insert(worker_id, TaskQueue::new(SchedulingPolicy::EDF));
    }
    async fn remove_worker(&mut self, worker_id: Uuid) {
        self.workers.remove(&worker_id).unwrap();
        self.worker_schedule.remove(&worker_id).unwrap();
        // TODO: Migrate tasks to remaining workers
    }
    async fn add_task(&mut self, task: Task) -> Result<(Uuid, usize), String> {
        let task_cloned = task.clone();
        let task_id = task.id();

        if let Some((worker_id, mut worker_task_queue)) =
            self.allocate_task(task_cloned)
        {
            if task_id == worker_task_queue.highest_prio_task_id().unwrap() {
                self.workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .send_task(task)
                    .await;

                worker_task_queue.highest_prio_task().unwrap().start();

                if let Some(second_highest_prio_task) =
                    worker_task_queue.second_highest_prio_task()
                {
                    // A task was running that was preempted
                    second_highest_prio_task.preempt();
                    log_metric(
                        ID,
                        LogMetricType::SchedulerTaskPreempted,
                        second_highest_prio_task.id(),
                        &HashMap::from([("new_task", task_id)]),
                    );
                }
            }
            let expected_slack_times =
                worker_task_queue.expected_slack_times();
            debug!(
                "Added task on worker {:?}, expected slack times: {:#?}",
                worker_id, expected_slack_times
            );
            worker_task_queue.save_expected_slack_times();

            let worker_task_queue_len = worker_task_queue.len();
            self.worker_schedule.insert(worker_id, worker_task_queue);
            Ok((worker_id, worker_task_queue_len))
        } else {
            debug!("No worker available for task {}", task_id);
            Err(String::from("No worker available"))
        }
    }
    async fn finish_task(
        &mut self,
        finished_task: FinishedTask,
        worker_id: Uuid,
    ) {
        let worker_scheduler_delay = SystemTime::now()
            .duration_since(finished_task.timestamp)
            .unwrap();
        debug!(
            "TaskFinished message for task {:?} \
            took {:?} to arrive at scheduler",
            finished_task.task_id, worker_scheduler_delay
        );
        let task_queue = self.worker_schedule.get_mut(&worker_id).unwrap();
        task_queue.remove(finished_task.task_id);

        let expected_slack_times = task_queue.expected_slack_times();
        debug!(
            "Finished task {} on worker {:?}, \
            expected_exec_time: {:?}, real_exec_time: {:?}, \
            expected slack times: {:#?}",
            finished_task.task_id,
            worker_id,
            finished_task.expected_exec_time,
            finished_task.real_exec_time,
            expected_slack_times
        );
        if finished_task.real_exec_time > finished_task.expected_exec_time {
            let exec_time_diff = finished_task.real_exec_time
                - finished_task.expected_exec_time;
            debug!(
                "Task {:?} took {:?} longer than expected",
                finished_task.task_id, exec_time_diff,
            );
            task_queue.sub_diff_exec_time(exec_time_diff);
        }
        // task_queue.correct_slack_times();

        let expected_slack_times = task_queue.expected_slack_times();
        debug!(
            "Expected slack times on worker {:?} after corrections: {:#?}",
            worker_id, expected_slack_times
        );

        if let Some(next_task) = task_queue.highest_prio_task() {
            if next_task.prev_exec_time() > Duration::ZERO {
                // Task ran before and was preempted,
                // we don't need to send it to the worker
                debug!(
                    "Task {} already sent to worker {}",
                    next_task.id(),
                    worker_id
                );
                next_task.resume()
            } else {
                // Send new task to worker
                self.workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .send_task(next_task.clone())
                    .await;
                next_task.start();
            }
        }
    }
    fn get_overdue_tasks(&mut self) -> Vec<(Uuid, Task)> {
        let mut overdue_tasks = Vec::new();
        for (worker_id, task_queue) in self.worker_schedule.iter_mut() {
            if let Some(task) = task_queue.highest_prio_task() {
                if task.current_exec_time() >= task.actual_exec_time() * 2 {
                    overdue_tasks.push((worker_id.clone(), task.clone()));
                }
            }
        }
        overdue_tasks
    }
}

impl PartitionedSchedule {
    pub fn new(allocation: AllocationHeuristic) -> Self {
        Self {
            workers: HashMap::new(),
            worker_schedule: HashMap::new(),
            allocation,
        }
    }
    fn allocate_task(&self, task: Task) -> Option<(Uuid, TaskQueue)> {
        let task_id = task.id();
        let start_time = SystemTime::now();
        let res = match self.allocation {
            AllocationHeuristic::FF => self.allocate_task_ff(task),
            AllocationHeuristic::BF => self.allocate_task_bf(task),
            AllocationHeuristic::WF => self.allocate_task_wf(task),
        };
        log_metric(
            ID,
            LogMetricType::SchedulerTaskFinishedScheduling,
            task_id,
            &HashMap::from([(
                "runtime_nanos",
                SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_nanos(),
            )]),
        );
        res
    }
    fn allocate_task_ff(&self, task: Task) -> Option<(Uuid, TaskQueue)> {
        for (worker_id, task_queue) in self.worker_schedule.iter() {
            let task_cloned = task.clone();
            let mut task_queue = task_queue.clone();

            if task_queue.is_empty() {
                debug!("Found idle worker: {}", worker_id);
                task_queue.add_task(task_cloned);
                return Some((worker_id.clone(), task_queue));
            }
            task_queue.add_task(task_cloned);
            task_queue.schedule(task.id());
            let (deadline_miss, total_density) = task_queue.total_density();
            log_metric(
                ID,
                LogMetricType::SchedulerWorkerDensity,
                task.id(),
                &HashMap::from([
                    ("worker_id", worker_id.to_string()),
                    ("density", total_density.to_string()),
                ]),
            );
            if let DeadlineMiss::No = deadline_miss {
                return Some((worker_id.clone(), task_queue));
            }
        }
        None
    }
    fn allocate_task_wf(&self, task: Task) -> Option<(Uuid, TaskQueue)> {
        let mut min_density = f64::MAX;
        let mut return_tuple = None;

        for (worker_id, task_queue) in self.worker_schedule.iter() {
            let task_cloned = task.clone();
            let mut task_queue = task_queue.clone();
            task_queue.add_task(task_cloned);
            task_queue.schedule(task.id());
            let (deadline_miss, total_density) = task_queue.total_density();
            log_metric(
                ID,
                LogMetricType::SchedulerWorkerDensity,
                task.id(),
                &HashMap::from([
                    ("worker_id", worker_id.to_string()),
                    ("density", total_density.to_string()),
                ]),
            );
            if matches!(deadline_miss, DeadlineMiss::No)
                && total_density < min_density
            {
                min_density = total_density;
                return_tuple = Some((worker_id.clone(), task_queue));
            }
        }
        return_tuple
    }
    fn allocate_task_bf(&self, task: Task) -> Option<(Uuid, TaskQueue)> {
        let mut max_density = 0.0;
        let mut return_tuple = None;

        for (worker_id, task_queue) in self.worker_schedule.iter() {
            let task_cloned = task.clone();
            let mut task_queue = task_queue.clone();
            task_queue.add_task(task_cloned);
            task_queue.schedule(task.id());
            let (deadline_miss, total_density) = task_queue.total_density();
            log_metric(
                ID,
                LogMetricType::SchedulerWorkerDensity,
                task.id(),
                &HashMap::from([
                    ("worker_id", worker_id.to_string()),
                    ("density", total_density.to_string()),
                ]),
            );
            if matches!(deadline_miss, DeadlineMiss::No)
                && total_density > max_density
            {
                max_density = total_density;
                return_tuple = Some((worker_id.clone(), task_queue));
            }
        }
        return_tuple
    }
}
