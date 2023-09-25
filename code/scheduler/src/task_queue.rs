use log::{debug, warn};
use std::cmp::Ordering;
use std::time::{Duration, SystemTime};
use utils::Task;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum SchedulingPolicy {
    EDF, // Earliest Deadline First
}

#[derive(Debug, Clone)]
pub enum DeadlineMiss {
    Yes,
    No,
}

#[derive(Debug, Clone)]
pub struct TaskQueue {
    scheduling_policy: SchedulingPolicy,
    queue: Vec<Task>,
}

impl TaskQueue {
    pub fn new(scheduling_policy: SchedulingPolicy) -> Self {
        Self {
            scheduling_policy,
            queue: Vec::new(),
        }
    }
    pub fn add_task(&mut self, task: Task) {
        self.queue.push(task);
    }
    pub fn is_empty(&self) -> bool {
        self.queue.len() == 0
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn highest_prio_task(&mut self) -> Option<&mut Task> {
        self.queue.last_mut()
    }
    pub fn second_highest_prio_task(&mut self) -> Option<&mut Task> {
        if self.queue.len() > 1 {
            let idx = self.queue.len() - 2;
            Some(&mut self.queue[idx])
        } else {
            None
        }
    }
    pub fn highest_prio_task_id(&self) -> Option<Uuid> {
        if let Some(task) = self.queue.last() {
            Some(task.id())
        } else {
            None
        }
    }
    pub fn _second_highest_prio_task_id(&self) -> Option<Uuid> {
        if self.queue.len() > 1 {
            let idx = self.queue.len() - 2;
            Some(self.queue[idx].id())
        } else {
            None
        }
    }
    pub fn lowest_prio_task_id(&self) -> Option<Uuid> {
        if let Some(task) = self.queue.first() {
            Some(task.id())
        } else {
            None
        }
    }
    pub fn pop(&mut self) -> Option<Task> {
        self.queue.pop()
    }
    pub fn _pop_second(&mut self) -> Option<Task> {
        if self.queue.len() > 1 {
            let idx = self.queue.len() - 2;
            Some(self.queue.remove(idx))
        } else {
            None
        }
    }
    pub fn remove(&mut self, task_id: Uuid) {
        if let Some(idx) = self.queue.iter().position(|t| t.id() == task_id) {
            self.queue.remove(idx);
        } else {
            warn!("Task {} not found in queue, was probably overdue", task_id);
        }
    }
    pub fn _total_slack_time(&self) -> (DeadlineMiss, Duration) {
        if self.queue.len() == 0 {
            return (DeadlineMiss::No, Duration::MAX);
        }
        let mut deadline_miss = DeadlineMiss::No;
        let mut queue = self.queue.clone();
        let running_task = queue.pop().unwrap();
        let current_time = SystemTime::now();
        let mut total_slack_time = running_task.slack_time(current_time);

        let mut next_task_start_time =
            current_time + running_task.remaining_exec_time();

        queue.reverse();
        for task in queue.iter() {
            let slack_time = task.slack_time(next_task_start_time);
            if slack_time == Duration::ZERO {
                deadline_miss = DeadlineMiss::Yes;
            } else {
                total_slack_time += slack_time;
            }
            next_task_start_time =
                next_task_start_time + task.remaining_exec_time();
        }
        (deadline_miss, total_slack_time)
    }
    pub fn _avg_slack_time(&self) -> (DeadlineMiss, Duration) {
        let (deadline_miss, total_slack_time) = self._total_slack_time();
        let avg_slack_time = total_slack_time.as_nanos()
            / u128::try_from(self.queue.len()).unwrap();
        (
            deadline_miss,
            Duration::from_nanos(u64::try_from(avg_slack_time).unwrap()),
        )
    }
    pub fn total_density(&self) -> (DeadlineMiss, f64) {
        if self.queue.len() == 0 {
            return (DeadlineMiss::No, 0.0);
        }
        let mut deadline_miss = DeadlineMiss::No;
        let mut queue = self.queue.clone();
        let running_task = queue.pop().unwrap();
        let current_time = SystemTime::now();
        let mut total_density = running_task.density(current_time);

        let mut next_task_start_time =
            current_time + running_task.remaining_exec_time();

        queue.reverse();
        for task in queue.iter() {
            let slack_time = task.slack_time(next_task_start_time);
            if slack_time < Duration::from_millis(1) {
                deadline_miss = DeadlineMiss::Yes;
            }
            total_density += task.density(next_task_start_time);
            next_task_start_time =
                next_task_start_time + task.remaining_exec_time();
        }
        (deadline_miss, total_density)
    }
    pub fn expected_slack_times(
        &self,
    ) -> Vec<(Uuid, Duration, Duration, Duration)> {
        let mut expected_slack_times: Vec<(
            Uuid,
            Duration,
            Duration,
            Duration,
        )> = Vec::new();
        let mut queue = self.queue.clone();
        if queue.len() < 1 {
            return expected_slack_times;
        }
        let running_task = queue.pop().unwrap();
        let current_time = SystemTime::now();

        let remaining_exec_time = running_task.remaining_exec_time();
        let mut next_task_start_time = current_time + remaining_exec_time;
        let slack_time;
        if running_task.time_to_deadline() > remaining_exec_time {
            slack_time = running_task.time_to_deadline() - remaining_exec_time;
        } else {
            slack_time = Duration::ZERO;
        }
        expected_slack_times.push((
            running_task.id(),
            remaining_exec_time,
            running_task.time_to_deadline(),
            slack_time,
        ));

        queue.reverse();
        for task in queue.iter() {
            let slack_time = task.slack_time(next_task_start_time);
            let remaining_exec_time = task.remaining_exec_time();
            next_task_start_time = next_task_start_time + remaining_exec_time;
            expected_slack_times.push((
                task.id(),
                remaining_exec_time,
                task.time_to_deadline(),
                slack_time,
            ));
        }
        expected_slack_times
    }
    pub fn sub_diff_exec_time(&mut self, diff_exec_time: Duration) {
        let mut i = self.queue.len();
        while i > 0 {
            let task = &mut self.queue[i - 1];
            if let None = task.start_time() {
                if task.actual_exec_time() > diff_exec_time {
                    debug!(
                        "Subtracting {:?} task {:?}",
                        diff_exec_time,
                        task.id()
                    );
                    task.sub_exec_time(diff_exec_time);
                }
                break;
            }
            i -= 1;
        }
    }
    pub fn save_expected_slack_times(&mut self) {
        let mut queue = self.queue.clone();
        if queue.len() < 1 {
            return;
        }
        let running_task = queue.pop().unwrap();
        let current_time = SystemTime::now();

        let remaining_exec_time = running_task.remaining_exec_time();
        let mut next_task_start_time = current_time + remaining_exec_time;
        queue.reverse();
        for task in queue.iter() {
            let slack_time = task.slack_time(next_task_start_time);
            let remaining_exec_time = task.remaining_exec_time();
            next_task_start_time = next_task_start_time + remaining_exec_time;
            let real_task =
                self.queue.iter_mut().find(|t| t.id() == task.id()).unwrap();
            real_task.slack_time = Some(slack_time);
        }
    }
    pub fn _correct_slack_times(&mut self) {
        if self.queue.len() < 1 {
            return;
        }
        let next_task = self.queue.last_mut().unwrap();
        let current_time = SystemTime::now();

        if let Some(prev_slack_time) = next_task.slack_time {
            let slack_time = next_task.slack_time(current_time);
            if prev_slack_time > slack_time {
                let slack_time_diff = prev_slack_time - slack_time;
                debug!(
                    "Next Task {:?} had {:?} more slack time \
                    (prev slack time: {:?} now {:?})",
                    next_task.id(),
                    slack_time_diff,
                    prev_slack_time,
                    slack_time
                );
                next_task.sub_exec_time(slack_time_diff);
                next_task.slack_time =
                    Some(next_task.slack_time(current_time));
            }
        }
    }
    pub fn schedule(&mut self, task_id: Uuid) {
        match self.scheduling_policy {
            SchedulingPolicy::EDF => self.schedule_edf(),
        }
        let mut pos =
            self.queue.iter().position(|t| t.id() == task_id).unwrap();
        debug!(
            "New task {:?} has priority {}/{} in this queue",
            task_id,
            pos + 1,
            self.queue.len()
        );
        while pos != 0 {
            pos -= 1;
            if let Some(_) = self.queue[pos].start_time() {
                debug!(
                    "Task {:?} would preempt some lower priority task, \
                    so we add preemption overhead",
                    task_id
                );
                let task =
                    self.queue.iter_mut().find(|t| t.id() == task_id).unwrap();
                task.add_exec_time(Duration::from_millis(10));
                break;
            }
        }
    }
    fn schedule_edf(&mut self) {
        // Implements earliest deadline first (EDF)
        self.queue.sort_by(|t1, t2| {
            if t1.deadline() > t2.deadline() {
                Ordering::Less
            } else if t1.deadline() == t2.deadline() {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        });
    }
    fn _schedule_lst(&mut self) {
        // Least Slack Time first (LST)
        // Implemented as job static priority, which is not correct
        // for LST. The slack times of the waiting jobs change over time
        // and thus the priorities have to be adjusted. I don't want to
        // This is not suited to be implemented this in this kind of
        // event-based scheduler, it would need a scheduler that schedules
        // at certain intervals
        //
        // slack_time = D - t - e'
        // D: Deadline
        // t: current time
        // e': Remaining execution time
        let current_time = SystemTime::now();
        self.queue.sort_by(|t1, t2| {
            if t1.slack_time(current_time) > t2.slack_time(current_time) {
                Ordering::Less
            } else if t1.slack_time(current_time)
                == t2.slack_time(current_time)
            {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        });
    }
}
