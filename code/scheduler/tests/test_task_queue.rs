use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

use scheduler::task_queue::{DeadlineMiss, SchedulingPolicy, TaskQueue};
use utils::Task;

#[test]
fn test_total_slack_time() {
    let initial_time_to_deadline = Duration::from_millis(2000);
    let task1 = Task::new(
        1,
        Uuid::new_v4(),
        "127.0.0.1:1337".parse().unwrap(),
        SystemTime::now() + initial_time_to_deadline,
        initial_time_to_deadline,
        Duration::from_millis(1000),
        Duration::from_millis(1000),
        Duration::from_secs(0),
        HashMap::from([(
            String::from("return_data_bytes"),
            String::from("1"),
        )]),
    );

    let initial_time_to_deadline = Duration::from_millis(1000);
    let mut task2 = Task::new(
        1,
        Uuid::new_v4(),
        "127.0.0.1:1337".parse().unwrap(),
        SystemTime::now() + initial_time_to_deadline,
        initial_time_to_deadline,
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(0),
        HashMap::from([(
            String::from("return_data_bytes"),
            String::from("1"),
        )]),
    );

    let mut task_queue = TaskQueue::new(SchedulingPolicy::EDF);
    task2.start();
    task_queue.add_task(task1);
    task_queue.add_task(task2);
    let (deadline_miss, total_slack_time) = task_queue._total_slack_time();

    assert!(matches!(deadline_miss, DeadlineMiss::No));
    assert!(total_slack_time <= Duration::from_millis(1000));
    assert!(total_slack_time >= Duration::from_millis(999));
}

#[test]
fn test_total_density() {
    let initial_time_to_deadline = Duration::from_millis(2000);
    let task1 = Task::new(
        1,
        Uuid::new_v4(),
        "127.0.0.1:1337".parse().unwrap(),
        SystemTime::now() + initial_time_to_deadline,
        initial_time_to_deadline,
        Duration::from_millis(1400),
        Duration::from_millis(1400),
        Duration::from_secs(0),
        HashMap::from([(
            String::from("return_data_bytes"),
            String::from("1"),
        )]),
    );
    let initial_time_to_deadline = Duration::from_millis(1000);
    let mut task2 = Task::new(
        1,
        Uuid::new_v4(),
        "127.0.0.1:1337".parse().unwrap(),
        SystemTime::now() + initial_time_to_deadline,
        initial_time_to_deadline,
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(0),
        HashMap::from([(
            String::from("return_data_bytes"),
            String::from("1"),
        )]),
    );

    let mut task_queue = TaskQueue::new(SchedulingPolicy::EDF);
    task2.start();
    task_queue.add_task(task1);
    task_queue.add_task(task2);
    let (deadline_miss, total_density) = task_queue.total_density();

    println!("Total density: {}", total_density);
    assert!(matches!(deadline_miss, DeadlineMiss::No));
    assert!(total_density > 1.1 && total_density < 1.3);
}
