#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads) {
    numGroup = 0;
    exitFlag = false;
    for (int i = 0; i < num_threads; ++i) { threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::func, this); }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    exitFlag = true;
    task_queue_cond_.notify_all();
    for (auto &thread: threads) { thread.join(); }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {
    TaskGroup* newTaskGroup = new TaskGroup(numGroup, runnable, num_total_tasks, deps);
    
    taskGroupQueue.push(newTaskGroup);

    taskGroupSet.insert(newTaskGroup);
    return numGroup++;
}

void TaskSystemParallelThreadPoolSleeping::func() {
    RunnableTask *task;
    TaskGroup *taskBelongTo;
    while (true) {
        while (true) {
            std::unique_lock<std::mutex> lock(task_queue_mutex_);
            task_queue_cond_.wait(lock, [&] { return !taskQueue.empty()||exitFlag; });

            if (exitFlag) return;
            if (taskQueue.empty()) continue;
            task = taskQueue.front();
            taskQueue.pop();
            break;
        }
        taskBelongTo = task->belongTo;
        taskBelongTo->runnable->runTask(task->in_group_taskid, taskBelongTo->numTotalTasks);
        
        std::unique_lock<std::mutex> lock2(count_mutex_);
        taskBelongTo->taskRemained--;
        if (taskBelongTo->taskRemained <= 0) {
            for (auto element: taskGroupQueue.heap) { element->depending.erase(taskBelongTo->groupId); }

            taskGroupQueue.adjustHeap();
            lock2.unlock();

            heap_cond_.notify_one();

            count_cond_.notify_one();
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    TaskGroup *nowTaskGroup;
    RunnableTask *nowRunnableTask;
    while (!taskGroupQueue.empty()) {

        std::unique_lock<std::mutex> heap_lock(count_mutex_);
        heap_cond_.wait(heap_lock, [&] { return !taskGroupQueue.empty() || taskGroupQueue.top()->depending.empty(); });

        if(taskGroupQueue.empty())
            break;

        nowTaskGroup = taskGroupQueue.top();
        if (!nowTaskGroup->depending.empty()) {
            continue;
        }

        taskGroupQueue.pop();
        heap_lock.unlock();


        task_queue_mutex_.lock();
        for (int i = 0; i < nowTaskGroup->numTotalTasks; i++) {
            nowRunnableTask = new RunnableTask(nowTaskGroup, i);
            taskQueue.push(nowRunnableTask);
        }
        task_queue_mutex_.unlock();
        
        task_queue_cond_.notify_all();
        
    }
    while (true) {
        std::unique_lock<std::mutex> lock2(count_mutex_);
        count_cond_.wait(lock2, [&] { 
            finishFlag = true;
            for (auto taskGroup: taskGroupSet) {
                if (taskGroup->taskRemained > 0) {
                    finishFlag = false;
                    break;
                }
            }
            return finishFlag; 
        });

        if (finishFlag) return;
    }
}
