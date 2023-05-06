#include "tasksys.h"
#include <iostream>
#include <algorithm>


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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    killed_ = false;
    num_threads_ = num_threads;
    current_taskid = 0;
    for(int i=0;i<num_threads;++i)
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread,this);

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    killed_ = true;
    task_queue_cond_.notify_all();
    for(auto &t:threads){
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    if(deps.size()==0)
        task_dep_map_[current_taskid];

    for(auto dep:deps){
        task_dep_map_[current_taskid].insert(dep);
    }
    task_group_map_[current_taskid] = new TaskGroup(current_taskid, runnable, num_total_tasks);
    return current_taskid++;
}

void TaskSystemParallelThreadPoolSleeping::scanForReadyTasks(){
    for(auto it = task_dep_map_.begin();it!=task_dep_map_.end();){
        auto tasks = it->second;
        if(tasks.empty()){
            TaskGroup* t = task_group_map_[it->first];
            task_remained_map_[t->id] = t->num_total_tasks;
            task_queue_mutex_.lock();
            for(int i=0;i<t->num_total_tasks;++i){
                task_queue_.push_back(new RunnableTask(t->id, i, t->runnable, t->num_total_tasks));
            }
            task_queue_mutex_.unlock();
            task_queue_cond_.notify_all();
            it = task_dep_map_.erase(it);
        }
        else{
            ++it;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::removeTaskIDFromDependency(TaskID finished_task){
    for(auto it = task_dep_map_.begin();it!=task_dep_map_.end();++it){
        it->second.erase(finished_task);
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    scanForReadyTasks();
    bool done=false;
    while(!done){
        std::unique_lock<std::mutex> finished_lock(finished_task_mutex_);
        finished_task_cond_.wait(finished_lock,[&]{return !finished_task_queue_.empty();});

        bool has_more_finished_task = true;
        while(has_more_finished_task){
            auto task_done_id = finished_task_queue_.front();
            finished_task_queue_.pop_front();
            task_remained_map_.erase(task_remained_map_.find(task_done_id));
            finished_lock.unlock();

            removeTaskIDFromDependency(task_done_id);

            finished_lock.lock();
            has_more_finished_task = !finished_task_queue_.empty();
        }

        finished_lock.unlock();

        scanForReadyTasks();

        finished_task_mutex_.lock();
        done = task_dep_map_.empty() && task_remained_map_.empty();
        finished_task_mutex_.unlock();

    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread(){
    while(!killed_){

        while(true){
            std::unique_lock<std::mutex> task_queue_lock(task_queue_mutex_);
            task_queue_cond_.wait(task_queue_lock, [&]{return killed_ || !task_queue_.empty();});

            if(task_queue_.empty()){
                task_queue_lock.unlock();
                break;
            }

            auto task = task_queue_.front();
            task_queue_.pop_front();
            task_queue_lock.unlock();

            task->runnable->runTask(task->current_task, task->num_total_tasks);

            finished_task_mutex_.lock();
            task_remained_map_[task->id]--;
            if(task_remained_map_[task->id]<=0){
                finished_task_queue_.push_back(task->id);
                finished_task_mutex_.unlock();

                finished_task_cond_.notify_all();
            }
            else{
                finished_task_mutex_.unlock();
            }
        }
    }

}