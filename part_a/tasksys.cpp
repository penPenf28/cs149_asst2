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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->threads_pool = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete [] threads_pool;
}

void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* m, int* curr_task){
    int curr_run_task = -1;
    while(curr_run_task < num_total_tasks){
        m->lock();
        curr_run_task = *curr_task;
        *curr_task = *curr_task + 1;
        m->unlock();
        if(curr_run_task >= num_total_tasks){
            break;
        }
        runnable->runTask(curr_run_task, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    std::mutex* m = new std::mutex;
    int* curr_task = new int;
    *curr_task = 0;

    for(int i = 0; i < num_threads; i++){
        threads_pool[i]= std::thread(&TaskSystemParallelSpawn::threadRun, this, runnable, num_total_tasks, m, curr_task);
    }
    for(int i = 0; i < num_threads; i++){
        threads_pool[i].join();
    }
    delete m;
    delete curr_task;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    killed_ = false;
    my_runnable_ = nullptr;
    num_total_tasks_ = 0;

    for(int i=0;i<num_threads;++i)
        threads_pool_.emplace_back(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    killed_ = true;
    for(auto& t: threads_pool_)
        t.join();
}

void TaskSystemParallelThreadPoolSpinning::spinningThread(){
    int task_id;
    while(!killed_){
        task_id = -1;
        mutex_.lock();
        if(!task_queue.empty()){
            task_id = task_queue.front();
            task_queue.pop();
        }
        mutex_.unlock();
        
        if(task_id != -1){
            my_runnable_->runTask(task_id, num_total_tasks_);
            task_remained_--;
        }


    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    task_remained_ = num_total_tasks;
    my_runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;

    for(int i=0;i<num_total_tasks;++i){
        mutex_.lock();
        task_queue.push(i);
        mutex_.unlock();
    }
    while(task_remained_);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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
    my_runnable_ = nullptr;
    num_total_tasks_ = 0;
    task_remained_ = -1;

    for(int i=0;i<num_threads;++i)
        threads_pool_.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    killed_ = true;
    my_runnable_ = nullptr;
    queue_cond_.notify_all();

    for(auto& t: threads_pool_)
        t.join();
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread(){

    int id;
    while(true){
        while(true){
            std::unique_lock<std::mutex> lock2(queue_mutex_);
            queue_cond_.wait(lock2,[&]{return !task_queue.empty()||killed_;;});

            if(killed_)
                return;
            if(task_queue.empty())
                continue;
            
            id = task_queue.front();
            task_queue.pop();
            break;
        }


        my_runnable_->runTask(id, num_total_tasks_);

        std::unique_lock<std::mutex> lock4(count_mutex_);
        task_remained_--;
        if(task_remained_ == 0){
            lock4.unlock();
            count_cond_.notify_one();
        }

            
    }
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

    my_runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    killed_ = false;
    task_remained_ = num_total_tasks;

    for(int i=0;i<num_total_tasks_;++i){
        std::unique_lock<std::mutex> lock2(queue_mutex_);
        task_queue.push(i);
    }
    queue_cond_.notify_all();

    while(true){
        std::unique_lock<std::mutex> lock3(count_mutex_);
        count_cond_.wait(lock3,[&](){return task_remained_ == 0;});
        if(!task_remained_) return;
    }

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
