#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <map>
#include <thread>
#include <tuple>
#include <deque>
#include <iostream>
#include <mutex>
#include <set>
#include <condition_variable>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

class TaskGroup{
    public:
        TaskID id;
        IRunnable* runnable;
        int num_total_tasks;
        TaskGroup(TaskID id, IRunnable* runnable, int num_total_tasks){
            this->id = id;
            this->runnable = runnable;
            this->num_total_tasks = num_total_tasks;
        }
        TaskGroup(const TaskGroup &task){
            this->id = task.id;
            this->runnable = task.runnable;
            this->num_total_tasks = task.num_total_tasks;
        }
};

class RunnableTask{
    public:
        TaskID id;
        IRunnable* runnable;
        int current_task;
        int num_total_tasks;

        RunnableTask(TaskID id, int current_task, IRunnable* runnable,  int num_total_tasks){
            this->id = id;
            this->runnable = runnable;
            this->current_task = current_task;
            this->num_total_tasks = num_total_tasks;
        }

        RunnableTask(const RunnableTask &task){
            this->id = task.id;
            this->runnable = task.runnable;
            this->current_task = task.current_task;
            this->num_total_tasks = task.num_total_tasks;
        }

};




class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    public:
        bool killed_;
        int num_threads_;
        int current_taskid;

        std::vector<std::thread> threads;

        std::map<TaskID, std::set<TaskID>> task_dep_map_;
        std::map<TaskID, TaskGroup*> task_group_map_;
        std::map<TaskID, int> task_remained_map_;

        std::deque<RunnableTask*> task_queue_;
        std::deque<TaskID> finished_task_queue_;

        std::mutex finished_task_mutex_;
        std::condition_variable finished_task_cond_;

        std::mutex task_queue_mutex_;
        std::condition_variable task_queue_cond_;

        void worker_thread();
        void scanForReadyTasks();
        void removeTaskIDFromDependency(TaskID id);

};

#endif