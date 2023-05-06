#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <set>

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

struct TaskGroup {
    int groupId;
    IRunnable *runnable;
    int numTotalTasks;
    int taskRemained;
    std::set<TaskID> depending;

    TaskGroup(int groupId, IRunnable *runnable, int numTotalTasks, const std::vector<TaskID> &deps) {
        this->groupId = groupId;
        this->runnable = runnable;
        this->numTotalTasks = numTotalTasks;
        this->taskRemained = numTotalTasks;
        this->depending = {};
        for (auto dep: deps) { this->depending.insert(dep); }
    }

};
// Comparison function for priority_queue
struct CompareTaskGroup {
    bool operator()(const TaskGroup* taskGroup1, const TaskGroup* taskGroup2) const {
        return taskGroup1->depending.size() > taskGroup2->depending.size();
    }
};


struct RunnableTask {
    TaskGroup *belongTo;
    int in_group_taskid;

    RunnableTask(TaskGroup *belongTo, int in_group_taskid) {
        this->belongTo = belongTo;
        this->in_group_taskid = in_group_taskid;
    }
};

class MinHeap {
public:
    std::vector<TaskGroup*> heap;

    void heapifyUp(int index) {
        int parent = (index - 1) / 2;
        while (index > 0 && compare(heap[index], heap[parent])) {
            std::swap(heap[index], heap[parent]);
            index = parent;
            parent = (index - 1) / 2;
        }
    }

    void heapifyDown(int index) {
        int size = heap.size();
        int leftChild = 2 * index + 1;
        int rightChild = 2 * index + 2;
        int smallest = index;

        if (leftChild < size && compare(heap[leftChild], heap[smallest])) {
            smallest = leftChild;
        }

        if (rightChild < size && compare(heap[rightChild], heap[smallest])) {
            smallest = rightChild;
        }

        if (smallest != index) {
            std::swap(heap[index], heap[smallest]);
            heapifyDown(smallest);
        }
    }

    bool compare(TaskGroup* a, TaskGroup* b) {
        return a->depending.size() < b->depending.size();
    }

public:
    void push(TaskGroup* taskGroup) {
        heap.push_back(taskGroup);
        heapifyUp(heap.size() - 1);
    }

    void pop() {
        if (heap.empty()) {
            return;
        }
        std::swap(heap[0], heap[heap.size() - 1]);
        heap.pop_back();
        heapifyDown(0);
    }

    TaskGroup* top() const {
        return heap.empty() ? nullptr : heap[0];
    }

    void adjustHeap() {
        for (int i = heap.size() / 2 - 1; i >= 0; --i) {
            heapifyDown(i);
        }
    }

    bool empty() const {
        return heap.empty();
    }
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {

    public:
        std::vector<std::thread> threads;
        std::mutex count_mutex_;
        std::condition_variable count_cond_;

        std::mutex task_queue_mutex_;
        std::condition_variable task_queue_cond_;

        std::condition_variable heap_cond_;

        std::queue<RunnableTask *> taskQueue;
        std::set<TaskGroup *> taskGroupSet;
        MinHeap taskGroupQueue;
        
        bool exitFlag;
        int numGroup;
        bool finishFlag{};

        void func();

    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
