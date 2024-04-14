//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//


#include <thread>
#include <future>

#include "gc.hpp"

namespace gc {
        

    template<typename T>
    struct TrieberStack : Object {
        
        struct Node : Object {
            
            AtomicStrong<Node> next;
            T value;
            
            virtual ~Node() override = default;

            virtual void scan(ScanContext& context) const override {
                context.push(this->next);
            }
                        
        }; // struct Node
        
        AtomicStrong<Node> head;
        
        virtual void scan(ScanContext& context) const override {
            context.push(this->head);
        }
        
        void push(T value) {
            Node* desired = new Node;
            desired->value = std::move(value);
            Node* expected = head.load(ACQUIRE);
            do {
                desired->next.ptr.store(expected, RELAXED);
            } while (!head.compare_exchange_strong(expected,
                                                   desired,
                                                   RELEASE,
                                                   ACQUIRE));            
        }
        
        bool pop(T& value) {
            Node* expected = head.load(ACQUIRE);
            for (;;) {
                if (expected == nullptr)
                    return false;
                Node* desired = expected->next.load(RELAXED);
                if (head.compare_exchange_strong(expected,
                                                 desired,
                                                 RELAXED,
                                                 ACQUIRE)) {
                    value = std::move(expected->value);
                    return true;
                }
            }
        }
                
    };

    std::mutex global_integers_mutex;
    std::vector<int> global_integers;
    
    constexpr std::size_t THREADS = 3;

    const char* mutatorNames[THREADS] = {
        "M0", "M1", "M2",
    };
    
    void mutate(TrieberStack<int>* trieber_stack,
                const std::size_t index,
                std::promise<std::vector<int>> result) {
        
        pthread_setname_np(mutatorNames[index]);
        
        push(trieber_stack);
                        
        size_t allocated = 0;
                
        int k = (int) index;
        std::vector<int> integers;
    
        gc::enter();
                               
        for (;;) {

            gc::handshake();
                                    
            // do some graph work
            
            bool nonempty = true;
            for (int i = 0; i != 1000; ++i) {
                
                if ((k >= 10000000) || !(rand() % 2)) {
                    int j = -1;
                    if((nonempty = trieber_stack->pop(j))) {
                        integers.push_back(j);
                    }
                } else {
                    trieber_stack->push(k);
                    k += THREADS;
                    nonempty = true;
                }
                
            }
            
            // std::this_thread::sleep_for(std::chrono::milliseconds{1});
                        
            if ((k >= 10000000) && !nonempty) {
                LOG("%p no more work to do", local.channel);
                LOG("lifetime alloc %zu", allocated);
                result.set_value(std::move(integers));
                return;
            }
            
        }
    }
    

    
    
    
    
   
    
    
    
    
    void exercise() {
        
        gc::enter();

        LOG("create a concurrent stack");
        auto p = new TrieberStack<int>;
        gc::push(p);
        
        std::stack<std::future<std::vector<int>>> futures;
        std::stack<std::thread> mutators;
        for (std::size_t i = 0; i != THREADS; ++i) {
            LOG("spawn a mutator thread");
            std::promise<std::vector<int>> promise;
            futures.push(promise.get_future());
            mutators.emplace(mutate, p, i, std::move(promise));
        }
        
        gc::leave();

        LOG("spawn the collector thread");
        std::thread collector{collect};
        
        std::vector<int> integers;
        while (!futures.empty()) {
            std::vector<int> result = futures.top().get();
            LOG("received %zu integers", result.size());
            futures.pop();
            integers.insert(integers.end(), result.begin(), result.end());
        }
        
        LOG("sorting %zu results", integers.size());
        std::sort(integers.begin(), integers.end());
        
        for (int i = 0;; ++i) {
            if (i == integers.size()) {
                LOG("all integers popped exactly once");
                break;
            }
            if (i != integers[i]) {
                LOG("first error at [%d] != %d", i, integers[i]);
                break;
            }
        }

        while (!mutators.empty()) {
            mutators.top().join();
            LOG("joined a mutator thread");
            mutators.pop();
        }
        
        collector.join();
        LOG("joined the collector thread");
    }
    
}

int main(int argc, const char * argv[]) {
    pthread_setname_np("MAIN");
    srand(79);
    gc::exercise();
}



