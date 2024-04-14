//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//


#include <thread>

#include "gc.hpp"

namespace gc {
        

    template<typename T>
    struct TrieberStack {
        
        struct Node final : Object {
            AtomicStrong<Node> next;
            T payload;
            
            explicit Node(auto&&... args)
            : payload(std::forward<decltype(args)>(args)...) {
            }

            virtual ~Node() override = default;

            virtual void scan(ScanContext& context) const override {
                context.push(this->next);
            }
                        
        };
        
        AtomicStrong<Node> head;
        
        void push(Node* desired) {
            Node* expected = head.load(ACQUIRE);
            do {
                desired->next.ptr.store(expected, RELAXED);
            } while (!head.compare_exchange_strong(expected,
                                                   desired,
                                                   RELEASE,
                                                   ACQUIRE));
            
        }
        
        bool pop(T& victim) {
            Node* expected = head.load(ACQUIRE);
            for (;;) {
                if (expected == nullptr)
                    return false;
                Node* desired = expected->next.load(RELAXED);
                if (head.compare_exchange_strong(expected,
                                                 desired,
                                                 RELAXED,
                                                 ACQUIRE)) {
                    victim = std::move(expected->payload);
                    return true;
                }
            }
        }
        
        void scan(std::stack<Object*>&) {
            //LOG("scans Stack<T>\n");
            Object::shade(head.load(std::memory_order_acquire));
        }
        
    };

    
    
    TrieberStack<int> global_trieber_stack;
    
    std::mutex global_integers_mutex;
    std::vector<int> global_integers;
    
    constexpr std::size_t THREADS = 3;

    const char* mutatorNames[THREADS] = {
        "M0", "M1", "M2",
    };
    
    void mutate(const std::size_t index) {
        
        pthread_setname_np(mutatorNames[index]);
                        
        size_t allocated = 0;
        
        std::vector<Object*> roots;
                
        int k = (int) index;
        std::vector<int> integers;
    
        gc::enter();
       
                        
        for (;;) {

            gc::handshake();
            
            // handshake completes

            // publish the roots between handshakes
            {
                std::stack<Object*> working;
                global_trieber_stack.scan(working);
            }
                        
            // do some graph work
            
            bool nonempty = true;
            for (int i = 0; i != 1000; ++i) {
                
                if ((k >= 10000000) || !(rand() % 2)) {
                    int j = -1;
                    if((nonempty = global_trieber_stack.pop(j))) {
                        integers.push_back(j);
                    }
                } else {
                    auto* desired = new  TrieberStack<int>::Node;
                    ++allocated;
                    desired->payload = k;
                    global_trieber_stack.push(desired);
                    k += THREADS;
                    nonempty = true;
                }
                
            }
            
            // std::this_thread::sleep_for(std::chrono::milliseconds{1});
                        
            if ((k >= 10000000) && !nonempty) {
                LOG("%p no more work to do\n", local.channel);
                LOG("lifetime alloc %zu\n", allocated);
                std::unique_lock lock(global_integers_mutex);
                global_integers.insert(global_integers.end(), integers.begin(), integers.end());
                gc::leave();
                return;
            }
            
        }
    }
    

    
    
    
    
   
    
    
    
    
    void exercise() {
        std::thread collector(collect);
        std::vector<std::thread> mutators;
        for (std::size_t i = 0; i != THREADS; ++i) {
            mutators.emplace_back(mutate, i);
        }
        while (!mutators.empty()) {
            mutators.back().join();
            LOG("A mutator thread has terminated\n");
            mutators.pop_back();
        }
        LOG("All mutators terminated\n");
        LOG("global_integers.size() = %zu\n", global_integers.size());
        std::sort(global_integers.begin(), global_integers.end());
        //for (int i = 0; i != 100; ++i) {
            //printf("[%d] == %d\n", i, global_integers[i]);
        //}
        bool perfect = true;
        for (int i = 0; i != global_integers.size(); ++i) {
            if (global_integers[i] != i) {
                LOG("The first error occurs at [%d] != %d\n", i, global_integers[i]);
                perfect = false;
                break;
            }
        }
        if (perfect) {
            LOG("All integers up to %zd were popped once.\n", global_integers.size());
        }

        
        collector.join();
        LOG("Collector thread has terminated\n");
    }
    
}

int main(int argc, const char * argv[]) {
    pthread_setname_np("MAIN");
    srand(79);
    gc::exercise();
}



