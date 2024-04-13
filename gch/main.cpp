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
    struct Node : Object {
        AtomicStrong<Node> next;
        T payload;
        
        virtual ~Node() override = default;
        virtual void scan(std::stack<Object*>&) const override;

        explicit Node(auto&&... args)
        : payload(std::forward<decltype(args)>(args)...) {
        }
        
    };
    
    template<typename T>
    void Node<T>::scan(std::stack<Object*>& working) const {
        // LOG("scans %p %s\n", (Object*) this, local.color_names[_gc_color.load(std::memory_order_relaxed)]);
        
        // Implementation A: shade child GRAY and return
        // Object::shade(next.load(std::memory_order_acquire));
        
        // Implementation B:
        // Trace white chains immediately
        Color white = global.white.load(std::memory_order_relaxed);
        Color black = white ^ 2;
        const Node<T>* parent = this;
        for (;;) {
            Node<T>* child = parent->next.load(std::memory_order_acquire);
            if (!child)
                return;
            Color expected = white;
            // If WHITE, transform directly to BLACK and trace into that node
            if (child->color.compare_exchange_strong(expected,
                                                     black,
                                                     std::memory_order_relaxed,
                                                     std::memory_order_relaxed)) {
                local.dirty = true;
                parent = child;
            } else {
                // assert(expected == local.GRAY || expected == local.BLACK);
                return;
            }
        }
    }

    template<typename T>
    struct TrieberStack {
    
        AtomicStrong<Node<T>> head;
        
        void push(Node<T>* desired) {
            Node<T>* expected = head.load(std::memory_order_acquire);
            do {
                desired->next.store(expected, std::memory_order_release);
            } while (!head.compare_exchange_strong(expected,
                                                   desired,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire));
        }
        
        bool pop(T& victim) {
            Node<T>* expected = head.load(std::memory_order_acquire);
            for (;;) {
                if (expected == nullptr)
                    return false;
                Node<T>* desired = expected->next.load(std::memory_order_acquire);
                bool flag = head.compare_exchange_strong(expected,
                                                         desired,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_acquire);
                if (flag) {
                    victim = std::move(expected->payload);
                    
                    // TODO: we break the chain here but should fix the scanner
                    // instead to be better
                    
                    // expected->next.store(nullptr, RELEASE);
                    
                    
                    
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
        
    void mutate(const std::size_t index) {
                        
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
            
            for (int i = 0; i != 1000; ++i) {
                
                if (!(rand() % 2)) {
                    int j = -1;
                    if(global_trieber_stack.pop(j))
                        integers.push_back(j);
                } else {
                    Node<int>* desired = new Node<int>;
                    ++allocated;
                    desired->payload = k;
                    global_trieber_stack.push(desired);
                    k += THREADS;
                }
                
            }
            
            // std::this_thread::sleep_for(std::chrono::milliseconds{1});
                        
            LOG("lifetime alloc %zu\n", allocated);

            if (k > 10000000) {
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
            mutators.pop_back();
        }
        printf("global_integers.size() = %zu\n", global_integers.size());
        std::sort(global_integers.begin(), global_integers.end());
        //for (int i = 0; i != 100; ++i) {
            //printf("[%d] == %d\n", i, global_integers[i]);
        //}
        for (int i = 0; i != global_integers.size(); ++i) {
            if (global_integers[i] != i) {
                printf("The first missing integer is %d\n", i);
                break;
            }
        }

        
        collector.join();
    }
    
}

int main(int argc, const char * argv[]) {
    srand(79);
    gc::exercise();
}



