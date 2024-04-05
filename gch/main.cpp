//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//

#include <atomic>
#include <cassert>
#include <deque>
#include <iostream>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include <set>
#include <map>


namespace gch3 {
    
    constexpr int SLOTS = 7;
    
    struct Object;
    
    std::mutex mutex;
    std::condition_variable condition_variable;
        
    std::deque<Object*> worklist;
    bool worklist_request_roots = false;
    std::vector<Object*> infants;
    
    enum Color : intptr_t {
        WHITE,
        GRAY,
        BLACK,
    };
    
    constexpr const char* ColorNames[3] = {
        [WHITE] = "WHITE",
        [GRAY] = "GRAY",
        [BLACK] = "BLACK",
    };
    
    struct Object {
        std::atomic<Color> color;
        std::atomic<Object*> slots[SLOTS];
    };
    
    Object* Read(Object* src, int i) {
        assert(src);
        assert(i < SLOTS);
        // The mutator thread is already ordered wrt the fields
        return src->slots[i].load(std::memory_order_relaxed);
    }
        
    
    void Write(Object* src, int i, Object* ref) {
        src->slots[i].store(ref, std::memory_order_release);
        
        // Steele
        // if (isBlack(src) && isWhite(ref)) revert(src)
        /*
        if (ref && ref->color.load(std::memory_order_relaxed) == WHITE) {
            std::unique_lock lock(mutex);
            Color expected = BLACK;
            Color desired = GRAY;
            if (src->color.compare_exchange_strong(expected, desired, std::memory_order_acquire, std::memory_order_relaxed)) {
                worklist.push_back(src);
            }
        }
         */

        // Boehm
        // if (isBlack(src)) revert(src)
        /*
        std::unique_lock lock(mutex);
        Color expected = BLACK;
        Color desired = GRAY;
        if (src->color.compare_exchange_strong(expected, desired, std::memory_order_acquire, std::memory_order_relaxed)) {
            worklist.push_back(src);
        }
         */

         // Djikstra
        // if (isBlack(src)) shade(ref)
        if (ref && src->color.load(std::memory_order_relaxed) == BLACK) {
            std::unique_lock lock(mutex);
            Color expected = WHITE;
            Color desired = GRAY;
            if (ref->color.compare_exchange_strong(expected, desired, std::memory_order_relaxed, std::memory_order_relaxed)) {
                worklist.push_back(ref);
            }
        }
        

    }
    
    void collect() {
        
        size_t freed = 0;
        
        std::vector<Object*> objects;
        for (;;) {
            // step 1, get roots snapshot
            {
                std::unique_lock lock(mutex);
                worklist_request_roots = true;
                while (worklist_request_roots) {
                    condition_variable.wait(lock);
                }
            }
            // step 2, process worklist
            
            for  (;;) {
                Object* object = nullptr;
                {
                    std::unique_lock lock(mutex);
                    if (!worklist.empty()) {
                        object = worklist.front();
                        worklist.pop_front();
                        Color color = object->color.exchange(BLACK, std::memory_order_release);
                        assert(color == GRAY);
                    }
                }
                if (!object)
                    break;
                for (std::atomic<Object*>& field : object->slots) {
                    Object* ref = field.load(std::memory_order_acquire);
                    if (ref) {
                        std::unique_lock lock(mutex);
                        Color expected = WHITE;
                        Color desired = GRAY;
                        if (ref->color.compare_exchange_strong(expected, desired, std::memory_order_release, std::memory_order_relaxed))
                            worklist.push_back(ref);
                    }
                }

                                
            }
            
            // worklist is now empty
            // everything reachable from roots is black
            // everything newly allocated is black
            // the mutator has no whites to insert into black
            
            // does it make sense to ask for more roots here?
            // everything the mutator could have installed in its roots was
            // reachable from the original roots, or allocated since it exported
            // the roots
            
            // if this is true, the roots should all be black at this point
            
            // multiple threads may change this argument?
            
            // step 3, sweep
            {
                // grab everything that was allocated recently
                std::vector<Object*> adoptees;
                {
                    std::unique_lock lock(mutex);
                    adoptees = std::move(infants);
                    infants.clear();
                }
                objects.insert(objects.end(), adoptees.begin(), adoptees.end());

                
                // the mutator will continue adding new objects to nodes, but
                // we won't sweep them (they are black anyway)
                
                // At this point everything in objects should be black or white
                // with no gray
                
                // The mutator continues to allocate new black objects and puts
                // them in nodes
                
                // The mutator has no access to white objects, so it never turns
                // any black object gray
                
                // We could check this no gray invariant here
                
                
                // sweep
                //
                // free all white objects and erase them from the object list
                //
                // turn all black objects white
                //
                // as soon as we turn some black objects white, the mutator
                // might write them over a still-black object, making it gray
                // and putting it in the work-queue.
                //
                // this is fine, we leave gray objects alone -- they used to
                // be black and would have survived, and since the mutator
                // can clearly find them, they would have been turned gray
                // eventually, so this is a bonus
                
                auto first = objects.begin();
                auto middle = first;
                auto last = objects.end();
                for (; middle != last; ++middle) {
                    Object* ref = *middle;
                    Color expected = BLACK;
                    Color desired = WHITE;
                    ref->color.compare_exchange_weak(expected, desired, std::memory_order_relaxed, std::memory_order_acquire);
                    switch (expected) {
                        case WHITE:
                            delete ref;
                            ++freed;
                            break;
                        case GRAY:
                            // the mutator can be turning new allocations gray
                            [[fallthrough]];
                        case BLACK:
                            *first = ref;
                            ++first;
                            break;
                    }
                }
                size_t n = objects.size();
                objects.erase(first, last);
                size_t m = objects.size();
                printf("     freed %zu objects %zu -> %zu (%zu)\n", freed, n, m, n-m);
                
            }
            
            
            
        }
    }
    
    std::vector<Object*> nodesFromRoot(Object* root) {
        std::set<Object*> set;
        std::vector<Object*> vector;
        vector.push_back(root);
        set.emplace(root);
        for (int i = 0; i != vector.size(); ++i) {
            Object* src = vector[i];
            for (int j = 0; j != SLOTS; ++j) {
                Object* ref = src->slots[j].load(std::memory_order_relaxed);
                if (ref) {
                    auto [_, flag] = set.insert(ref);
                    if (flag) {
                        vector.push_back(ref);
                    }
                }
            }
        }
        return vector;
    }
    
    std::pair<std::set<Object*>, std::set<std::pair<Object*, Object*>>> nodesEdgesFromRoot(Object* root) {
        std::set<Object*> nodes;
        std::set<std::pair<Object*, Object*>> edges;
        std::stack<Object*> stack;
        stack.push(root);
        nodes.emplace(root);
        while (!stack.empty()) {
            Object* src = stack.top();
            stack.pop();
            for (int i = 0; i != SLOTS; ++i) {
                Object* ref = src->slots[i].load(std::memory_order_relaxed);
                if (ref) {
                    auto [_, flag] = nodes.insert(ref);
                    if (flag) {
                        stack.push(ref);
                    }
                    edges.emplace(src, ref);
                }
            }
        }
        return std::pair(std::move(nodes), std::move(edges));
    }
    
    void printObjectGraph(Object* root) {
        
        auto [nodes, edges] = nodesEdgesFromRoot(root);
        
        std::map<Object*, int> labels;
        int count = 0;
        for (Object* object : nodes) {
            labels.emplace(object, count++);
        }

        printf("%zu nodes\n", nodes.size());
        printf("%zu edges\n", edges.size());
        for (auto [a, b] : edges) {
            printf("  (%d) -> (%d)\n", labels[a], labels[b]);
        }
    }
    
    
    void mutate() {
        
        size_t allocated = 0;
        
        auto allocate = []() -> Object* {
            Object* infant = new Object();
            infant->color.store(BLACK, std::memory_order_relaxed);
            {
                std::unique_lock lock(mutex);
                infants.push_back(infant);
            }
            return infant;
        };
        
        std::vector<Object*> roots;
        std::vector<Object*> nodes;
        
        roots.push_back(allocate());
        nodes = nodesFromRoot(roots.back());
        
        for (;;) {
            {
                std::unique_lock lock(mutex);
                if (worklist_request_roots) {
                    assert(roots.size() <= 1);
                    for (Object* ref : roots) {
                        Color expected = WHITE;
                        Color desired = GRAY;
                        if (ref->color.compare_exchange_strong(expected, desired, std::memory_order_release, std::memory_order_relaxed)) {
                            worklist.push_back(ref);
                        }
                    }
                    worklist_request_roots = false;
                    condition_variable.notify_all();
                }
            }
            
            printf("allocated %zd\n", allocated);
            // do some graph work
            
            for (int i = 0; i != 100; ++i) {
                
                assert(!nodes.empty()); // we should at least have the root
                
                // choose two nodes and a slot
                int j = rand() % nodes.size();
                int k = rand() % SLOTS;
                int l = rand() % nodes.size();
                Object* p = nullptr;
                
                if (nodes.size() < rand() % 100) {
                    // add a new node
                    p = allocate();
                    ++allocated;
                } else if (nodes.size() < rand() % 100) {
                    // add a new edge betweene existing nodes
                    p = nodes[l];
                }
                Write(nodes[j], k, p);
                // Whatever we wrote may have overwritten an existing edge
                
                nodes = nodesFromRoot(roots.back());
                
                // printObjectGraph(roots.back());
                
            }
            
            
        }
    }
    

    
    
    
    
   
    
    
    
    
    void exercise() {
        std::thread collector(collect);
        std::thread mutator(mutate);
        mutator.join();
        collector.join();
    }
    
}

int main(int argc, const char * argv[]) {
    srand(79);
    gch3::exercise();
}



