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
#include <concepts>


namespace gch3 {
        
    enum Mark 
    : intptr_t           // <-- machine word
    {
          MARKED = 0,    // <-- objects are allocated in MARKED state
        UNMARKED = 1,
    };
    
    constexpr const char* MarkNames[] = {
        [  MARKED] =   "MARKED",
        [UNMARKED] = "UNMARKED",
    };
    
    // Color / Mark / Worklist states
    //
    //     WHITE <=> UNMARKED
    //      GRAY <=> MARKED and in the WORKLIST
    //     BLACK <=> MARKED and not in the WORKLIST
    //
    // Object lifecycle:
    //
    //     / -> BLACK
    //            when the mutator allocates a new Object
    //
    // BLACK -> WHITE
    // WHITE -> /
    //            when the collector completes a cycle
    //
    // WHITE -> GRAY
    //            when the mutator marks its roots
    //            when the mutator overwrites a pointer to the Object
    //            when the collector traces a pointer to the Object
    //
    // GRAY  -> BLACK
    //            when the collector traces the Object's own fields
    //
    
    struct Object {
        
        // void* __vtbl;                         // <-- hidden pointer
                                                   
        mutable std::atomic<Mark> _gc_mark = MARKED; // <-- zero initialize
                                            
        virtual ~Object() = default;

        // MARK; WHITE -> GRAY
        [[nodiscard]] static std::size_t _gc_shade(Object*);

        // shade(fields)
        [[nodiscard]] virtual std::size_t _gc_scan() const = 0;
        
        // UNMARK; BLACK -> WHITE
        void _gc_clear() const;


    }; // struct Object

    std::size_t Object::_gc_shade(Object* object) {
        Mark expected = UNMARKED;
        return object && object->_gc_mark.compare_exchange_strong(expected,
                                                                  MARKED,
                                                                  std::memory_order_relaxed,
                                                                  std::memory_order_relaxed);
    }
    
    void Object::_gc_clear() const {
        _gc_mark.store(UNMARKED, std::memory_order_relaxed);
    }


    
    
    
    
    // Mutator-collector handshake channel
    //
    // The mutator must periodically check in for pending requests from the
    // mutator.
    //
    // - to mark its roots
    // - to report if it has produced any WHITE -> GRAY transitions since last asked
    // - to export its pending allocations
    //

    struct Channel {
        
        std::mutex mutex;
        std::condition_variable condition_variable;
        
        bool pending = false;
        
        bool dirty = false;
        std::vector<Object*> objects;
        
    };
    
    constexpr std::size_t THREADS = 3;

    Channel channels[THREADS] = {};
    
    
    void collect() {
        
        // TODO: inserting infants is expensive
        
        // what if we maintain a blacklist, whitelist, and worklist
        // pass through worklist moving entries to black or whitelist
        // swap whitelist to empty worklist for next iteration
        // finally, delete all white, whiten all black, blacklist becomes worklist
        //
        // at any time, we can append the infants to the blacklist
        
        // TODO: pointer chasing is expensive
        // What if we keep the lists sorted?  We will then access memory
        // in order, at least, and can predict runs?
        
        // TODO: long chains are pathological
        //
        // Suppose we have a linked list and lots of unreachable objects
        // When we sweep over the objects, we find the GRAY node in the list
        // and scan it to GRAY another node somewhere in the list.  We will
        // have to walk half the unreachable objects to find it, on average, and
        // thus we will do O(N) sweeps to collect a list of N nodes.
        //
        // But, the collector will make the great majority of GRAYs so we know
        // where they are.  We only need to sweep to find the ones made by the
        // mutators.  We don't want to blow the stack by recursing, so we can
        // get the scanning to make an explicit graystack
        
        size_t freed = 0;
        
        std::vector<Object*> objects;
        std::vector<Object*> infants;
        bool dirty;
        
        // The mutator may concurrently allocate BLACK objects at any time
        
        for (;;) {
            
            // "objects" contains only BLACK objects
            // Recently allocated objects are BLACK
            
            // All objects are BLACK

            // The collector sets these objects BLACK -> WHITE

            for (Object* object : objects)
                object->_gc_clear();
            
            // The mutator roots may now be WHITE
            dirty = true;

            // The mutator may now encounter WHITE objects
            // The mutator write barrier may set some WHITE objects GRAY
            // The mutator may now encounter GRAY objects
            
            // The WORKLIST is the range first, objects.end()
            // The BLACKLIST is the range objects.begin(), first

            // All objects in the BLACKLIST are BLACK
            // All objects in the WORKLIST are GRAY or WHITE
            
            auto first = objects.begin();
            
            // Begin marking
                        
            for (;;) {
                                
                // Initiate handshake with all threads
                for (std::size_t i = 0; i != THREADS; ++i) {
                    std::unique_lock lock(channels[i].mutex);
                    //printf("Collector acquires lock %zu\n", i);
                    //printf("Collector requests handshake\n");
                    //printf("Collector says it is %s\n", dirty ? "dirty" : "clean");
                    channels[i].pending = true;
                    channels[i].dirty = dirty;
                    //printf("Collector releases lock %zu\n", i);
                }
                
                // Wait for all handshakes to complete
                for (std::size_t i = 0; i != THREADS; ++i) {
                    {
                        std::unique_lock lock(channels[i].mutex);
                        //printf("Collector acquires lock %zu\n", i);
                        while (channels[i].pending) {
                            //printf("Collector sees handshake is pending %zu\n", i);
                            //printf("Collector wait-releases %zu\n", i);
                            channels[i].condition_variable.wait(lock);
                            //printf("Collector acquire-wakes %zu\n", i);
                        }
                        //printf("Collector sees handshake with %zu is complete\n", i);
                        //printf("Mutator %zu says it is %s\n", i, dirty ? "dirty" : "clean");
                        if (channels[i].dirty)
                            dirty = true;
                        assert(infants.empty());
                        infants.swap(channels[i].objects);
                        //printf("Collector releases lock %zu\n", i);
                    }
                    first = objects.insert(first,
                                           infants.begin(),
                                           infants.end()) + infants.size();
                    infants.clear();
                }
                
                if (!dirty)
                    break;
                
                std::size_t count;
                do {
                    //printf("Collector searches for GRAY\n");
                    std::size_t gray_found = 0;
                    count = 0;
                    for (auto last = objects.end(); first != last;) {
                        Object* src = *first;
                        Mark mark = src->_gc_mark.load(std::memory_order_relaxed);
                        if (mark == MARKED) {
                            ++gray_found;
                            count += src->_gc_scan();
                            ++first;
                        } else {
                            --last;
                            if (first != last)
                                std::swap(*first, *last);
                        }
                    }
                   // printf("Collector made %zu GRAY, found %zu GRAY\n", count, gray_found);
                } while (count);
                                
                // On the most recent pass, the collector turned all GRAY
                // objects it found BLACK, and made no new GRAY objects, so
                // the WORKLIST can only contain GRAY objects if the mutator
                // has made some since the last handshake
                
                dirty = false;
                                
            }
            
            // Free all WHITE objects
            
            {
                auto first2 = first;
                auto last = objects.end();
                for (; first2 != last; ++first2) {
                    Object* ref = *first2;
                    assert(ref && ref->_gc_mark.load(std::memory_order_relaxed) == UNMARKED);
                    delete ref;
                    ++freed;
                }
                objects.erase(first, last);
                printf("Total objects freed     %zu\n", freed);
            }
          
            // Start a new collection cycle
            
        }
        
    }

    
    template<typename T>
    struct Slot {
        
        std::atomic<T*> slot = nullptr;
        
        [[nodiscard]] T* 
        load(std::memory_order order = std::memory_order_relaxed) const {
            return slot.load(order);
        }
        
        [[nodiscard]] std::size_t 
        store(T* desired,
              std::memory_order order = std::memory_order_release) {
            return Object::_gc_shade(slot.exchange(desired, order));
        }

        [[nodiscard]] std::pair<T*, std::size_t> 
        exchange(T* desired,
                 std::memory_order order = std::memory_order_release) {
            Object* found = slot.exchange(found, order);
            return { Object::_gc_shade(found), found };
        }

        [[nodiscard]] std::pair<bool, std::size_t>
        compare_exchange(T*& expected, 
                         T* desired,
                         std::memory_order success = std::memory_order_release,
                         std::memory_order failure = std::memory_order_relaxed) {
            bool flag = compare_exchange(expected,
                                         desired,
                                         success,
                                         failure);
            return { flag, flag ? Object::_gc_shade(expected) : 0 };
        }
        
    };
    
    
    constexpr std::size_t SLOTS = 6;
    
    struct TestObject : Object {
        
        Slot<TestObject> slots[SLOTS];
        
        virtual ~TestObject() override = default;
        [[nodiscard]] virtual std::size_t _gc_scan() const override;
        
    };
    
    // When the collector scans an object it may see any of the values a slot
    // has taken on since the last handshake
    
    // The collector must load-acquire the pointers because of the ordering
    // - handshake
    // - mutator allocates A
    // - mutator writes A into B[i]    RELEASE
    // - collector reads B[i]          ACQUIRE
    // - collector dereferences A
    
    std::size_t TestObject::_gc_scan() const {
        std::size_t count = 0;
        for (const auto& slot : slots)
            if (Object::_gc_shade(slot.load(std::memory_order_acquire)))
                ++count;
        return count;
    }
    
    
    
    
    
    std::vector<TestObject*> nodesFromRoot(TestObject* root) {
        std::set<TestObject*> set;
        std::vector<TestObject*> vector;
        vector.push_back(root);
        set.emplace(root);
        for (int i = 0; i != vector.size(); ++i) {
            TestObject* src = vector[i];
            for (int j = 0; j != SLOTS; ++j) {
                TestObject* ref = src->slots[j].load();
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
    
    std::pair<std::set<TestObject*>, std::set<std::pair<TestObject*, TestObject*>>> nodesEdgesFromRoot(TestObject* root) {
        std::set<TestObject*> nodes;
        std::set<std::pair<TestObject*, TestObject*>> edges;
        std::stack<TestObject*> stack;
        stack.push(root);
        nodes.emplace(root);
        while (!stack.empty()) {
            TestObject* src = stack.top();
            stack.pop();
            for (int i = 0; i != SLOTS; ++i) {
                TestObject* ref = src->slots[i].load();
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
    
    void printObjectGraph(TestObject* root) {
        
        auto [nodes, edges] = nodesEdgesFromRoot(root);
        
        std::map<TestObject*, int> labels;
        int count = 0;
        for (TestObject* object : nodes) {
            labels.emplace(object, count++);
        }

        printf("%zu nodes\n", nodes.size());
        printf("%zu edges\n", edges.size());
        for (auto [a, b] : edges) {
            printf("  (%d) -> (%d)\n", labels[a], labels[b]);
        }
    }
    
    
    
    
    void mutate(const std::size_t index) {
        
        //printf("Thread %zu begins\n", index);
                
        size_t allocated = 0;
        
        std::vector<TestObject*> roots;
        bool dirty = false;
        std::vector<Object*> infants;
        
        auto allocate = [&infants]() -> TestObject* {
            TestObject* infant = new TestObject();
            infants.push_back(infant);
            return infant;
        };
        
        roots.push_back(allocate());

        std::vector<TestObject*> nodes;
        nodes = nodesFromRoot(roots.back());
                
        for (;;) {

            // Handshake
            
            {
                bool pending;
                bool collector_was_dirty = false;
                {
                    std::unique_lock lock(channels[index].mutex);
                    //printf("Thread %zu acquired lock\n", index);
                    pending = channels[index].pending;
                    if (pending) {
                        //printf("Thread %zu performs handshake\n", index);

                        collector_was_dirty = channels[index].dirty;
                        channels[index].dirty = dirty;
                        dirty = false;

                        assert(channels[index].objects.empty());
                        channels[index].objects.swap(infants);
                        assert(infants.empty());

                        channels[index].pending = false;
                    }
                    //printf("Thread %zu release lock\n", index);
                }

                if (pending) {
                    //printf("Thread %zu notifies\n", index);
                    channels[index].condition_variable.notify_all();
                }

                if (collector_was_dirty) {
                    for (TestObject* ref : roots)
                        if (Object::_gc_shade(ref))
                            dirty = true;
                }
                

            }

            
            // printf("Thread total objects allocated %zd\n", allocated);
            // do some graph work
            
            for (int i = 0; i != 100; ++i) {
                
                assert(!nodes.empty()); // we should at least have the root
                
                // choose two nodes and a slot
                int j = rand() % nodes.size();
                int k = rand() % SLOTS;
                int l = rand() % nodes.size();
                TestObject* p = nullptr;
                
                if (nodes.size() < rand() % 100) {
                    // add a new node
                    p = allocate();
                    ++allocated;
                } else if (nodes.size() < rand() % 100) {
                    // add a new edge betweene existing nodes
                    p = nodes[l];
                }
                // Write(nodes[j], k, p, &local_dirty);
                if (nodes[j]->slots[k].store(p))
                    dirty = true;
                
                if (dirty)
                    // Whatever we wrote overwrote an existing edge
                    nodes = nodesFromRoot(roots.back());
                
                // printObjectGraph(roots.back());
                
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
        collector.join();
    }
    
}

int main(int argc, const char * argv[]) {
    srand(79);
    gch3::exercise();
}



