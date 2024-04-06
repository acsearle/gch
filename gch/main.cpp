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

    Channel channel;
    
    
    void collect() {
        
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
                
                // Initiate handshake
                {
                    std::unique_lock lock(channel.mutex);
                    channel.pending = true;
                    channel.dirty = dirty;
                    while (channel.pending)
                        channel.condition_variable.wait(lock);
                    dirty = channel.dirty;
                    assert(infants.empty());
                    infants.swap(channel.objects);
                }
                
                first = objects.insert(first,
                                       infants.begin(),
                                       infants.end()) + infants.size();
                infants.clear();

                if (!dirty)
                    break;
                dirty = false;

                std::size_t count;
                do {
                    count = 0;
                    for (auto last = objects.end(); first != last;) {
                        Object* src = *first;
                        Mark mark = src->_gc_mark.load(std::memory_order_relaxed);
                        if (mark == MARKED) {
                            count += src->_gc_scan();
                            ++first;
                        } else {
                            --last;
                            if (first != last)
                                std::swap(*first, *last);
                        }
                    }
                } while (count);
                
                // On the most recent pass, the collector turned all GRAY
                // objects it found BLACK, and made no new GRAY objects, so
                // the WORKLIST can only contain GRAY objects if the mutator
                // has made some since the last handshake
                                
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
        
        [[nodiscard]] T* load(std::memory_order order
                              = std::memory_order_relaxed) const {
            return slot.load(order);
        }
        
        [[nodiscard]] std::size_t store(T* ref,
                                 std::memory_order order
                                 = std::memory_order_release) {
            return Object::_gc_shade(slot.exchange(ref, order));
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
    
    
    
    
    void mutate() {
                
        size_t allocated = 0;
        
        std::vector<TestObject*> local_roots;
        bool local_dirty = true;
        std::vector<Object*> local_infants;
        
        auto allocate = [&local_infants]() -> TestObject* {
            TestObject* infant = new TestObject();
            local_infants.push_back(infant);
            return infant;
        };
        
        local_roots.push_back(allocate());

        std::vector<TestObject*> nodes;
        nodes = nodesFromRoot(local_roots.back());
                
        for (;;) {

            // Handshake
            
            {
                bool pending;
                bool collector_dirty = false;
                {
                    std::unique_lock lock(channel.mutex);
                    pending = channel.pending;
                    if (pending) {
                        
                        collector_dirty = channel.dirty;

                        if (channel.dirty)
                            local_dirty = true;
                        channel.dirty = local_dirty;
                        local_dirty = false;
                        
                        if (channel.dirty) {
                            local_dirty = true;
                        } else {
                            channel.dirty = local_dirty;
                        }

                        assert(channel.objects.empty());
                        channel.objects.swap(local_infants);
                        assert(local_infants.empty());

                        channel.pending = false;
                    }
                }
                if (pending) {
                    channel.condition_variable.notify_all();
                }

                if (collector_dirty) {
                    for (TestObject* ref : local_roots)
                        if (Object::_gc_shade(ref))
                            local_dirty = true;
                }
                

            }

            
            printf("Total objects allocated %zd\n", allocated);
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
                    local_dirty = true;
                // Whatever we wrote may have overwritten an existing edge
                
                nodes = nodesFromRoot(local_roots.back());
                
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



