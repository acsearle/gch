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
    
    enum Mark : intptr_t {
        MARKED = 0,
        UNMARKED = 1,
    };

    constexpr const char* MarkNames[3] = {
        [MARKED] = "MARKED",
        [UNMARKED] = "UNMARKED",
    };

    constexpr std::size_t SLOTS = 6;
    
    struct Object;
            
    enum Requests {
        REQUEST_ROOTS = 1,
        REQUEST_DIRTY = 2,
        REQUEST_INFANTS = 4,
    };

    std::mutex mutex;
    std::condition_variable condition_variable;
    
    int request_flags = 0;
    bool dirty = false;
    std::vector<Object*> infants;
        
    // mutator informs collector it made grays since last handshake
    
    // collector requests mutator mark its roots GRAY
    // this makes more sense when scanning a thread stack; otherwise why not
    // just have the collector share the roots and scan them at will?
    //
    
    struct alignas(64) Object {
        
        // 8 : __vtbl
        // 8 : mark
                                           
        mutable std::atomic<Mark> mark = MARKED;
        std::atomic<Object*> slots[SLOTS];
        
        virtual ~Object() = default;
        
        virtual std::size_t scan() const;

    };
    
    Object* Read(Object* src, std::size_t i) {
        assert(src);
        assert(i < SLOTS);
        // The mutator thread is already ordered wrt the fields
        return src->slots[i].load(std::memory_order_relaxed);
    }
        
    
    void Write(Object* src, std::size_t i, Object* ref, bool* dirty) {
        assert(src);
        assert(i < SLOTS);
        // src is reachable, because the mutator reached it
        // ref is reachable (or nullptr), because the mutator reached it

        
        if (Object* old = src->slots[i].exchange(ref, std::memory_order_release)) {
            // old was reachable, because the mutator just reached it
            
            // The store is RELEASE because the collector will dereference the
            // pointer so writes to its Object must happen-before the store
            
            // The load part is RELAXED because the mutator itself perfomed the
            // original store
            
            // src may be BLACK and ref may be WHITE; we have just violated
            // the strong tricolor invariant
            
            // old may have been part of the chain by which such a WHITE ref
            // was reached, so we GRAY it; we have just preserved the weak
            // tricolor invariant
            
            Mark expected = UNMARKED;
            if (old->mark.compare_exchange_strong(expected,
                                                  MARKED,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
                // We race the collector to transition old from UNMARKED:WHITE
                // to MARKED:GRAY.
                
                // Memory order is relaxed because no memory is published by
                // this operation.  Synchronization is ultimately achieved via
                // the handshake.
                
                assert(expected == UNMARKED);
                *dirty = true;
                
                // When the collector is unable to find any GRAY items, it
                // rendevous with each thread and clears the dirty flag.
                //
                // If the collector saw any set flag, it must scan again for
                // GRAY items.
                //
                // Dirty assists in discovering GRAYs made between the
                // collector's last inspection of a white object, and the
                // handshake.
                //
                // The mutator can only produce GRAY objects by reaching
                // WHITE objects, so it can no longer produce GRAY when the
                // scan is actually complete.
                //
                // Allowed operations are 
                // - WHITE -> GRAY, performed by both the mutator and collector
                // - GRAY -> BLACK, performed only by the collector
                // - allocate BLACK, performed only by the mutator
                // so we cannot run indefinitely; in fact we can do only
                // do up to the initial number of WHITE objects of each
                // transition, so we will terminate.
                //
                // Newly allocated BLACK objects are of no interest to the
                // mark phase.  We can incorporate them en masse when we
                // sweep.
                

            } else {
                assert(expected == MARKED);
            }
        }

    }
    
    std::size_t Object::scan() const {
        std::size_t count = 0;
        for (std::atomic<Object*> const& field : slots) {
            Object* ref = field.load(std::memory_order_acquire);
            if (ref) {
                Mark expected = UNMARKED;
                if (ref->mark.compare_exchange_strong(expected,
                                                      MARKED,
                                                      std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
                    // ref was UNMARKED:WHITE and is now MARKED:GRAY
                    assert(expected == UNMARKED);
                    ++count;
                } else {
                    // ref is MARKED:GRAY or MARKED:BLACK
                    assert(expected == MARKED);
                }
            }
        }
        return count;
    }
    
    void collect() {
        
        size_t freed = 0;
        
        std::vector<Object*> objects;
        std::vector<Object*> local_infants;
        for (;;) {
            
            // step 1, get roots snapshot
            {
                std::unique_lock lock(mutex);
                request_flags = REQUEST_ROOTS;
                while (request_flags) {
                    condition_variable.wait(lock);
                }
                // thread has now turned white roots gray
            }
            
            // step 2, process worklist
            {
                
                // "objects" contains all objects that survived the last
                // collection and were marked WHITE.
                //
                // "objects" does not contain newly allocated objects
                
                // Objects starts all WHITE
                
                // The mutator GRAYS its roots (above) and concurrently with our
                // marking, the mutator write barrier generates more GRAY
                // objects.
                
                // The collector walks the list of objects inspecting their
                // color.  GRAY objects are BLACKENED, flipping more objects
                // GRAY.
             
                // objects.begin()
                //     MARKED:BLACK
                // first
                //     UNMARKED:WHITE or MARKED:GRAY to be processed
                // last
                //     was UNMARKED:WHITE when processed but may turn MARKED:GRAY concurrently
                // end
                
                auto first = objects.begin();
                
                printf("Scanning %zu objects\n", objects.size());
                                
                for (;;) {
                    
                    auto last = objects.end();
                    
                    std::size_t count = 0;
                    
                    while (first != last) {
                        
                        Object* src = *first;
                        assert(src);
                        // Read color
                        Mark mark = src->mark.load(std::memory_order_relaxed);
                        switch (mark) {
                            case MARKED: {
                                // src is MARKED:GRAY
                                count += src->scan();
                                // src becomes MARKED:BLACK
                                ++first;
                                break;
                            }
                            case UNMARKED: {
                                // src is UNMARKED:WHITE
                                --last;
                                if (first != last)
                                    std::swap(*first, *last);
                                break;
                            }
                        } // switch (src->mark)
                    } // while(first != last)
                    
                    // We've now looked at every object
                    // Black ones will stay black and require no further action
                    // They have beem moved to the front of the list
                    
                    // White ones have been moved to the back of the list, but
                    // once there they may have been GRAYED by
                    // - the mutator write barrier shading a victim
                    // - the collector scanning a GRAY object
                    
                    // Do we need to scan again?
                    
                    printf("    Transformed WHITE -> GRAY : %zd\n", count);
                    
                    if (count != 0) {
                        printf("  Rescanning %zu of %zu objects (collector made GRAYS)\n",
                               objects.end() - first,
                               objects.size());
                        continue;
                    }
                    
                    // Since we BLACKENED every GRAY we found, and we made no
                    // new GRAYS, any GRAYS in the list must be from the
                    // mutator GRAYING a WHITE we had already processed.
                                        
                    {
                        bool local_dirty;
                        {
                            std::unique_lock lock(mutex);
                            request_flags = REQUEST_DIRTY | REQUEST_INFANTS;
                            while (request_flags) {
                                condition_variable.wait(lock);
                            }
                            local_dirty = dirty;
                            local_infants.insert(local_infants.end(), infants.begin(), infants.end());
                            infants.clear();
                        }
                        if (!local_dirty)
                            break;
                    }
                    
                    printf("  Rescanning %zu objects (mutator made GRAYS)\n", objects.end() - first);

                    // the mutator made new GRAYs, so we have to scan again
                    // to find them, or to prove that we got them last scan
                    
                } // scan the reduced was-WHITE partition again
                
                // we have proved there are no GRAY objects
                // TODO: we could assert this here with one last sweep
                printf("  Terminates with [ BLACK : %zd , WHITE : %zd)\n", first - objects.begin(), objects.end() - first);
                
                {
                    // The unreachable objects extend from first to objects.end()
                    auto n = objects.size();
                    auto first2 = first;
                    auto last = objects.end();
                    for (; first2 != last; ++first2) {
                        Object* ref = *first2;
                        assert(ref->mark.load(std::memory_order_relaxed) == UNMARKED);
                        delete ref;
                        ++freed;
                    }
                    objects.erase(first, last);
                    auto m = objects.size();
                    printf("  Swept %zu objects total, %zu -> %zu (%zu)\n", freed, n, m, n-m);
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
                // put the newly allocated objects at the end, reusing where
                // the WHITEs used to be.
                objects.insert(objects.end(), local_infants.begin(), local_infants.end());
                local_infants.clear(); // capacity unchanged
                
                for (Object* ref : objects) {
#ifdef NDEBUG
                    ref->mark.store(UNMARKED, std::memory_order_relaxed);
#else
                    Mark old = ref->mark.exchange(UNMARKED, std::memory_order_relaxed);
                    assert(old == MARKED);
#endif
                }
                
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
        
        std::vector<Object*> local_roots;
        bool local_dirty = false;
        std::vector<Object*> local_infants;
        
        auto allocate = [&local_infants]() -> Object* {
            Object* infant = new Object();
            local_infants.push_back(infant);
            return infant;
        };
        
        local_roots.push_back(allocate());

        std::vector<Object*> nodes;
        nodes = nodesFromRoot(local_roots.back());
                
        for (;;) {

            // Handshake
            
            {
                int local_request_flags;
                {
                    std::unique_lock lock(mutex);
                    local_request_flags = request_flags;
                    
                    if (request_flags) {
                        if (request_flags & REQUEST_ROOTS) {
                            assert(local_roots.size() <= 1);
                            for (Object* ref : local_roots) {
                                Mark expected = UNMARKED;
                                if (ref->mark.compare_exchange_strong(expected,
                                                                      MARKED,
                                                                      std::memory_order_relaxed,
                                                                      std::memory_order_relaxed)) {
                                    local_dirty = true;
                                }
                            }
                        }
                        if (request_flags & REQUEST_DIRTY) {
                            dirty = local_dirty;
                            local_dirty = false;
                        }
                        if (request_flags & REQUEST_INFANTS) {
                            assert(infants.empty());
                            infants.swap(local_infants);
                            assert(local_infants.empty());
                        }
                        request_flags = 0;
                    }
                }
                if (local_request_flags)
                    condition_variable.notify_all();
            }

            
            printf("Total objects allocated %zd\n", allocated);
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
                Write(nodes[j], k, p, &local_dirty);
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



