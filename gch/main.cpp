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
        
    enum Mark : intptr_t // <-- machine word
    {
        MARKED,          // <-- objects are allocated in MARKED state
        UNMARKED,
        POISONED,
    };
    
    constexpr const char* MarkNames[] = {
        [MARKED  ] = "MARKED",
        [UNMARKED] = "UNMARKED",
        [POISONED] = "POISONED",
    };
    
    thread_local const char* thread_local_name = "main";
    
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
        
        // void* __vtbl;                             // <-- hidden pointer
                                                   
        mutable std::atomic<Mark> _gc_mark = MARKED; // <-- zero initialize
                                            
        virtual ~Object() = default;

        
        [[nodiscard]] static std::size_t _gc_shade(Object*); // WHITE -> GRAY
        static void _gc_clear(Object*);                      // BLACK -> WHITE
        static Object* _gc_validate(Object*);

        // shade(fields)
        [[nodiscard]] virtual std::size_t _gc_scan() const = 0;
        


    }; // struct Object

    std::size_t Object::_gc_shade(Object* object) {
        if (!object)
            return 0;
        Mark expected = UNMARKED;
        object->_gc_mark.compare_exchange_strong(expected,
                                                 MARKED,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed);
        switch (expected) {
            case UNMARKED:
                printf("%p: UNMARKED -> MARKED    %s\n", object, thread_local_name);
                return 1;
            case MARKED:
                return 0;
            default:
                printf("%p: POISONED %zd           %s\n", object, expected, thread_local_name);
                abort();
        }
    }
    
    void Object::_gc_clear(Object* object) {
        // TODO: revert me
        // _gc_mark.store(UNMARKED, std::memory_order_relaxed);
        Mark old = object->_gc_mark.exchange(UNMARKED, std::memory_order_relaxed);
        printf("%p: MARKED -> UNMARKED     %s\n", object, thread_local_name);
        assert(old == MARKED);
    }
    
    Object* Object::_gc_validate(Object* object) {
        if (object) {
            Mark mark = object->_gc_mark.load(std::memory_order_relaxed);
            switch (mark) {
                case MARKED:
                case UNMARKED:
                    break;;
                case POISONED:
                    printf("%p -> POISONED\n", object);
                    abort();
                default:
                    printf("%p -> Mark{%zu}\n", object, mark);
                    abort();
            }
        }
        return object;
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
        std::vector<Object*> objects; // <-- objects the thread has allocated since last handshake
        
    };
    
    constexpr std::size_t THREADS = 1;

    Channel channels[THREADS] = {};
    
    void hack_globals();
    
    void collect() {
        
        thread_local_name = "collector";
        
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

            printf("Collector: whitening %zu objects\n", objects.size());
            for (Object* object : objects) {
                assert(object);
                Object::_gc_clear(object);
            }

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
                    printf("Collector: locks %zu\n", i);
                    printf("Collector: requests handshake\n");
                    printf("Collector: publishes %s\n", dirty ? "dirty" : "clean");
                    channels[i].pending = true;
                    channels[i].dirty = dirty;
                    printf("Collector: unlocks %zu\n", i);
                }
                
                // Wait for all handshakes to complete
                for (std::size_t i = 0; i != THREADS; ++i) {
                    {
                        std::unique_lock lock(channels[i].mutex);
                        printf("Collector: locks %zu\n", i);
                        while (channels[i].pending) {
                            printf("Collector: waits on %zu\n", i);
                            std::cv_status status
                                = channels[i].condition_variable.wait_for(lock,
                                                                          std::chrono::seconds(1));
                            if (status == std::cv_status::timeout) {
                                printf("Collector: timed out waiting for %zu\n", i);
                                return;
                            }
                            printf("Collector: wakes on %zu\n", i);
                        }
                        printf("Collector: %zu reports it is %s\n", i, channels[i].dirty ? "dirty" : "clean");
                        if (channels[i].dirty)
                            dirty = true;
                        assert(infants.empty());
                        infants.swap(channels[i].objects);
                        printf("Collector: %zu sends %zu new allocations\n", i, infants.size());
                        printf("Collector: unlocks %zu\n", i);
                    }
                    first = objects.insert(first,
                                           infants.begin(),
                                           infants.end()) + infants.size();
                    printf("Collector: ingests %zu new allocations from mutator %zu\n", infants.size(), i);
                    infants.clear();
                }
                
                if (!dirty)
                    break;
                
                std::size_t count;
                do {
                    printf("Collector: searches for GRAY\n");
                    std::size_t gray_found = 0;
                    count = 0;
                    
                    for (Object* object : objects) {
                        assert(object && object->_gc_mark.load(std::memory_order_relaxed) != POISONED);
                    }
                    
                    for (auto last = objects.end(); first != last;) {
                        Object* src = *first;
                        Mark mark = src->_gc_mark.load(std::memory_order_relaxed);
                        if (mark == MARKED) {
                            ++gray_found;
                            count += src->_gc_scan();
                            ++first;
                        } else if (mark == UNMARKED) {
                            --last;
                            if (first != last)
                                std::swap(*first, *last);
                        } else {
                            abort();
                        }
                    }
                   printf("Collector: GRAY  -> BLACK %zu\n"
                          "           WHITE -> GRAY  %zu\n", gray_found, count);
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
                printf("Collector: freeing %zu WHITE objects\n", last - first2);
                for (; first2 != last; ++first2) {
                    Object* ref = *first2;
                    assert(ref && ref->_gc_mark.load(std::memory_order_relaxed) == UNMARKED);
                    printf("%p: UNMARKED -> POISONED\n", ref);
                    if (ref)
                        ref->_gc_mark.store(POISONED, std::memory_order_relaxed);
                    // delete ref;
                    ++freed;
                }
                objects.erase(first, last);
                
                for (Object* object : objects) {
                    assert(object->_gc_mark.load(std::memory_order_relaxed) == MARKED);
                }
                
                printf("Collector: lifetime objects freed %zu\n", freed);
            }
          
            // Start a new collection cycle
            
        }
        
    }

    
    template<typename T>
    struct Slot {
        
        std::atomic<T*> slot = nullptr;
        
        // The default memory orders are for a thread working on its private
        // data and releasing its writes to the collector thread
        
        // They are sufficient for a "producer" thread (it publishes pointers
        // for the garbage collector dereference), and for other sorts of
        // producer operation.
        //
        // A consumer thread must additionally ACQUIRE any pointer it will
        // dereference
        //     load -> std::memory_order_acquire
        //     exchange -> std::memory_order_acq_rel
        
        // store operations include a WRITE BARRIER that shades the overwitten
        // value from WHITE to GRAY
        
        // Raw pointers returned from these methods are safe until the next
        // handshake.
        
        // atomic interface
        
        T* load(std::memory_order order = std::memory_order_relaxed) const {
            return slot.load(order);
        }
        
        [[nodiscard]] std::size_t 
        store(T* desired,
              std::memory_order order = std::memory_order_release) {
            return Object::_gc_shade(slot.exchange(desired, order));
        }

        /*
        [[nodiscard]] std::pair<T*, std::size_t>
        exchange(T* desired,
                 std::memory_order order = std::memory_order_release) {
            Object* found = slot.exchange(found, order);
            return { found, Object::_gc_shade(found) };
        }
         */

        [[nodiscard]] std::pair<bool, std::size_t>
        compare_exchange_strong(T*& expected,
                                T* desired,
                                std::memory_order success = std::memory_order_release,
                                std::memory_order failure = std::memory_order_relaxed) {
            bool flag = slot.compare_exchange_strong(expected,
                                                     desired,
                                                     success,
                                                     failure);
            return { flag, flag ? Object::_gc_shade(expected) : 0 };
        }
        
        
        // pointer interface
        /*
        Slot() = default;
                
        explicit Slot(T* other) : slot(other) {}
        
        Slot(const Slot& other) : Slot(other.load()) {}
        
        template<typename U> explicit Slot(const Slot<U>& other) : Slot(other.load()) {}

        ~Slot() = default;
        
        Slot& operator=(T* other) {
            store(other);
            return *this;
        }
        
        Slot& operator=(const Slot& other) {
            return operator=(other.load());
        }
        
        template<typename U>
        Slot& operator=(const Slot<U>& other) {
            return operator=(other.load());
        }
                
        explicit operator T*() const {
            return load();
        }
        
        explicit operator bool() const {
            return static_cast<bool>(load());
        }

        T* operator->() const {
            T* object = load();
            assert(object);
            return object;
        }
        
        bool operator!() const {
            return !load();
        }
        
        T& operator*() const {
            T* object = load();
            assert(object);
            return *object;
        }
        
        template<typename U>
        bool operator==(const Slot<U>& other) {
            return load() == other.load();
        }

        template<typename U>
        bool operator==(U* other) {
            return load() == other;
        }
*/
    };
    
    
   
    
    
    
    template<typename T>
    struct Node : Object {
        Slot<Node> next;
        T payload;
        
        virtual ~Node() override = default;
        [[nodiscard]] virtual std::size_t _gc_scan() const override;
        
        explicit Node(auto&&... args)
        : payload(std::forward<decltype(args)>(args)...) {
        }
        
    };
    
    template<typename T>
    [[nodiscard]] std::size_t Node<T>::_gc_scan() const {
        return Object::_gc_shade(next.load(std::memory_order_acquire));
    }

        
    template<typename T>
    struct Stack {
    
        Slot<Node<T>> head;
        
        [[nodiscard]] std::size_t push(Node<T>* desired) {
            std::size_t count = 0;
            Node<T>* expected = head.load(std::memory_order_acquire);
            for (;;) {
                count += desired->next.store(expected);
                auto [flag, delta] 
                = head.compare_exchange_strong(expected,
                                               desired,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire);
                count += delta;
                if (flag)
                    return count;
            }
        }
        
        [[nodiscard]] std::pair<bool, std::size_t> pop(T& victim) {
            std::size_t count = 0;
            Node<T>* expected = head.load(std::memory_order_acquire);
            for (;;) {
                if (expected == nullptr)
                    return {false, count};
                Node<T>* desired = expected->next.load(std::memory_order_acquire);
                auto [flag, delta]
                = head.compare_exchange_strong(expected,
                                               desired,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire);
                count += delta;
                if (flag) {
                    victim = std::move(expected->payload);
                    expected->next.store(nullptr, std::memory_order_release);
                    return {true, count};
                }
            }
        }
        
        std::size_t _gc_scan() {
            return Object::_gc_shade(head.load(std::memory_order_acquire));
        }
        
    };

    
    
    
    Stack<int> global_stack;
    
    void hack_globals() {
        global_stack._gc_scan();
    }
    
    std::mutex global_integers_mutex;
    std::vector<int> global_integers;
    
    const char* mutator_names[3] = { "mutator0", "mutator1", "mutator2" };
    
    void mutate(const std::size_t index) {
        
        thread_local_name = mutator_names[index];
                        
        size_t allocated = 0;
        
        std::vector<Object*> roots;
        std::size_t dirty_count = 0;
        std::vector<Object*> infants;
        
        auto allocate = [&infants]() -> Node<int>* {
            Node<int>* infant = new Node<int>();
            infants.push_back(infant);
            return infant;
        };
        
        int k = (int) index;
        std::vector<int> integers;
                        
        for (;;) {

            // Handshake
            
            {
                bool pending;
                bool collector_was_dirty = false;
                {
                    std::unique_lock lock(channels[index].mutex);
                    printf("Mutator %zu: locks\n", index);
                    pending = channels[index].pending;
                    if (pending) {
                        printf("Mutator %zu: performing handshake\n", index);

                        dirty_count += global_stack._gc_scan();
                        for (Object* ref : roots)
                            dirty_count += Object::_gc_shade(ref);
                                               
                        channels[index].dirty = (bool) dirty_count;
                        printf("Mutator %zu: publishing %s\n", index, dirty_count ? "dirty" : "clean");
                        dirty_count = 0;

                        printf("Mutator %zd: publishing %zd new allocations\n", index, infants.size());
                        assert(channels[index].objects.empty());
                        channels[index].objects.swap(infants);
                        assert(infants.empty());

                        channels[index].pending = false;
                        printf("Mutator %zu: completed handshake\n", index);

                    } else {
                        printf("Mutator %zu: handshake not requested\n", index);
                    }
                    printf("Mutator %zu: unlocks\n", index);
                }

                if (pending) {
                    printf("Mutator %zu: notifies collector\n", index);
                    channels[index].condition_variable.notify_all();
                }
            }
            
            // handshake completes
                        
            // do some graph work
            
            for (int i = 0; i != 10; ++i) {
                
                if (!(rand() % 2)) {
                    int j = -1;
                    auto [flag, count] = global_stack.pop(j);
                    dirty_count += count;
                    if (flag) {
                        integers.push_back(j);
                    }
                } else {
                    Node<int>* desired = allocate();
                    ++allocated;
                    desired->payload = k;
                    dirty_count += global_stack.push(desired);
                    k += THREADS;
                }
                
            }
            
            printf("Mutator %zu: WHITE -> GRAY +%zu\n", index, dirty_count);
            
            printf("Mutator %zu: lifetime objects allocated %zu\n", index, allocated);

            if (k > 100000000) {
                std::unique_lock lock(global_integers_mutex);
                global_integers.insert(global_integers.end(), integers.begin(), integers.end());
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
    gch3::exercise();
}



