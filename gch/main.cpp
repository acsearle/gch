//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//

#include <atomic>
#include <cassert>
#include <concepts>
#include <deque>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <stack>
#include <thread>
#include <vector>

#include "queue.hpp"

namespace gch3 {
    
    // Base class of all garbage collected objects
    // - virtual table for scan and finalize
    // - object color for mark-sweep
                    
    struct Object {
        
        // void* __vtbl;
        mutable std::atomic<int64_t> _gc_color;
                            
        Object();
        virtual ~Object() = default;
        
        static void _gc_shade(Object*);  // WHITE -> GRAY
        
        // shade(fields)
        virtual void _gc_scan() const = 0;
        
        // debug
        virtual void _gc_print() const = 0;
        
    }; // struct Object
    
    static_assert(sizeof(Object) == 16);
    static_assert(alignof(Object) == 8);
    
    // We change the color : integer mapping, and the allocation color, at
    // separate points in the algorithm
    
    struct Configuration {
        int64_t WHITE = 0; // <-- not reached yet
        int64_t GRAY  = 1; // <-- will be scanned
        int64_t BLACK = 2; // <-- was scanned
        int64_t GREEN = 3; // <-- poison value
        int64_t ALLOC = 0;
        const char* color_names[4] = { "WHITE", "GRAY", "BLACK", "GREEN" };
    };
    
    // We keep gc state in a thread_local to reduce awkward passing around of
    // context arguments
    
    // TODO: thread_local can be expensive
    
    struct Local : Configuration {
        const char* name = nullptr;
        bool dirty = false;
        Queue<Object*> infants;
    };
    
    thread_local Local local;
    
#define LOG(F, ...) printf("%s%c: " F, local.name, 'c' + local.dirty  __VA_OPT__(,) __VA_ARGS__)
    
    Object::Object() : _gc_color(local.ALLOC) {
        // LOG("allocates %p as %s\n", this, local.color_names[local.ALLOC]);
        local.infants.push_back(this);
    }

    void Object::_gc_shade(Object* object) {
        if (object) {
            int64_t expected = local.WHITE;
            bool result = object->_gc_color
                .compare_exchange_strong(expected, local.GRAY,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed);
            if (result) {
                // LOG("shades %p %s -> GRAY\n", object, local.color_names[expected]);
                assert(expected == local.WHITE);
                local.dirty = true;
            } else {
                if (expected != local.GREEN) {
                    // LOG("shades %p %s\n", object, local.color_names[expected]);
                } else {
                    LOG("shades %p %s\n --- FATAL ---\n", object, local.color_names[expected]);
                    abort();
                }
            }
        } else {
            // LOG("shades nullptr\n");
        }
    }
    
    
    
    // Mutator-collector handshake channel
    //
    // The mutator must periodically check in for pending requests from the
    // mutator.
    //
    // - to mark its roots
    // - to report if it has produced any WHITE -> GRAY transitions since last asked
    // - to export its pending allocations
    // - to change its color <-> integer mapping
    
    struct Channel {
        
        std::mutex mutex;
        std::condition_variable condition_variable;
        
        bool pending = false;
        Configuration configuration;
        
        bool dirty = false;
        Queue<Object*> infants; // <-- objects the thread has allocated since last handshake
        
    };
    
    
    // A steady state:
    //
    // All mutators are allocating WHITE.
    // The write barrier shades some WHITE -> GRAY.
    // The mutators are produding WHITE and GRAY objects.
    // There are GRAY and WHITE objects.
    // There are no BLACK objects.
    //
    // The collector requests mutators start allocating BLACK.
    // Now some mutators allocate BLACK, some WHITE.
    // The write barrier shades some objects WHITE to GRAY.
    // All colors exist.
    // The write barrier prevents the formation of BLACK nodes with WHITE
    // children, because all writes GRAY the children.
    // The mutators are producing objects of all colors.
    //
    // Handshakes complete.
    // Now all mutators are allocating BLACK.
    // All mutators have informed the collector of the objects it allocated
    // WHITE.
    // All mutators are shading WHITE to GRAY
    // All mutators shade any WHITE roots GRAY between handshakes.
    // The collector has been told about all objects that were allocated WHITE.
    // The collector knows about all objects that are WHITE or GRAY.
    // The collector does not know about allocated BLACK objects.
    // The collector sweeps known objects and traces them.
    // The tracing turns all GRAY objects BLACK, and some WHITE objects GRAY.
    // The mutators turn WHITE objects GRAY, and make new BLACK objects.
    // No new WHITE objects are being produced, so this cannot continue
    // indefinitely.
    // If there is no path to an object, it remains WHITE.
    // The collector sweeps until it discovers no more GRAY objects to trace.
    //
    // The collector now considers there may be no GRAY objects.
    // The mutators may have turned a WHITE object GRAY behind the collector,
    // The collector initiates a handshake to see if the mutators have made any
    // GRAY objects since the last handshake.  Note that the collector may
    // not even see GRAY objects made by mutators until the handshake is
    // performed, due to relaxed memory ordering.
    //
    // If some mutator has made GRAY objects (is dirty) then the collector
    // continues scanning.
    //
    // If no mutator has made GRAY objects, then we have reached a steady
    // state where all reachable objects are BLACK, and all WHITE objects are
    // unreachable.  Some unreachable objects may also be BLACK, and they will
    // not be reclaimed this cycle.
    //
    // All objects are BLACK or WHITE.
    // All WHITE objects are known to the collector.
    // Some recent BLACK allocations are not known to the collector.
    // All WHITE objects are unreachable by the mutators.
    // The mutators are allocating BLACK.
    // The mutator write barrier can reach now WHITE objects so it makes no objects
    // GRAY.
    //
    // The collector frees all WHITE objects.
    //
    // There are only BLACK objects.
    // Some BLACK objects are known to the collector.
    // Some BLACK objects are recently allocated and not known to the collector.
    // The mutator write barrier encounters no WHITE objects to turn GRAY.
    //
    // The collector requests handshakes to SWAP the encoding of BLACK and WHITE.
    //
    // Pre-handshake mutators allocate BLACK and see BLACK objects.
    // Post-handshake mutators allocate WHITE and see WHITE objects.
    // The post-handshake mutators turn WHITE objects GRAY.
    // The pre-handshake mutator may see these GRAY objects.
    //
    //
    // All handshakes complete.
    // All mutators now see
    
    
    
    
    
    constexpr std::size_t THREADS = 3;

    Channel channels[THREADS] = {};
        
    void collect() {
        
        local.name = "C0";
        
        size_t freed = 0;
        
        std::vector<Object*> objects;
        Queue<Object*> infants;
                        
        for (;;) {
            
            LOG("collection begins\n");

            // Mutators allocate WHITE and mark GRAY
            // There are no BLACK objects
            
            /*
            LOG("enumerating %zu known objects\n",  objects.size());
            for (Object* object : objects) {
                object->_gc_print();
                intptr_t color = object->_gc_color.load(std::memory_order_relaxed);
                assert(color == local.WHITE || color == local.GRAY);
            }
             */
            
            assert(local.ALLOC == local.WHITE);
            local.ALLOC = local.BLACK;
            
            LOG("begin transition to allocating BLACK\n");
            
            // initiate handshakes
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                LOG("requests handshake %zu\n", i);
                channels[i].pending = true;
                channels[i].configuration = local;
            }
            
            // receive handshakes
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                while (channels[i].pending) {
                    std::cv_status status
                    = channels[i].condition_variable.wait_for(lock,
                                                              std::chrono::seconds(1));
                    if (status == std::cv_status::timeout) {
                        LOG("timed out waiting for %zu\n --- FATAL ---\n", i);
                        abort();
                    }
                }
                assert(infants.empty());
                infants.swap(channels[i].infants);
                lock.unlock();
                LOG("ingesting objects from %zu\n", i);
                std::size_t count = 0;
                for (Object* object = nullptr; 
                     infants.pop_front(object);
                     ++count, objects.push_back(object))
                    ;
                LOG("ingested %zu objects from %zu\n", count, i);
                assert(infants.empty());
            }
            
            LOG("end transition to allocating BLACK\n");
            
            for (;;) {
                
                //LOG("enumerating %zu known objects\n",  objects.size());
                //for (Object* object : objects) {
                //    object->_gc_print();
                //}
                
                assert(!local.dirty);

                do {
                    local.dirty = false;
                    std::size_t blacks = 0;
                    std::size_t grays = 0;
                    std::size_t whites = 0;
                    LOG("scanning...\n");
                    for (Object* object : objects) {
                        //object->_gc_print();
                        assert(object);
                        int64_t expected = local.GRAY;
                        object->_gc_color.compare_exchange_strong(expected,
                                                                  local.BLACK,
                                                                  std::memory_order_relaxed,
                                                                  std::memory_order_relaxed);
                        if (expected == local.BLACK) {
                            ++blacks;
                        } else if (expected == local.GRAY) {
                            ++grays;
                            object->_gc_scan(); // <-- will set local.dirty when it marks a node GRAY
                        } else if (expected == local.WHITE) {
                            ++whites;
                        } else {
                            abort();
                        }
                    }
                    LOG("        ...scanning found %zu, %zu, %zu, 0\n", blacks, grays, whites);
                } while (local.dirty);
                
                assert(!local.dirty);
                
                // the collector has traced everything it knows about
                
                // handshake to see if the mutators have shaded any objects
                // GRAY since the last handshake
                
                // initiate handshakes
                for (std::size_t i = 0; i != THREADS; ++i) {
                    std::unique_lock lock(channels[i].mutex);
                    LOG("requests handshake %zu\n", i);
                    channels[i].pending = true;
                }

                // receive handshakes
                for (std::size_t i = 0; i != THREADS; ++i) {
                    std::unique_lock lock(channels[i].mutex);
                    while (channels[i].pending) {
                        std::cv_status status
                        = channels[i].condition_variable.wait_for(lock,
                                                                  std::chrono::seconds(1));
                        if (status == std::cv_status::timeout) {
                            LOG("timed out waiting for %zu\n --- FATAL ---\n", i);
                            abort();
                        }
                    }
                    LOG("%zu reports it was %s\n", i, channels[i].dirty ? "dirty" : "clean");
                    if (channels[i].dirty)
                        local.dirty = true;
                }
                
                if (!local.dirty)
                    break;
                
                local.dirty = false;
                
            }
            
            // Neither the collectors nor mutators marked any nodes GRAY since
            // the last handshake.  All remaining WHITE objects are unreachable.
            
            {
                LOG("begin sweep\n");
                std::size_t blacks = 0;
                std::size_t whites = 0;
                auto first = objects.begin();
                auto last = objects.end();
                for (; first != last;) {
                    Object* object = *first;
                    assert(object);
                    //object->_gc_print();
                    int64_t color = object->_gc_color.load(std::memory_order_relaxed);
                    if (color == local.BLACK) {
                        ++blacks;
                        ++first;
                        //LOG("retains %p BLACK\n", object);
                    } else if (color == local.WHITE) {
                        ++whites;
                        --last;
                        if (first != last) {
                            std::swap(*first, *last);
                        }
                        objects.pop_back();
                        object->_gc_color.store(local.GREEN, std::memory_order_relaxed);
                        //LOG("frees %p WHITE -> GREEN\n", object);
                        delete object;
                        ++freed;
                    } else {
                        LOG("sweep sees %s\n --- FATAL ---\n", local.color_names[color]);
                        abort();
                    }
                    
                }
                LOG("swept %zu, 0, %zu, 0\n", blacks, whites);
                LOG("freed %zu\n", whites);
                LOG("lifetime freed %zu\n", freed);
            }
            
            // Only BLACK objects exist

            // Reinterpret the colors
            
            std::swap(local.BLACK, local.WHITE);
            std::swap(local.color_names[local.BLACK], local.color_names[local.WHITE]);
            
            // Publish the reinterpretation
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                LOG("requests handshake %zu\n", i);
                channels[i].pending = true;
                channels[i].configuration = local;
            }
            
            // receive handshakes
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                while (channels[i].pending) {
                    std::cv_status status
                    = channels[i].condition_variable.wait_for(lock,
                                                              std::chrono::seconds(1));
                    if (status == std::cv_status::timeout) {
                        LOG("timed out waiting for %zu\n --- FATAL ---\n", i);
                        abort();
                    }
                }
                LOG("%zu acknowledges recoloring\n", i);
            }
            
            LOG("collection ends\n");
            
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
        
        void
        store(T* desired,
              std::memory_order order = std::memory_order_release) {
            Object::_gc_shade(desired);
            T* old = slot.exchange(desired, order);
            Object::_gc_shade(old);
        }

        bool compare_exchange_strong(T*& expected,
                                     T* desired,
                                     std::memory_order success = std::memory_order_release,
                                     std::memory_order failure = std::memory_order_relaxed) {
            bool did_exchange = slot.compare_exchange_strong(expected,
                                                             desired,
                                                             success,
                                                             failure);
            if (did_exchange) {
                Object::_gc_shade(expected);
                Object::_gc_shade(desired);
            }
            return did_exchange;
            
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
        virtual void _gc_scan() const override;
        virtual void _gc_print() const override;

        explicit Node(auto&&... args)
        : payload(std::forward<decltype(args)>(args)...) {
        }
        
    };
    
    template<typename T>
    void Node<T>::_gc_scan() const {
        // LOG("scans %p %s\n", (Object*) this, local.color_names[_gc_color.load(std::memory_order_relaxed)]);
        
        // Implementation A: shade chold GRAY and return
        // Object::_gc_shade(next.load(std::memory_order_acquire));
        
        // Implementation B:
        // Trace white chains immediately
        const Node<T>* parent = this;
        for (;;) {
            Node<T>* child = parent->next.load(std::memory_order_acquire);
            if (!child)
                return;
            int64_t expected = local.WHITE;
            // If WHITE, transform directly to BLACK and trace into that node
            if (child->_gc_color.compare_exchange_strong(expected,
                                                         local.BLACK,
                                                         std::memory_order_relaxed,
                                                         std::memory_order_relaxed)) {
                local.dirty = true;
                parent = child;
            } else {
                assert(expected == local.GRAY || expected == local.BLACK);
                return;
            }
        }
    }

    template<typename T>
    void Node<T>::_gc_print() const {
        /*
        int64_t parent_color = _gc_color.load(std::memory_order_relaxed);
        Object* child = next.slot.load(std::memory_order_acquire);
        if (child) {
            int64_t child_color = child->_gc_color.load(std::memory_order_relaxed);
            LOG("sees %p %s -> %p %s\n", this, local.color_names[parent_color], child, local.color_names[child_color]);
        } else {
            LOG("sees %p %s -> nullptr\n", this, local.color_names[parent_color]);
        }
         */
    }

        
    template<typename T>
    struct Stack {
    
        Slot<Node<T>> head;
        
        void push(Node<T>* desired) {
            Node<T>* expected = head.load(std::memory_order_acquire);
            do {
                desired->next.store(expected);
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
                    
                    expected->next.store(nullptr);
                    
                    
                    
                    return true;
                }
            }
        }
        
        void _gc_scan() {
            //LOG("scans Stack<T>\n");
            Object::_gc_shade(head.load(std::memory_order_acquire));
        }
        
    };

    
    
    Stack<int> global_stack;
    
    std::mutex global_integers_mutex;
    std::vector<int> global_integers;
        
    void mutate(const std::size_t index) {
        
        char _name[5] = {' ', ' ', 'M','0','\0'};
        _name[3] += index;
        
        local.name = _name;
                        
        size_t allocated = 0;
        
        std::vector<Object*> roots;
                
        int k = (int) index;
        std::vector<int> integers;
                        
        for (;;) {

            // Handshake
            
            {
                bool pending;
                {
                    std::unique_lock lock(channels[index].mutex);
                    pending = channels[index].pending;
                    if (pending) {
                        LOG("handshaking\n");
                        
                        LOG("lifetime alloc %zu\n", allocated);
                        
                        LOG("was WHITE=%lld BLACK=%lld ALLOC=%lld\n", local.WHITE, local.BLACK, local.ALLOC);
                        
                        bool flipped_ALLOC = local.ALLOC != channels[index].configuration.ALLOC;
                        
                        (Configuration&) local = channels[index].configuration;
                        
                        LOG("becomes WHITE=%lld BLACK=%lld ALLOC=%lld\n", local.WHITE, local.BLACK, local.ALLOC);
           
                        channels[index].dirty = local.dirty;
                        LOG("publishing %s\n", local.dirty ? "dirty" : "clean");
                        local.dirty = false;

                        if (flipped_ALLOC) {
                            LOG("publishing ?? new allocations\n");
                            assert(channels[index].infants.empty());
                            channels[index].infants.swap(local.infants);
                            assert(local.infants.empty());
                        }

                        channels[index].pending = false;

                    } else {
                        // LOG("handshake not requested\n");
                    }
                }

                if (pending) {
                    LOG("notifies collector\n");
                    channels[index].condition_variable.notify_all();
                }
            }
            
            // handshake completes

            // scan the roots
            global_stack._gc_scan();
            for (Object* ref : roots)
                Object::_gc_shade(ref);

                        
            // do some graph work
            
            for (int i = 0; i != 1000; ++i) {
                
                if (!(rand() % 2)) {
                    int j = -1;
                    if(global_stack.pop(j))
                        integers.push_back(j);
                } else {
                    Node<int>* desired = new Node<int>;
                    ++allocated;
                    desired->payload = k;
                    global_stack.push(desired);
                    k += THREADS;
                }
                
            }
            
            // std::this_thread::sleep_for(std::chrono::milliseconds{1});
                        
            // LOG("lifetime alloc %zu\n", allocated);

            if (k > 10000000) {
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



