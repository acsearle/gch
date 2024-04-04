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
    
    struct Object {
        Color color;
        Object* slots[SLOTS];
    };
    
    bool isWhite(Object* ref) {
        return ref->color == WHITE;
    }
    
    bool isGray(Object* ref) {
        return ref->color == GRAY;
    }
    
    bool isBlack(Object* ref) {
        return ref->color == BLACK;
    }

    Object* Read(Object* src, int i) {
        // std::unique_lock lock(mutex);
        assert(src);
        assert(i < SLOTS);
        return src->slots[i];
    }
        
    // The mutator can turn a BLACK object back to GRAY only when it encounters
    // a WHITE object, implying that some reachable objects are white and thus
    // the tracing is incomplete
    /*
    void revert(Object* ref) {
        assert(isBlack(ref));
        ref->color = GRAY;
        worklist.push_back(ref);
    }
     */
    
    // Steele
    void Write(Object* src, int i, Object* ref) {
        std::unique_lock lock(mutex);
        src->slots[i] = ref;
        if (ref && isBlack(src) && isWhite(ref)) {
            src->color = GRAY;
            worklist.push_back(src);
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
                        //assert(object->color == GRAY);
                        object->color = BLACK;
                        for (Object* field : object->slots) {
                            if (field && field->color == WHITE) {
                                field->color = GRAY;
                                worklist.push_back(field);
                            }
                        }
                    }
                }
                if (!object)
                    break;
                                
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
                    Color color = WHITE;
                    {
                        std::unique_lock lock(mutex);
                        color = ref->color;
                        if (color == BLACK)
                            ref->color = WHITE; // reset the color
                    }
                    if (color == WHITE) {
                        // free the inacessible object
                        delete ref;
                        ++freed;
                    } else {
                        // move it up in the list
                        *first = ref;
                        ++first;
                    }
                }
                size_t n = objects.size();
                objects.erase(first, last);
                size_t m = objects.size();
                // printf("     freed %zu objects %zu -> %zu (%zu)\n", freed, n, m, n-m);
                
            }
            
            
            
        }
    }
    
    
    
    
    
    void mutate() {
        
        size_t allocated = 0;
        
        auto allocate = []() -> Object* {
            Object* infant = new Object();
            infant->color = BLACK;
            {
                std::unique_lock lock(mutex);
                infants.push_back(infant);
            }
            return infant;
        };
        
        std::vector<Object*> roots;
        std::vector<Object*> known;
        for (;;) {
            {
                std::unique_lock lock(mutex);
                if (worklist_request_roots) {
                    assert(roots.size() <= 1);
                    for (Object* ref : roots) {
                        if (ref->color == WHITE) {
                            ref->color = GRAY;
                            worklist.push_back(ref);
                        }
                    }
                    worklist_request_roots = false;
                    condition_variable.notify_all();
                }
            }
            
            // printf("allocated %zd\n", allocated);
            // do some work
            
            // We record objects we know about here as we explore the graph
            // They are kept live only by the graph, so when we mutate the
            // graph we must forget about them since some of them may have
            // been lost
            
            for (int i = 0; i != 1000; ++i) {

                if (roots.empty()) {
                    Object* ref = allocate(); // black until next roots export
                    roots.push_back(ref); // live by roots
                }
                
                if (known.empty()) {
                    known.push_back(roots.back());
                }

                if (known.size() < 1 + rand() % 10) {
                    int j = rand() % known.size();
                    int k = rand() % SLOTS;
                    // lock?
                    Object* ref = Read(known[j], k);
                    if (ref) {
                        known.push_back(ref);
                    }
                    continue;
                }

                int j = rand() % known.size();
                int k = rand() % SLOTS;
                int l = rand() % known.size();
                {
                    switch (rand() % 3) {
                        case 0:
                            Write(known[j], k, nullptr);
                            break;
                        case 1:
                            ++allocated;
                            Write(known[j], k, allocate());
                            break;
                        case 2:
                            Write(known[j], k, known[l]);
                            break;
                    }
                }
                // When we mutated the graph we could have lost our paths to
                // these elements so we have to forget them and rebuild our
                // knowledge
                known.clear();
                // This is only because we are randomly hacking at a random
                // graph, we don't have to start every operation from the root
                // in normal code
                
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






namespace gch {
    
    // The Garbage Collection Handbook
    
    enum Color {
        WHITE, // not marked
        GRAY,  // marked and in worklist
        BLACK, // marked and not in worklist
    };
    
    struct Object {
        Color color = WHITE;
        Object* next = nullptr; // All Objects as an implicit linked list
        Object* slots[8] = {};
    };
    
    bool isWhite(Object* object) {
        return object->color == WHITE;
    }
    
    bool isGray(Object* object) {
        return object->color == GRAY;
    }
    
    bool isBlack(Object* object) {
        return object->color == BLACK;
    }
    
    bool isMarked(Object* objects) {
        return !isWhite(objects);
    }
    
    Object* Read(Object* src, int i) {
        return src->slots[i];
    }
    
    void Write(Object* src, int i, Object* ref) {
        src->slots[i] = ref;
    }
    
    intptr_t count = 0;
    
    Object* head = nullptr; // all allocated objects chain from this
    
    Object* allocate() {
        Object* ref = new Object;
        assert(ref);
        ref->next = head;
        head = ref;
        ++count;
        return ref;
    }
    
    void free(Object* ref) {
        assert(ref);
        delete ref;
        --count;
    }
    
    std::vector<Object*> roots;
    std::vector<Object*> worklist;
    
    void revert(Object* ref) {
        assert(isBlack(ref));
        ref->color = GRAY;
        worklist.push_back(ref);
    }
    
    void shade(Object* ref) {
        if (isWhite(ref)) {
            ref->color = GRAY;
            worklist.push_back(ref);
        }
    }
    
    void scan(Object* ref) {
        assert(isBlack(ref));
        for (Object* child : ref->slots)
            if (child)
                shade(child);
    }
    
    void mark() {
        while (!worklist.empty()) {
            Object* ref = worklist.back();
            assert(isGray(ref));
            worklist.pop_back();
            ref->color = BLACK;
            for (Object* child : ref->slots)
                if (child)
                    shade(child);
        }
    }
    
    void markFromRoots() {
        assert(worklist.empty());
        for (Object* ref : roots) {
            if (ref && isWhite(ref)) {
                ref->color = GRAY;
                worklist.push_back(ref);
                mark(); // depth first traversal of roots
            }
        }
    }
    
    void sweep() {
        Object** next = &head;
        while (*next) {
            Object* object = *next;
            switch (object->color) {
                case WHITE:
                    *next = object->next;
                    free(object);
                    break;
                case GRAY:
                    abort();
                case BLACK:
                    object->color = WHITE;
                    next = &object->next;
            }
        }
    }
    
    void scanRoots() {
        for (Object* ref : roots) {
            if (ref && isWhite(ref)) {
                ref->color = GRAY;
                worklist.push_back(ref);
            }
        }
    }
    
    bool markSome() {
        if (worklist.empty()) {
            scanRoots();
            if (worklist.empty()) {
                sweep();
                return false;
            }
        }
        Object* ref = worklist.back();
        worklist.pop_back();
        ref->color = BLACK;
        scan(ref);
        return true;
    }
    
    void WriteSteele(Object* src, int i, Object* ref) {
        src->slots[i] = ref;
        if (isBlack(src))
            if (isWhite(ref))
                revert(src);
    }
    
    void WriteBoehm(Object* src, int i, Object* ref) {
        src->slots[i] = ref;
        if (isBlack(src))
            revert(src);
    }
    
    void WriteDijkstra(Object* src, int i, Object* ref) {
        src->slots[i] = ref;
        if (isBlack(src))
            shade(ref);
    }
    
    
    void mutate() {
        printf("mutating %zd", gch::count);
        for (;;) {
            if (!(rand() % 100))
                // yield
                break;
            if (!(rand() % 11)) {
                // erase root
                if (!roots.empty()) {
                    int i = rand() % roots.size();
                    roots.erase(roots.begin() + i);
                    continue;
                }
            }
            if (!(rand() % 10)) {
                // add root
                Object* ref = allocate();
                roots.push_back(ref);
                continue;
            }
            if (!roots.empty()) {
                // choose root
                Object* ref = roots[rand() % roots.size()];
                while (ref) {
                    // choose slot
                    int i = rand() % 8;
                    if (!(rand() % 8)) {
                        // clear slot
                        Write(ref, i, nullptr);
                        continue;
                    }
                    if (!(rand() % 4)) {
                        // overwrite slot
                        Write(ref, i, allocate());
                        continue;
                    }
                    // recurse slot
                    ref = Read(ref, i);
                }
            }
        }
        printf(" -> %zd\n", gch::count);
    }
    
    void collect() {
        printf("collecting %zd", gch::count);
        markFromRoots();
        sweep();
        printf(" -> %zd\n", gch::count);
    }
}


void exercise() {
    printf("start\n");
    gch::collect();
    for (int i = 0; i != 100; ++i) {
        gch::mutate();
        gch::collect();
    }
    printf("clearing roots\n");
    gch::roots.clear();
    gch::collect();
}

namespace gch2 {
    
    // The Garbage Collection Handbook
    
    // Concurrent collector with a single mutator thread and a single collector
    // thread
    
    enum Color : intptr_t {
        WHITE,
        GRAY,
        BLACK,
        
        YELLOW,
    };
    
    // we don't actually need gray or yellow?
    
    struct alignas(64) Object {
        intptr_t color; // only touched by GC thread (!)
        std::atomic<Object*> slots[7];
    };
    
    
    struct Epoch {
        std::vector<Object*> roots;
        std::vector<Object*> victims;
        std::vector<Object*> infants;
    };
    
    std::mutex epoch_queue_mutex;
    std::condition_variable epoch_queue_condition_variable;
    std::deque<Epoch> epoch_queue;
    
    void publish(Epoch epoch) {
        {
            std::unique_lock<std::mutex> lock(epoch_queue_mutex);
            epoch_queue.push_back(std::move(epoch));
        }
        epoch_queue_condition_variable.notify_one();
        
    }
    
    void mutate() {
        std::vector<Object*> roots;
        Epoch epoch;
        int allocated = 0;
        
        auto allocate = [&]() -> Object* {
            Object* object = new Object;
            epoch.infants.push_back(object);
            ++allocated;
            return object;
        };
        
        auto read = [&epoch](Object* object, int i) -> Object* {
            return object->slots[i].load(std::memory_order_relaxed);
        };
        
        auto write = [&epoch](Object* object, int i, Object* ref) {
            Object* victim = object->slots[i].exchange(ref, std::memory_order_release);
            if (victim)
                epoch.victims.push_back(victim);
        };
        
        for (;;) {
            // copy the true roots
            epoch.roots = roots;
            assert(epoch.victims.empty());
            assert(epoch.infants.empty());
            
            // now mutate the graph freely for a while, subject to the
            // following "taxes"
            // record overwritten pointers in victims
            // record new allocations in infants
            
            if (roots.empty()) {
                roots.push_back(allocate());
            }
            
            // this is not doing the hard case of erase and reinsert
            
            Object* object = nullptr;
            for (int i = 0; i != 1000; ++i) {
                if (!object)
                    object = roots.back();
                int j = rand() % 7;
                switch (rand() % 3) {
                    case 0: {
                        object = read(object, j);
                        break;
                    }
                    case 1: {
                        write(object, j, allocate());
                        break;
                    }
                    case 2: {
                        write(object, j, nullptr);
                        break;
                    }
                }
            }
            
            publish(std::move(epoch));
            printf("allocated %d\n", allocated);
        }
    }
    
    void collect() {
        
        Epoch epoch;
        std::vector<Object*> nodes; // all objects
        std::vector<Object*> worklist; // gray objects
        int freed = 0;
        
        for (;;) {
            {
                std::unique_lock<std::mutex> lock(epoch_queue_mutex);
                while (epoch_queue.empty()) {
                    epoch_queue_condition_variable.wait(lock);
                }
                epoch = std::move(epoch_queue.front());
                epoch_queue.pop_front();
            }
            // We now have
            // - the roots at the beginning of the epoch
            // - the original value of any pointers overwritten during the epoch
            // - the pointers allocated during the epoch
            
            // We already know all objects that survived the previous
            // collection, and the roots will be a subset of these
            
            // For multi-threading, we need to construct a consensus epoch
            // for which the beginning of every thread's epoch is before the
            // end of every thread's epoch; see crossbeam's epochs.  Basically
            // we don't advance until every thread has checked in on the epoch
            
            for (Object* object : epoch.roots) {
                if (object->color == WHITE) {
                    object->color = GRAY;
                    worklist.push_back(object);
                }
            }
            epoch.roots.clear();
            
            for (Object* object : epoch.victims) {
                if (object->color == WHITE) {
                    object->color = GRAY;
                    worklist.push_back(object);
                }
            }
            epoch.victims.clear();
            
            // trace
            while (!worklist.empty()) {
                Object* object = worklist.back();
                assert(object->color == GRAY);
                object->color = BLACK;
                worklist.pop_back();
                
                for (std::atomic<Object*>& slot : object->slots) {
                    // Acquire because we will dereference the pointer and it
                    // may have changed after the epoch closed
                    Object* p = slot.load(std::memory_order_acquire);
                    if (p && object->color == WHITE) {
                        // WHITE  -> needs to be traced
                        // GRAY   -> is already in the worklist
                        // BLACK  -> is already traced
                        // YELLOW -> can only hold yellow or alternatively reached objects
                        worklist.push_back(p);
                    }
                }
            }
            
            // sweep
            {
                auto a = nodes.begin();
                auto b = a;
                auto c = nodes.end();
                while (b != c) {
                    Object* object = *b;
                    switch (object->color) {
                            
                        case WHITE: {
                            free(object);
                            ++freed;
                            ++b;
                            break;
                        }
                            
                        case BLACK: {
                            object->color = WHITE;
                            *a = *b;
                            ++a;
                            ++b;
                            break;
                        }
                            
                        default: {
                            abort();
                        }
                            
                    };
                }
                nodes.erase(a, c);
            }
            
            printf("freed %d\n", freed);
            
            // promote
            for (Object* object : epoch.infants) {
                object->color = WHITE;
                nodes.push_back(object);
            }
            
        }
    }
    
    
    void exercise2() {
        std::thread collector(gch2::collect);
        std::thread mutator(gch2::mutate);
        mutator.join();
        collector.join();
    }
    
    
}
