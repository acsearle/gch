//
//  gc.cpp
//  gch
//
//  Created by Antony Searle on 10/4/2024.
//

#include <thread>
#include "gc.hpp"

namespace gc {
    
    void LOG(const char* format, ...) {
        char buffer[256];
        pthread_getname_np(pthread_self(), buffer + 240, 16);
        char dirty = 'c' + local.dirty;
        int n = snprintf(buffer, 240, "%s/%c: ", buffer + 240, dirty);
        va_list args;
        va_start(args, format);
        vsnprintf(buffer + n, 256 - n, format, args);
        va_end(args);
        puts(buffer);
    }
    
    struct ScanContextPrivate : ScanContext {
        
        void set_white(Color value) {
            _white = value;
        }
        
        void process() {
            while (!_stack.empty()) {
                Object const* object = _stack.top();
                _stack.pop();
                assert(object && object->color.load(RELAXED) == (_white ^ 1));
                object->scan(*this);
            }
        }
        
    };
    
    void enter() {

        assert(local.depth >= 0);
        if (local.depth++) {
            return;
        }
                
        // Create a new communication channel to the collector
        
        assert(local.channel == nullptr);
        Channel* channel = local.channel = new Channel;
        LOG("enters collectible state");
        {
            // Publish it to the collector's list of channels
            std::unique_lock lock{global.mutex};
            // channel->next = global.channels;
            global.entrants.push_back(channel);
        }
        // Wake up the mutator
        global.condition_variable.notify_all();
    }
    
    void leave() {
        
        assert(local.depth >= 0);
        if (--local.depth) {
            return;
        }
        
        // Look up the communication channel
        
        Channel* channel = local.channel;
        assert(channel);
        bool pending;
        {
            std::unique_lock lock(channel->mutex);
            LOG("leaves collectible state", channel);
            // Was there a handshake requested?
            pending = std::exchange(channel->pending, false);
            channel->abandoned = true;
            channel->dirty = local.dirty;
            LOG("%spublishes %s, orphans", pending ? "handshakes, " : "", local.dirty ? "dirty" : "clean");
            local.dirty = false;
            if (channel->infants.empty()) {
                channel->infants.swap(local.allocations);
                assert(local.allocations.empty());
            } else {
                // we are leaving after we have acknowledged a handshake, but
                // before the collector has taken our infants
                while (!local.allocations.empty()) {
                    Object* p = local.allocations.front();
                    local.allocations.pop_front();
                    channel->infants.push_back(p);
                }
            }
            channel->request_infants = false;
        }
        // wake up the collector if it was already waiting on a handshake
        if (pending) {
            LOG("notifies collector");
            channel->condition_variable.notify_all();
        }
        local.channel = nullptr;
        
    }
    
    
    void handshake() {
        Channel* channel = local.channel;
        bool pending;
        {
            std::unique_lock lock(channel->mutex);
            pending = channel->pending;
            if (pending) {
                LOG("handshaking");
                
                // LOG("lifetime alloc %zu", allocated);
                
                // LOG("was WHITE=%lld BLACK=%lld ALLOC=%lld", local.WHITE, local.BLACK, local.ALLOC);
                
                //bool flipped_ALLOC = local.ALLOC != channels[index].configuration.ALLOC;
                
                // (Configuration&) local = channels[index].configuration;
                
                // LOG("becomes WHITE=%lld BLACK=%lld ALLOC=%lld", local.WHITE, local.BLACK, local.ALLOC);
                
                channel->dirty = local.dirty;
                LOG("publishing %s", local.dirty ? "dirty" : "clean");
                local.dirty = false;
                
                if (channel->request_infants) {
                    LOG("publishing ?? new allocations");
                    assert(channel->infants.empty());
                    channel->infants.swap(local.allocations);
                    assert(local.allocations.empty());
                }
                channel->request_infants = false;
                
                channel->pending = false;
                
            } else {
                // LOG("handshake not requested");
            }
        }
        
        if (pending) {
            LOG("notifies collector");
            channel->condition_variable.notify_all();
            for (Object* ref : local.roots)
                shade(ref);
        }
    }
    
    
    void collect() {
        
        pthread_setname_np("C0");
        
        gc::enter();
        
        size_t freed = 0;
        
        std::vector<Object*> objects;
        deque<Object*> infants;
        
        
        Color white = global.white.load(RELAXED);
        Color black = white ^ 1;

        ScanContextPrivate working;
        working.set_white(white);

        std::vector<Channel*> mutators, mutators2;
        
        auto accept_entrants = [&]() {
            assert(mutators2.empty());
            std::unique_lock lock{global.mutex};
            for (;;) {
                mutators.insert(mutators.end(),
                                global.entrants.begin(),
                                global.entrants.end());
                global.entrants.clear();
                LOG("mutators.size() -> %zu", mutators.size());
                LOG("objects.size() -> %zu", objects.size());
                if (!mutators.empty() || !objects.empty())
                    return;
                LOG("collector has no work; waiting for new entrant");
                LOG("lifetime freed %zu", freed);
                global.condition_variable.wait(lock);
            }
        };
        
        auto adopt_infants = [&]() {
            // TODO: O(1) splice not O(N) copy
            while (!infants.empty()) {
                objects.push_back(infants.front());
                infants.pop_front();
            }
        };
        
        for (;;) {
            
            LOG("collection begins");
            
            // Mutators allocate WHITE and mark GRAY
            // There are no BLACK objects

            assert(global.white.load(std::memory_order_relaxed) == white);
            assert(global.alloc.load(std::memory_order_relaxed) == white);
            global.alloc.store(black, std::memory_order_relaxed);
            
            LOG("begin transition to allocating BLACK");
            
            // handshake to ensure all mutators have seen the write to alloc
            
            {
                accept_entrants();
                {
                    // Request handshake and handover of infants
                    assert(mutators2.empty());
                    while (!mutators.empty()) {
                        Channel* channel = mutators.back();
                        mutators.pop_back();
                        bool abandoned = false;
                        {
                            std::unique_lock lock{channel->mutex};
                            assert(!channel->pending); // handshake fumbled?!
                            if (!channel->abandoned) {
                                channel->pending = true;
                                channel->request_infants = true;
                            } else {
                                abandoned = true;
                                assert(infants.empty());
                                infants.swap(channel->infants);
                            }
                        }
                        if (!abandoned) {
                            mutators2.push_back(channel);
                        } else {
                            delete channel;
                            adopt_infants();
                        }
                    }

                    // scan the global roots
                    for (Object* ref : global.roots)
                        shade(ref);

                    // handshake ourself!
                    gc::handshake();
                    
                    // Receive acknowledgements and recent allocations
                    while (!mutators2.empty()) {
                        Channel* channel = mutators2.back();
                        mutators2.pop_back();
                        bool abandoned = false;
                        {
                            std::unique_lock lock{channel->mutex};
                            while (!channel->abandoned && channel->pending)
                                channel->condition_variable.wait(lock);
                            if (channel->abandoned)
                                abandoned = true;
                            channel->dirty = false;
                            assert(infants.empty());
                            infants.swap(channel->infants);
                        }
                        adopt_infants();
                        if (!abandoned) {
                            mutators.push_back(channel);
                        } else {
                            delete channel;
                        }
                    }
                                        
                    // Mutators that entered before global.mutex.unlock have
                    // been handshaked
                    
                    // Mutators that entered after global.mutex.unlock
                    // synchronized with that unlock
                    
                    // Thus all mutators have seen
                    //     alloc = black;
                    
                    // All objects not in our list of objects were allocated
                    // black after alloc = black
                    
                    // The list of objects consists of
                    // - white objects
                    // - gray objects produced by the write barrier shading
                    //   recently white objects
                    // - black objects allocated between the alloc color change
                    //   and the handoff of objects
                }
                
                
                /*
                Channel* head = nullptr;
                {
                    std::unique_lock lock{global.mutex};
                    while (!(head = global.channels)) {
                        LOG("waiting for mutators to opt-in");
                        global.condition_variable.wait(lock);
                    }
                }
                for (Channel* channel = head; channel; channel = channel->next) {
                    std::unique_lock lock{channel->mutex};
                    if (!channel->abandoned) {
                        channel->pending = true;
                        channel->request_infants = true;
                    }
                }
                for (Channel* channel = head; channel; channel = channel->next) {
                    {
                        std::unique_lock lock{channel->mutex};
                        while (channel->pending)
                            channel->condition_variable.wait(lock);
                        channel->dirty = false;
                        assert(infants.empty());
                        infants.swap(channel->infants);
                    }
                    LOG("ingesting objects from channel %p", channel);
                    std::size_t count = 0;
                    while (!infants.empty()) {
                        objects.push_back(infants.front());
                        infants.pop_front();
                        ++count;
                    }
                    LOG("ingested %zu objects from channel %p", count, channel);
                    assert(infants.empty());

                }
                 */
            }
            
            
            LOG("end transition to allocating BLACK");
            
            for (;;) {
                
                //LOG("enumerating %zu known objects",  objects.size());
                //for (Object* object : objects) {
                //    object->_gc_print();
                //}
                
                // assert(!local.dirty);
                // TODO: local.dirty means, the collector made a GRAY object,
                // or a mutator has reported making a GRAY object, and we must
                // scan for GRAY objects.
                //
                // It is possible to get here and not be dirty if the mutators
                // made no GRAY objects since we flipped BLACK to WHITE, so
                // we could skip directly to the possible-termination handshake.
                // But we did just ask them to scan their roots, so that is
                // a very niche case.
                
                do {
                    local.dirty = false; // <-- reset local dirty since we are now going to find any of the dirt that flipped this flag
                    std::size_t blacks = 0;
                    std::size_t grays = 0;
                    std::size_t whites = 0;
                    std::size_t reds = 0;
                    LOG("scanning...");
                    for (Object* object : objects) {
                        //object->_gc_print();
                        assert(object);
                        Color expected = GRAY;
                        object->color.compare_exchange_strong(expected,
                                                              black,
                                                              std::memory_order_relaxed,
                                                              std::memory_order_relaxed);
                        if (expected == black) {
                            // TODO: there's no point discovering the same black
                            // objects every sweep, we should move them to another
                            // list
                            ++blacks;
                        } else if (expected == GRAY) {
                            ++grays;
                            object->scan(working);
                            working.process();
                        } else if (expected == white) {
                            ++whites;
                        } else if (expected == RED) {
                            ++reds;
                        } else {
                            abort();
                        }
                    }
                    LOG("        ...scanning found BLACK=%zu, GRAY=%zu, WHITE=%zu, RED=%zu", blacks, grays, whites, reds);
                } while (local.dirty);
                
                assert(!local.dirty);
                
                // the collector has traced everything it knows about
                
                // handshake to see if the mutators have shaded any objects
                // GRAY since the last handshake
                
                // initiate handshakes
                
                /*
                Channel* head = nullptr;
                {
                    std::unique_lock lock{global.mutex};
                    head = global.channels;
                }
                 */
                
                accept_entrants();
                /*
                for (Channel* channel = head; channel; channel = channel->next) {
                    std::unique_lock lock{channel->mutex};
                    if (!channel-> abandoned)
                        channel->pending = true;
                }
                 */
                // request handshake
                                
                assert(mutators2.empty());
                while (!mutators.empty()) {
                    Channel* channel = mutators.back();
                    mutators.pop_back();
                    bool abandoned = false;
                    {
                        std::unique_lock lock{channel->mutex};
                        assert(!channel->pending);
                        if (!channel->abandoned) {
                            channel->pending = true;
                        } else {
                            abandoned = true;
                            if (channel->dirty) {
                                local.dirty = true;
                                channel->dirty = false;
                            }
                            assert(infants.empty());
                            infants.swap(channel->infants);
                        }
                    }
                    if (!abandoned) {
                        mutators2.push_back(channel);
                    } else {
                        delete channel;
                        // all of these orphans were created after the mutator
                        // observed alloc = black, so they should all be black,
                        // and we should put them directly into some place we
                        // won't keep rescanning
                        adopt_infants();
                    }
                }
                // autoshake
                gc::handshake();
                // Receive acknowledgements and recent allocations
                while (!mutators2.empty()) {
                    Channel* channel = mutators2.back();
                    mutators2.pop_back();
                    bool abandoned = false;
                    {
                        std::unique_lock lock{channel->mutex};
                        while (!channel->abandoned && channel->pending)
                            channel->condition_variable.wait(lock);
                        if (channel->abandoned) {
                            abandoned = true;
                            assert(infants.empty());
                            infants.swap(channel->infants);
                        }
                        LOG("%p reports it was %s", channel, channel->dirty ? "dirty" : "clean");
                        if (channel->dirty) {
                            local.dirty = true;
                            channel->dirty = false;
                        }
                    }
                    if (!abandoned) {
                        mutators.push_back(channel);
                    } else {
                        delete channel;
                        adopt_infants();
                    }
                }
                /*
                for (Channel* channel = head; channel; channel = channel->next) {
                    {
                        std::unique_lock lock{channel->mutex};
                        while (channel->pending)
                            channel->condition_variable.wait(lock);
                        LOG("channel %p reports it was %s", channel, channel->dirty ? "dirty" : "clean");
                        if (channel->dirty) {
                            local.dirty = std::exchange(channel->dirty, false);
                        }

                    }
                }
                 */
                
                
                if (!local.dirty) {
                    break;
                }
                
                local.dirty = false;
                
            }
            
            // Neither the collectors nor mutators marked any nodes GRAY since
            // the last handshake.
            //
            // No weak pointers were upgraded during the handshakes.
            //
            // All remaining WHITE objects are unreachable.
            //
            // All weak_pointer upgrades will fail
            
            {
                LOG("sweeping...");
                std::size_t blacks = 0;
                std::size_t whites = 0;
                auto first = objects.begin();
                auto last = objects.end();
                SweepContext context;
                context._white = white;
                for (; first != last;) {
                    Object* object = *first;
                    assert(object);
                    if (object->sweep(context)) {
                        ++whites;
                        --last;
                        if (first != last) {
                            std::swap(*first, *last);
                        }
                        objects.pop_back();
                        ++freed;
                    } else {
                        ++blacks;
                        ++first;
                    }                    
                }
                LOG("    ...sweeping found BLACK=%zu, WHITE=%zu", blacks, whites);
                LOG("freed %zu", whites);
            }
            
            // Only BLACK objects exist
            
            // Reinterpret the colors
            
            // std::swap(local.BLACK, local.WHITE);
            // std::swap(local.color_names[local.BLACK], local.color_names[local.WHITE]);
            std::swap(white, black);
            global.white.store(white, std::memory_order_relaxed);
            working.set_white(white);
            
            /*
            
            // Publish the reinterpretation
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                LOG("requests handshake %zu", i);
                channels[i].pending = true;
                // channels[i].configuration = local;
            }
            */
            
            /*
            Channel* head = nullptr;
            {
                std::unique_lock lock{global.mutex};
                head = global.channels;
            }
             */
            accept_entrants();
            assert(mutators2.empty());
            while (!mutators.empty()) {
                Channel* channel = mutators.back();
                mutators.pop_back();
                bool abandoned = false;
                {
                    std::unique_lock lock{channel->mutex};
                    assert(!channel->pending);
                    if (!channel->abandoned) {
                        channel->pending = true;
                        assert(channel->infants.empty());
                    } else {
                        abandoned = true;
                        assert(infants.empty());
                        infants.swap(channel->infants);
                    }
                }
                if (!abandoned) {
                    mutators2.push_back(channel);
                } else {
                    delete channel;
                    // These orphans were all created with the same color value,
                    // which has changed meaning from black to white since the
                    // last handshake.  Some of them may have already been
                    // turned from white to gray by the write barrier.  None
                    // of them should be black
                    adopt_infants();
                }
            }
            // autoshake
            gc::handshake();
            // Receive acknowledgements and recent allocations
            while (!mutators2.empty()) {
                Channel* channel = mutators2.back();
                mutators2.pop_back();
                bool abandoned = false;
                {
                    std::unique_lock lock{channel->mutex};
                    LOG("%p reports it was %s", channel, channel->dirty ? "dirty" : "clean");
                    while (!channel->abandoned && channel->pending)
                        channel->condition_variable.wait(lock);
                    if (!channel->abandoned) {
                        LOG("%p acknowledges recoloring", channel);
                        assert(infants.empty());
                    } else {
                        LOG("%p leaves", channel);
                        abandoned = true;
                        assert(infants.empty());
                        infants.swap(channel->infants);
                    }
                    if (channel->dirty) {
                        local.dirty = true;
                        channel->dirty = false;
                    }
                }
                if (!abandoned) {
                    mutators.push_back(channel);
                } else {
                    delete channel;
                    adopt_infants();
                }
            }
            /*
            for (Channel* channel = head; channel; channel = channel->next) {
                std::unique_lock lock{channel->mutex};
                if (!channel->abandoned)
                    channel->pending = true;
            }
             */
            /*
            for (Channel* channel = head; channel; channel = channel->next) {
                {
                    std::unique_lock lock{channel->mutex};
                    while (channel->pending)
                        channel->condition_variable.wait(lock);
                    LOG("%p acknowledges recoloring", channel);
                }
            }
             */
            
            /*
            // receive handshakes
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                while (channels[i].pending) {
                    std::cv_status status
                    = channels[i].condition_variable.wait_for(lock,
                                                              std::chrono::seconds(1));
                    if (status == std::cv_status::timeout) {
                        LOG("timed out waiting for %zu\n --- FATAL ---", i);
                        abort();
                    }
                }
                LOG("%zu acknowledges recoloring", i);
            }
            
            LOG("collection ends");
             */
            
        }
        
        leave();
        
    }
    
    
    
    
    
    

} // namespace gc
