//
//  gc.cpp
//  gch
//
//  Created by Antony Searle on 10/4/2024.
//

#include "gc.hpp"

namespace gc {
    
    struct ScanContextPrivate : ScanContext {
        
        void set_white(Color value) {
            _white = value;
        }
        
        void process() {
            while (!_stack.empty()) {
                Object const* object = _stack.top();
                _stack.pop();
                assert(object && object->color.load(RELAXED) == (_white ^ 2));
                object->scan(*this);
            }
        }
        
    };
    
    void enter() {
                
        // Create a new communication channel to the collector
        
        assert(local.channel == nullptr);
        Channel* channel = local.channel = new Channel;
        LOG("enters collectible state\n");
        {
            // Publish it to the collector's list of channels
            std::unique_lock lock{global.mutex};
            // channel->next = global.channels;
            global.mutators_entering.push_back(channel);
        }
        // Wake up the mutator
        global.condition_variable.notify_all();
    }
    
    void leave() {
        
        // Look up the communication channel
        
        Channel* channel = local.channel;
        LOG("mutator %p leaves collectible state\n", channel);
        bool pending;
        {
            std::unique_lock lock(channel->mutex);
            // Was there a handshake requested?
            pending = std::exchange(channel->pending, false);
            channel->abandoned = true;
            channel->dirty = local.dirty;
            LOG("publishing %s\n", local.dirty ? "dirty" : "clean");
            local.dirty = false;
            assert(channel->infants.empty());
            channel->infants.swap(local.allocations);
            assert(local.allocations.empty());
            channel->request_infants = false;
        }
        // wake up the collector if it was already waiting on a handshake
        if (pending)
            channel->condition_variable.notify_all();
        local.channel = nullptr;
        
    }
    
    void push(Object* object) {
        local.roots.push_back(object);
    }
    
    void pop() {
        local.roots.pop_back();
    }

    
    void handshake() {
        Channel* channel = local.channel;
        bool pending;
        {
            std::unique_lock lock(channel->mutex);
            pending = channel->pending;
            if (pending) {
                LOG("handshaking\n");
                
                // LOG("lifetime alloc %zu\n", allocated);
                
                // LOG("was WHITE=%lld BLACK=%lld ALLOC=%lld\n", local.WHITE, local.BLACK, local.ALLOC);
                
                //bool flipped_ALLOC = local.ALLOC != channels[index].configuration.ALLOC;
                
                // (Configuration&) local = channels[index].configuration;
                
                // LOG("becomes WHITE=%lld BLACK=%lld ALLOC=%lld\n", local.WHITE, local.BLACK, local.ALLOC);
                
                channel->dirty = local.dirty;
                LOG("publishing %s\n", local.dirty ? "dirty" : "clean");
                local.dirty = false;
                
                if (channel->request_infants) {
                    LOG("publishing ?? new allocations\n");
                    assert(channel->infants.empty());
                    channel->infants.swap(local.allocations);
                    assert(local.allocations.empty());
                }
                channel->request_infants = false;
                
                channel->pending = false;
                
            } else {
                // LOG("handshake not requested\n");
            }
        }
        
        if (pending) {
            LOG("notifies collector\n");
            channel->condition_variable.notify_all();
            for (Object* ref : local.roots)
                Object::shade(ref);
        }
    }

    
    void collect() {
        
        pthread_setname_np("C0");
                
        size_t freed = 0;
        
        std::vector<Object*> objects;
        deque<Object*> infants;
        
        
        Color white = global.white.load(RELAXED);
        Color black = white xor 2;

        ScanContextPrivate working;
        working.set_white(white);

        std::vector<Channel*> mutators, mutators2;
        
        auto accept_entrants = [&]() {
            assert(mutators2.empty());
            std::unique_lock lock{global.mutex};
            for (;;) {
                mutators.insert(mutators.end(),
                                global.mutators_entering.begin(),
                                global.mutators_entering.end());
                global.mutators_entering.clear();
                if (!mutators.empty() || !objects.empty())
                    return;
                LOG("collector has no work; waiting for new entrant\n");
                LOG("lifetime freed %zu\n", freed);
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
            
            LOG("collection begins\n");
            
            // Mutators allocate WHITE and mark GRAY
            // There are no BLACK objects

            assert(global.white.load(std::memory_order_relaxed) == white);
            assert(global.alloc.load(std::memory_order_relaxed) == white);
            global.alloc.store(black, std::memory_order_relaxed);
            
            LOG("begin transition to allocating BLACK\n");
            
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
                    LOG("ingesting objects from channel %p\n", channel);
                    std::size_t count = 0;
                    while (!infants.empty()) {
                        objects.push_back(infants.front());
                        infants.pop_front();
                        ++count;
                    }
                    LOG("ingested %zu objects from channel %p\n", count, channel);
                    assert(infants.empty());

                }
                 */
            }
            
            
            LOG("end transition to allocating BLACK\n");
            
            for (;;) {
                
                //LOG("enumerating %zu known objects\n",  objects.size());
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
                    LOG("scanning...\n");
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
                        } else {
                            abort();
                        }
                    }
                    LOG("        ...scanning found BLACK=%zu, GRAY=%zu, WHITE=%zu\n", blacks, grays, whites);
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
                        LOG("%p reports it was %s\n", channel, channel->dirty ? "dirty" : "clean");
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
                        LOG("channel %p reports it was %s\n", channel, channel->dirty ? "dirty" : "clean");
                        if (channel->dirty) {
                            local.dirty = std::exchange(channel->dirty, false);
                        }

                    }
                }
                 */
                
                
                if (!local.dirty)
                    break;
                
                local.dirty = false;
                
            }
            
            // Neither the collectors nor mutators marked any nodes GRAY since
            // the last handshake.  All remaining WHITE objects are unreachable.
            
            {
                LOG("sweeping...\n");
                std::size_t blacks = 0;
                std::size_t whites = 0;
                auto first = objects.begin();
                auto last = objects.end();
                for (; first != last;) {
                    Object* object = *first;
                    assert(object);
                    //object->_gc_print();
                    Color color = object->color.load(std::memory_order_relaxed);
                    if (color == black) {
                        ++blacks;
                        ++first;
                        //LOG("retains %p BLACK\n", object);
                    } else if (color == white) {
                        ++whites;
                        --last;
                        if (first != last) {
                            std::swap(*first, *last);
                        }
                        objects.pop_back();
                        // object->_gc_color.store(local.GREEN, std::memory_order_relaxed);
                        //LOG("frees %p WHITE -> GREEN\n", object);
                        delete object;
                        ++freed;
                    } else {
                        LOG("    ...sweeping sees %zd\n --- FATAL ---\n", color);
                        abort();
                    }
                    
                }
                LOG("    ...sweeping found BLACK=%zu, WHITE=%zu\n", blacks, whites);
                LOG("freed %zu\n", whites);
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
                LOG("requests handshake %zu\n", i);
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
            // Receive acknowledgements and recent allocations
            while (!mutators2.empty()) {
                Channel* channel = mutators2.back();
                mutators2.pop_back();
                bool abandoned = false;
                {
                    std::unique_lock lock{channel->mutex};
                    LOG("%p reports it was %s\n", channel, channel->dirty ? "dirty" : "clean");
                    while (!channel->abandoned && channel->pending)
                        channel->condition_variable.wait(lock);
                    if (!channel->abandoned) {
                        LOG("%p acknowledges recoloring\n", channel);
                        assert(infants.empty());
                    } else {
                        LOG("%p leaves\n", channel);
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
                    LOG("%p acknowledges recoloring\n", channel);
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
                        LOG("timed out waiting for %zu\n --- FATAL ---\n", i);
                        abort();
                    }
                }
                LOG("%zu acknowledges recoloring\n", i);
            }
            
            LOG("collection ends\n");
             */
            
        }
        
    }
    
    
    
    
    
    

} // namespace gc
