//
//  gc.cpp
//  gch
//
//  Created by Antony Searle on 10/4/2024.
//

#include "gc.hpp"

namespace gc {
    
    void enter() {
                
        // Create a new communication channel to the collector
        
        assert(local.channel == nullptr);
        Channel* channel = local.channel = new Channel;
        {
            // Publish it to the collector's list of channels
            std::unique_lock lock{global.mutex};
            channel->next = global.channels;
            global.channels = channel;
        }
        // Wake up the mutator
        global.condition_variable.notify_all();
    }
    
    void leave() {
        
        // Look up the communication channel
        
        Channel* channel = local.channel;
        bool pending;
        {
            std::unique_lock lock(channel->mutex);
            // Was there a handshake requested?
            pending = std::exchange(channel->pending, false);
            channel->abandoned = true;
            channel->dirty = local.dirty;
            LOG("publishing %s\n", local.dirty ? "dirty" : "clean");
            local.dirty = false;
            LOG("publishing ?? new allocations\n");
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
                
        size_t freed = 0;
        
        std::vector<Object*> objects;
        deque<Object*> infants;
        
        std::stack<Object*> working;
        
        Color white = global.white.load(RELAXED);
        Color black = white xor 2;

        
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
            
            // assert(local.ALLOC == local.WHITE);
            global.alloc.store(black, std::memory_order_relaxed);
            
            LOG("begin transition to allocating BLACK\n");
            
            // initiate handshakes
            /*
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                LOG("requests handshake %zu\n", i);
                channels[i].pending = true;
                channels[i].request_infants = true;
                //channels[i].configuration = local;
            }
             */
            {
                Channel** clobber = nullptr;
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
                
            }
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
                assert(infants.empty());
                infants.swap(channels[i].infants);
                lock.unlock();
                LOG("ingesting objects from %zu\n", i);
                std::size_t count = 0;
                while (!infants.empty()) {
                    objects.push_back(infants.front());
                    infants.pop_front();
                    ++count;
                }
                LOG("ingested %zu objects from %zu\n", count, i);
                assert(infants.empty());
            }
             */
            
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
                        Color expected = GRAY;
                        object->color.compare_exchange_strong(expected,
                                                              black,
                                                              std::memory_order_relaxed,
                                                              std::memory_order_relaxed);
                        if (expected == black) {
                            ++blacks;
                        } else if (expected == GRAY) {
                            ++grays;
                            object->scan(working); // <-- will set local.dirty when it marks a node GRAY
                        } else if (expected == white) {
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
                /*
                for (std::size_t i = 0; i != THREADS; ++i) {
                    std::unique_lock lock(channels[i].mutex);
                    LOG("requests handshake %zu\n", i);
                    channels[i].pending = true;
                }
                 */
                Channel* head = nullptr;
                {
                    std::unique_lock lock{global.mutex};
                    head = global.channels;
                }
                for (Channel* channel = head; channel; channel = channel->next) {
                    std::unique_lock lock{channel->mutex};
                    if (!channel-> abandoned)
                        channel->pending = true;
                }
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
                    LOG("%zu reports it was %s\n", i, channels[i].dirty ? "dirty" : "clean");
                    if (channels[i].dirty)
                        local.dirty = true;
                }
                 */
                
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
                        LOG("sweep sees %zd\n --- FATAL ---\n", color);
                        abort();
                    }
                    
                }
                LOG("swept %zu, 0, %zu, 0\n", blacks, whites);
                LOG("freed %zu\n", whites);
                LOG("lifetime freed %zu\n", freed);
            }
            
            // Only BLACK objects exist
            
            // Reinterpret the colors
            
            // std::swap(local.BLACK, local.WHITE);
            // std::swap(local.color_names[local.BLACK], local.color_names[local.WHITE]);
            std::swap(white, black);
            global.white.store(white, std::memory_order_relaxed);
            
            /*
            
            // Publish the reinterpretation
            for (std::size_t i = 0; i != THREADS; ++i) {
                std::unique_lock lock(channels[i].mutex);
                LOG("requests handshake %zu\n", i);
                channels[i].pending = true;
                // channels[i].configuration = local;
            }
            */
            
            Channel* head = nullptr;
            {
                std::unique_lock lock{global.mutex};
                head = global.channels;
            }
            for (Channel* channel = head; channel; channel = channel->next) {
                std::unique_lock lock{channel->mutex};
                if (!channel->abandoned)
                    channel->pending = true;
            }
            for (Channel* channel = head; channel; channel = channel->next) {
                {
                    std::unique_lock lock{channel->mutex};
                    while (channel->pending)
                        channel->condition_variable.wait(lock);
                    LOG("%p acknowledges recoloring\n", channel);
                }
            }
            
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
