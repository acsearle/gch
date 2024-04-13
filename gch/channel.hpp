//
//  channel.hpp
//  gch
//
//  Created by Antony Searle on 13/4/2024.
//

#ifndef channel_hpp
#define channel_hpp

#include <mutex>
#include <condition_variable>
#include <optional>

namespace gc {
    
    template<typename T>
    struct Channel {

        std::mutex mutex;
        std::condition_variable cv;
        std::optional<T> inner;
        
        void send(T value) {
            std::unique_lock lock{mutex};
            while (inner.has_value())
                cv.wait(lock);
            inner = std::move(value);
            lock.unlock();
            cv.notify_all();
        }
        
        T recv() {
            std::unique_lock lock{mutex};
            while (!inner.has_value())
                cv.wait(lock);
            T result(std::move(inner.value()));
            inner.reset();
            lock.unlock();
            cv.notify_all();
            return result;
        }
        
        std::optional<T> try_recv() {
            std::unique_lock lock{mutex};
            std::optional result(std::move(inner));
            inner.reset();
            lock.unlock();
            cv.notify_all();
            return result;
        }
        
    };


    
    
    
} // namespace gc

#endif /* channel_hpp */
