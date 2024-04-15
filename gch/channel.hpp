//
//  channel.hpp
//  gch
//
//  Created by Antony Searle on 13/4/2024.
//

#ifndef channel_hpp
#define channel_hpp

#include <deque>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <semaphore>

namespace gc {
    
#define with(X) if (X; true)
    
    template<typename T>
    struct UnboundedChannel {
        
        std::mutex mutex;
        std::condition_variable condition_variable;
        std::deque<T> inner;
        
        void send(T value) {
            with (std::unique_lock lock{mutex})
                inner.push_back(std::move(value));
            condition_variable.notify_one();
        }
        
        T recv() {
            T result;
            with (std::unique_lock lock{mutex}) {
                while (inner.empty())
                    condition_variable.wait(lock);
                result = std::move(inner.front());
                inner.pop_front();
            }
            return result;
        }
        
        std::optional<T> try_recv() {
            std::optional<T> result;
            with (std::unique_lock lock{mutex}) {
                if (!inner.empty()) {
                    result = std::move(inner.front());
                    inner.pop_front();
                }
            }
            return result;
        }
        
    }; // struct UnboundedChannel<T>
    
    template<typename T, std::ptrdiff_t CAPACITY = (std::numeric_limits<std::ptrdiff_t>::max() >> 1)>
    struct Channel {

        std::mutex mutex;
        std::condition_variable recv_condition_variable;
        std::counting_semaphore<> semaphore{CAPACITY};
        std::deque<T> inner;
        
        // CAPACITY does not bound the elements in the queue; instead it bounds
        // the number of unpaired sends, which is slightly different.  The queue
        // size can exceed the capacity, but the sends that caused it to do
        // so will block until the queue is drained.
        
        // TODO: subtle difference between queue ordering and concurrency
        // fairness?
        //
        // Two threads block in recv.  An item is pushed, and one thread is
        // notified and consumes it.  It's not ncessarily the recv that has
        // been waiting the longest, if that is well defined.
        
        void send(T value) {
            with (std::unique_lock lock{mutex}) {
                inner.push_back(std::move(value));
            }
            recv_condition_variable.notify_one();
            semaphore.acquire();
        }
        
        T recv() {
            T result;
            with (std::unique_lock lock{mutex}) {
                while (inner.empty())
                    recv_condition_variable.wait(lock);
                result = std::move(inner.front());
                inner.pop_front();
            }
            semaphore.release();
            return result;
        }
        
        std::optional<T> try_send(T value) {
            std::optional<T> result;
            if (semaphore.try_acquire()) {
                with (std::unique_lock lock{mutex})
                    inner.push_back(std::move(value));
                recv_condition_variable.notify_one();
            } else {
                result = std::move(value);
            }
            return result;
        }
        
        std::optional<T> try_recv() {
            std::optional<T> result;
            with (std::unique_lock lock{mutex}) {
                if (!inner.empty()) {
                    result = std::move(inner.front());
                    inner.pop_front();
                }
            }
            if (result)
                semaphore.release();
            return result;
        }

    };

} // namespace gc

#endif /* channel_hpp */
