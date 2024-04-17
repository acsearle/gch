//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//


#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "gc.hpp"

namespace usr {
    
    using gc::LOG;

    template<typename T>
    struct TrieberStack : gc::Object {
        struct Node : gc::Object {
            gc::Atomic<gc::StrongPtr<Node>> next;
            T value;
            virtual ~Node() override = default;
            virtual void scan(gc::ScanContext& context) const override {
                context.push(this->next);
                context.push(this->value);
            }
        }; // struct Node
        
        gc::Atomic<gc::StrongPtr<Node>> head;
        
        virtual void scan(gc::ScanContext& context) const override {
            context.push(this->head);
        }
        
        void push(T value) {
            Node* desired = new Node;
            desired->value = std::move(value);
            Node* expected = head.load(gc::ACQUIRE);
            do {
                desired->next.ptr.store(expected, gc::RELAXED);
            } while (!head.compare_exchange_strong(expected,
                                                   desired,
                                                   gc::RELEASE,
                                                   gc::ACQUIRE));
        }
        
        bool pop(T& value) {
            Node* expected = head.load(gc::ACQUIRE);
            for (;;) {
                if (expected == nullptr)
                    return false;
                Node* desired = expected->next.load(gc::RELAXED);
                if (head.compare_exchange_strong(expected,
                                                 desired,
                                                 gc::RELAXED,
                                                 gc::ACQUIRE)) {
                    value = std::move(expected->value);
                    return true;
                }
            }
        }
                
    }; // TrieberStack<T>
    
    
    template<typename T>
    struct MichaelScottQueue : gc::Object {
        
        struct Node : gc::Object {
            
            gc::Atomic<gc::StrongPtr<Node>> next;
            T value;
            
            virtual ~Node() override = default;
            
            virtual void scan(gc::ScanContext& context) const override {
                context.push(this->next);
                context.push(this->value);
            }
            
        }; // struct Node
        
        gc::Atomic<gc::StrongPtr<Node>> head;
        gc::Atomic<gc::StrongPtr<Node>> tail;
        
        MichaelScottQueue() : MichaelScottQueue(new Node) {} // <-- default constructible T
        MichaelScottQueue(const MichaelScottQueue&) = delete;
        MichaelScottQueue(MichaelScottQueue&&) = delete;
        virtual ~MichaelScottQueue() = default;
        MichaelScottQueue& operator=(const MichaelScottQueue&) = delete;
        MichaelScottQueue& operator=(MichaelScottQueue&&) = delete;

        explicit MichaelScottQueue(Node* sentinel)
        : head(sentinel)
        , tail(sentinel) {
        }
        
        virtual void scan(gc::ScanContext& context) const override {
            context.push(this->head);
        }
        
        void push(T value) {
            // Make new node
            Node* a = new Node;
            assert(a);
            a->value = std::move(value);

            // Load the tail
            Node* b = tail.load(gc::ACQUIRE);
            for (;;) {
                assert(b);
                // If tail->next is null, install the new node
                Node* next = nullptr;
                if (b->next.compare_exchange_strong(next, a, gc::RELEASE, gc::ACQUIRE))
                    return;
                assert(next);
                // tail is lagging, advance it to the next value
                if (tail.compare_exchange_strong(b, next, gc::RELEASE, gc::ACQUIRE))
                    b = next;
                // Either way, b is now our last observation of tail
            }
        }
        
        bool pop(T& value) {
            Node* expected = head.load(gc::ACQUIRE);
            for (;;) {
                assert(expected);
                Node* next = expected->next.load(gc::ACQUIRE);
                if (next == nullptr)
                    // The queue contains only the sentinel node
                    return false;
                if (head.compare_exchange_strong(expected, next, gc::RELEASE, gc::ACQUIRE)) {
                    // We moved head forward
                    value = std::move(next->value);
                    return true;
                }
                // Else we loaded an unexpected value for head, try again
            }
        }
        
    }; // MichaelScottQueue<T>
    
    
    struct string_hash
    {
        using hash_type = std::hash<std::string_view>;
        using is_transparent = void;
        
        std::size_t operator()(const char* str) const        { return hash_type{}(str); }
        std::size_t operator()(std::string_view str) const   { return hash_type{}(str); }
        std::size_t operator()(std::string const& str) const { return hash_type{}(str); }
    };

    // String has no inner gc pointers, so it can go directly WHITE -> BLACK
    // without making any GRAY work to delay termination

    struct String : gc::Leaf {
        
        std::string inner;
        
        // weak hash map of strings
        
        static std::mutex mutex;
        static std::unordered_map<std::string, String*, string_hash, std::equal_to<>> map;
        
        [[nodiscard]] static String* from(std::string_view v) {
            std::unique_lock lock{mutex};
            String*& p = map[std::string(v)];
            if (p) {
                gc::shade(p);
            } else {
                p = new String;
                p->inner = v;
            }
            return p;
        }
        
        virtual bool sweep(gc::SweepContext& context) override {
            // Check for the common case of early out before we take the lock
            // and delay the mutator
            Color color = this->color.load(gc::RELAXED);
            assert(color != gc::GRAY);
            if (color == context.BLACK())
                return false;
            std::unique_lock lock{mutex};
            // A WHITE String is only shaded while this lock is held, so
            // the color test is now authoritative
            color = this->color.load(gc::RELAXED);
            assert(color != gc::GRAY);
            if (color == context.BLACK())
                return false;
            // The String is not strong-reachable, so we sweep it
            auto it = map.find(inner);
            assert(it != map.end());
            map.erase(it);
            delete this;
            return true;
        }
                
    };
    
    std::mutex String::mutex;
    std::unordered_map<std::string, String*, string_hash, std::equal_to<>> String::map;

        
    
    /*
    struct WeakDictionary : gc::Leaf {
        
        // TODO: needs to be concurrent!
        
        std::mutex mutex;
        std::unordered_map<std::string, gc::WeakPtr<String>, string_hash, std::equal_to<>> inner;
                
        String* lookup(std::string_view key) {
            std::unique_lock lock(mutex);
            auto a = inner.find(key);
            if (a == inner.end()) {
                auto [b, c] = inner.emplace(key, nullptr);
                assert(c);
                assert(b != inner.end());
                a = b;
            }
            String* value = a->second.try_lock();
            if (!value) {
                value = new String;
                a->second = value;
            }
            return value;
        }
        
        

        
    };
     */
    
    
    
    
    constexpr std::size_t THREADS = 3;
    constexpr std::size_t PUSHES = 10000000;

    void mutate(// TrieberStack<int>* trieber_stack,
                MichaelScottQueue<int>* michael_scott_queue,
                const std::size_t index,
                std::promise<std::vector<int>> result) {
    
        {
            char name[3] = "M0";
            name[1] += index;
            pthread_setname_np(name);
        }

        // gc::local.roots.push_back(trieber_stack);
        gc::local.roots.push_back(michael_scott_queue);

        gc::enter();
                        
        size_t allocated = 0;
                
        int k = (int) index;
        std::vector<int> integers;
    
                               
        for (;;) {

            gc::handshake();
                                    
            // do some graph work
            
            bool nonempty = true;
            for (int i = 0; i != 1000; ++i) {
                
                if ((k >= 10000000) || !(rand() % 2)) {
                    int j = -1;
                    //if((nonempty = trieber_stack->pop(j))) {
                    if((nonempty = michael_scott_queue->pop(j))) {
                        integers.push_back(j);
                    }
                } else {
                    // trieber_stack->push(k);
                    michael_scott_queue->push(k);
                    k += THREADS;
                    nonempty = true;
                }
                
            }
                                    
            if ((k >= PUSHES) && !nonempty) {
                LOG("%p no more work to do", gc::local.channel);
                LOG("lifetime alloc %zu", allocated);
                result.set_value(std::move(integers));
                gc::leave();
                return;
            }
            
        }
    }
        
    void exercise() {
        
        gc::enter();

        LOG("creates a concurrent stack");
        
        LOG("spawns collector thread");
        std::thread collector{gc::collect};

        // auto p = new TrieberStack<int>; // <-- safe until we handshake or leave
        auto p = new MichaelScottQueue<int>; // <-- safe until we handshake or leave
        gc::local.roots.push_back(p);
        
        std::stack<std::future<std::vector<int>>> futures;
        std::stack<std::thread> mutators;
        for (std::size_t i = 0; i != THREADS; ++i) {
            LOG("spawns mutator thread");
            std::promise<std::vector<int>> promise;
            futures.push(promise.get_future());
            mutators.emplace(mutate, p, i, std::move(promise));
        }

        // If we leave now, the stack may be collected before the mutators
        // start.
        
        // TODO: Think about what it means to leave collectible state with
        // nonempty local.roots
        
        // TODO: Think about global.roots.  Does the collector shade them?
        
        // Wait for results.  Wake up regularly to provide handshakes.  This is
        // ugly, but I think it's just because testing forces strange patterns.
                        
        std::vector<int> integers;
        while (!futures.empty()) {
            while (futures.top().wait_for(std::chrono::milliseconds{20})
                   == std::future_status::timeout)
                gc::handshake();
            std::vector<int> result = futures.top().get();
            futures.pop();
            LOG("received %zu integers", result.size());
            integers.insert(integers.end(), result.begin(), result.end());
        }

        p = nullptr;
        gc::leave();
        
        LOG("received %zu pops expected %zu", integers.size(), PUSHES);
        
        LOG("sorting %zu results", integers.size());
        std::sort(integers.begin(), integers.end());
        
        for (int i = 0;; ++i) {
            if (i == integers.size()) {
                LOG("all integers popped exactly once");
                break;
            }
            if (i != integers[i]) {
                LOG("first error at [%d] != %d", i, integers[i]);
                break;
            }
        }

        while (!mutators.empty()) {
            mutators.top().join();
            LOG("joined a mutator thread");
            mutators.pop();
        }
        
        collector.join();
        LOG("joined the collector thread");
    }
    
    
    void exercise2() {
        std::thread collector{gc::collect};
        gc::enter();
        {
            for (int i = 0; i != 100; ++i) {
                gc::handshake();
                
                for (int j = 0; j != 10; ++j) {
                    char ch = (rand() % 26) + 'a';
                    (void) String::from(std::string_view(&ch, 1));
                }
                
            }
        }
        gc::leave();
        collector.join();
    }
    
}

int main(int argc, const char * argv[]) {
    pthread_setname_np("MAIN");
    srand(79);
    // usr::exercise();
    usr::exercise2();
}



