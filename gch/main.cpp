//
//  main.cpp
//  gch
//
//  Created by Antony Searle on 3/4/2024.
//


#include <future>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "gc.hpp"
#include "string.hpp"
#include "ctrie.hpp"

namespace usr {
    
    using namespace gc;

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
                // context.push(this->value);
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
    
   
    
    struct Dictionary : gc::Object {
        
        mutable std::mutex mutex;
        std::unordered_map<String*, gc::Object*, typename String::Hash, typename String::KeyEqual> map;
        

        // TODO: find must be fast
        gc::Object* load(String* key) const {
            std::unique_lock lock{mutex};
            auto it = map.find(key);
            return it != map.end() ? it->second : nullptr;
        }

        // TODO: set must be fast-ish
        Object* exchange(String* key, Object* value) {
            // Write barrier requires we shade the value, any old value,
            // and any new key
            gc::shade(value);
            std::unique_lock lock{mutex};
            auto it = map.find(key);
            if (it != map.end()) {
                gc::shade(value);
                Object* old = it->second;
                gc::shade(old);
                it->second = value;
                return old;
            } else {
                gc::shade(key);
                map.emplace(key, value); // <-- O(N) resize
                return nullptr;
            }
        }

        // scanning can be slow but it must be authoritative
        virtual void scan(gc::ScanContext& context) const override {
            std::unique_lock lock{mutex};
            for (const auto& kv : map) { // <-- O(N) iteration
                context.push(kv.first);
                context.push(kv.second);
            }
        }
        
    };
    
    
    
    
    
    
    
    
    constexpr std::size_t THREADS = 3;
    constexpr std::size_t PUSHES = 10000000;

    void mutate(// TrieberStack<int>* trieber_stack,
                MichaelScottQueue<int>* michael_scott_queue,
                Ctrie* ctrie,
                const std::size_t index,
                std::promise<std::vector<int>> result) {
    
        {
            char name[3] = "M0";
            name[1] += index;
            pthread_setname_np(name);
        }

        // gc::local.roots.push_back(trieber_stack);
        gc::local.roots.push_back(michael_scott_queue);
        gc::local.roots.push_back(ctrie);

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
        
        auto c = new Ctrie;
        gc::local.roots.push_back(c);

        std::stack<std::future<std::vector<int>>> futures;
        std::stack<std::thread> mutators;
        for (std::size_t i = 0; i != THREADS; ++i) {
            LOG("spawns mutator thread");
            std::promise<std::vector<int>> promise;
            futures.push(promise.get_future());
            mutators.emplace(mutate, p, c, i, std::move(promise));
        }

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
        auto c = new Ctrie;
        gc::local.roots.push_back(c);
        {
            for (int i = 0; i != 1000000; ++i) {
                gc::handshake();
                
                for (int j = 0; j != 100; ++j) {
                    {
                        char ch = (rand() % 26) + 'a';
                        _ctrie::Query q;
                        q.view = std::string_view(&ch, 1);
                        q.hash = std::hash<std::string_view>{}(q.view);
                        // String* s = String::from(std::string_view(&ch, 1));
                        printf("Doing '%c'\n", ch);
                        auto* t1 = c->lookup(q);
                        assert(!t1 || t1->view() == q.view);
                        c->debug();
                        c->emplace(q);
                        c->debug();
                        auto* t2 = c->lookup(q);
                        assert(!t2 || t2->view() == q.view);
                        assert(t2 && (!t1 || t1 == t2));
                    }
                    {
                        char ch = (rand() % 26) + 'a';
                        //String* s = String::from(std::string_view(&ch, 1));
                        _ctrie::Query q;
                        q.view = std::string_view(&ch, 1);
                        q.hash = std::hash<std::string_view>{}(q.view);
                        const auto* t1 = c->lookup(q);
                        assert(!t1 || t1->view() == q.view);
                        if (t1) {
                            const auto* t2 = c->remove(t1);
                            assert(t1 == t2);
                            const auto* t3 = c->lookup(q);
                            assert(!t3);
                        }
                    }

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
    usr::exercise2();
}



