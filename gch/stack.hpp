//
//  stack.hpp
//  gch
//
//  Created by Antony Searle on 23/4/2024.
//

#ifndef stack_hpp
#define stack_hpp

#include "gc.hpp"

namespace gc {
    
    template<typename T>
    struct TrieberStack : Object {
        struct Node : Object {
            Atomic<StrongPtr<Node>> next;
            T value;
            virtual ~Node() override = default;
            virtual void scan(ScanContext& context) const override {
                context.push(this->next);
                context.push(this->value);
            }
        }; // struct Node
        
        Atomic<StrongPtr<Node>> head;
        
        virtual void scan(ScanContext& context) const override {
            context.push(this->head);
        }
        
        void push(T value) {
            Node* desired = new Node;
            desired->value = std::move(value);
            Node* expected = head.load(ACQUIRE);
            do {
                desired->next.ptr.store(expected, RELAXED);
            } while (!head.compare_exchange_strong(expected,
                                                   desired,
                                                   RELEASE,
                                                   ACQUIRE));
        }
        
        bool pop(T& value) {
            Node* expected = head.load(ACQUIRE);
            for (;;) {
                if (expected == nullptr)
                    return false;
                Node* desired = expected->next.load(RELAXED);
                if (head.compare_exchange_strong(expected,
                                                 desired,
                                                 RELAXED,
                                                 ACQUIRE)) {
                    value = std::move(expected->value);
                    return true;
                }
            }
        }
        
    }; // TrieberStack<T>
}


#endif /* stack_hpp */
