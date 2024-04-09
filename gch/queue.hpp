//
//  queue.hpp
//  gch
//
//  Created by Antony Searle on 9/4/2024.
//

#ifndef queue_hpp
#define queue_hpp

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>

namespace gch3 {
    
    static_assert(sizeof(void*) == 8);
    
    template<typename T> struct Queue;
    
    template<typename T>
    struct Queue<T*> {
                
        struct alignas(4096) Node {
            std::array<T*, 511> elements;
            Node* next;                   // <-- next is deliberately uninitialized
        };
        
        
        T** _a;
        T** _b;
        
        // There is a chain of zero or more nodes
        //
        // _a is null or points into the first Node
        // _b is null or points into the last Node
        //
        // By masking the low bits of _a or _b we can construct a pointer to
        // [the first slot of] the Node
        //
        // By setting the low bits of _a or _b we can construct a pointer to
        // the last slot of the Node
        //
        // The last slot of the Node contains the NEXT pointer to another node,
        // or null.
        //
        // If _a and _b point into the same Node, _a <= _b
        //
        // _a never points to the last slot
        
        static Node* _node_from(T** p) {
            return (Node*) ((std::intptr_t)p & 0xFFFFFFFFFFFFF000);
        }
        
        static bool _is_aligned(Node* p) {
            return !((std::intptr_t)p & 0x0000000000000FFF);
        }
        
        void push(T* x) {
            if (!_b) {
                assert(!_a);
                Node* p = new Node;
                assert(p && _is_aligned(p));
                _a = _b = p->elements.begin();
            } else if (_b == _node_from(_b)->elements.end()) {
                Node* p = new Node;
                assert(p && _is_aligned(p));
                _node_from(_b)->next = p;
                _b = p->elements.begin();
            }
            *_b++ = x;
        }
        
        T* pop() {
            if (_a == _b)
                return nullptr;
            assert(_a != _node_from(_a)->elements.end());
            T* c = *_a++;
            if (_a == _node_from(_a)->elements.end()) {
                Node* d = _node_from(_a)->next;
                delete _node_from(_a);
                if (!d) {
                    assert(_a == _b);
                    _a = _b = nullptr;
                } else {
                    _a = d->elements.begin();
                }
            }
            return c;
        }
        
        bool empty() const {
            return _a == _b;
        }
        
        
    };
    
}

#endif /* queue_hpp */
