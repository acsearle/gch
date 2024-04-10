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

    // A queue of machine words
    //
    //
    
    template<typename T>
    struct Queue {
        
        static_assert(sizeof(T) == 8);
        
        using Z = std::intptr_t;
        
        enum {
            WORD = 8,
            PAGE = 4096,             // <-- not necessarily a system page
            HIGH = 0 - PAGE,
            MID  = PAGE - WORD,
            LOW  = WORD - 1,
            COUNT = PAGE / WORD - 1,
        };
        
        struct alignas(PAGE) Node {
            T begin[COUNT];
            union {
                Node* next;
                T end[1];
            };
        };
        
        static_assert(sizeof(Node) == PAGE);
        
        T* _a;
        T* _b;
                
        static bool _is_aligned(Node* p) {
            return !((Z)p & MID);
        }
        
        static bool _is_aligned(T* p) {
            return !((Z)p & LOW);
        }
        
        static bool _invariant(T* _a, T* _b) {
            if (!_a != !_b)
                return false;
            if (!_is_aligned(_a))
                return false;
            if (!_is_aligned(_b))
                return false;
            Node* first = _node_from(_a);
            if (!_is_aligned(first))
                return false;
            if (first && !(_a != first->end))
                return false;
            Node* last = _node_from(_b);
            if (!_is_aligned(last))
                return false;
            if (first == last && !(_a <= _b))
                return false;
            if (!first != !last)
                return false;
            while (first != last) {
                if (!first)
                    return false;
                first = first->next;
            }
            return true;
        }
        
        static Node* _node_from(T* p) {
            return (Node*) ((Z)p & HIGH);
        }
        

        bool empty() const {
            assert(_is_aligned(_a));
            return _a == _b;
        }
        
        void push_back(T x) {
            if (!_b || _b == _node_from(_b)->end) {
                Node* p = new Node;
                if (_b)
                    _node_from(_b)->next = p;
                else
                    _a = p->begin;
                _b = p->begin;
            }
            *_b++ = x;
        }
        
        void push_front(T x) {
            if (_a == _node_from(_a)->_a) {
                Node* p = new Node;
                p->next = _a;
                _a = p->end;
                if (!_b)
                    _b = _a;
            }
            *--_a = x;
        }
        
        bool pop_front(T& victim) {
            assert(_is_aligned(_a));
            if (empty())
                return false;
            assert(_a != _node_from(_a)->end);
            victim = *_a++;
            if (_a == _node_from(_a)->end) {
                if (_a != _b) {
                    Node* d = _node_from(_a)->next;
                    assert(_is_aligned(d));
                    delete _node_from(_a);
                    _a = d->begin;
                } else {
                    _a = _b = _node_from(_a)->begin;
                }
            }
            return true;
        }
        
        void _capacity_back() const {
            return _b ? _node_from(_b).end - _b : 0;
        }
        
        void _capacity_front() const {
            return _a - _node_from(_a).begin;
        }

        std::size_t push_back_some(T* buffer, std::size_t count) {
            std::size_t n = _capacity_back();
            assert(n <= COUNT);
            if (!n && !count) {
                Node* p = new Node;
                if (_b)
                    _b->next = p;
                else
                    _a = p->begin;
                _b = p->begin;
            }
            n = std::min(n, count);
            std::memcpy(_b, buffer, count);
            _b += n;
            return n;
        }
        
        std::size_t pop_front_some(T* buffer, std::size_t count) {
            if (!_a)
                return 0;
            std::size_t n;
            if (_node_from(_a) != _node_from(_b)) {
                 n = _node_from(_a)->end - _a;
            } else {
                n = _b - _a;
            }
            n = std::min(n, count);
            std::memcpy(buffer, _a, n);
            _a += n;
            if (_a == _node_from(_a)->end) {
                if (_a != _b) {
                    Node* c = _node_from(_a)->next;
                    delete _node_from(_a);
                    _a = c->begin;
                } else {
                    _a = _b = _node_from(_a)->begin;
                }
            }
        }
                
        // basic lifetime stuff
        
        Queue() 
        : _a(nullptr)
        , _b(nullptr) {
            assert(_is_aligned(_a));
        }
        
        Queue(const Queue&) = delete;
        
        Queue(Queue&& other)
        : _a(std::exchange(other._a, nullptr))
        , _b(std::exchange(other._b, nullptr)) {
            assert(_is_aligned(_a));
        }
        
        ~Queue() {
            // assert(_a == _b);
            assert(_is_aligned(_a));
            Node* first = _node_from(_a);
            Node* last = _node_from(_b);
            while (first != last) {
                delete std::exchange(first, first->next);
            }
            delete first;
        }
        
        Queue& operator=(const Queue&) = delete;
        
        Queue& swap(Queue& other) {
            assert(_is_aligned(_a));
            assert(_is_aligned(other._a));
            using std::swap;
            swap(_a, other._a);
            swap(_b, other._b);
            return other;
        }
        
        Queue& operator=(Queue&& other) {
            assert(_is_aligned(_a));
            assert(_is_aligned(other._a));
            return Queue(std::move(other)).swap(*this);
        }
        
        void clear() {
            Node* first = _node_from(_a);
            Node* last = _node_from(_b);
            while (first != last)
                delete std::exchange(first, first->next);
            if (last)
                _a = _b = last->elements.begin();
        }
        
    };
    
    template<typename T>
    void swap(Queue<T>& r, Queue<T>& s) {
        r.swap(s);
    }
    
}

#endif /* queue_hpp */
