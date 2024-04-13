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

namespace gc {

    // This deque provides true O(1) bounds on push.
    // TODO: allocator latency spikes?
    //
    // It does not exhibit the latency spikes of std::vector.  std::deque
    // uses std::vector-like metadata, but it would probably be OK in practice.
    //
    // TODO: bench gc::deque vs std::deque
    
    template<typename T>
    struct deque {
                
        struct node_type;
        
        enum {
            PAGE = 4096,             // <-- not necessarily a system page
            MASK = 0 - PAGE,
            COUNT = ((PAGE - 2 * std::max(sizeof(T), sizeof(node_type*)))
                     / sizeof(T)),   // <-- TODO: correctness of this layout calculation?
            INIT = COUNT / 2,
        };
        
        struct alignas(PAGE) node_type {
            
            node_type* prev;
            T elements[COUNT];
            node_type* next;
            
            T* begin() { return elements; }
            T* end() { return elements + COUNT; }
            
        };
        
        static_assert(sizeof(node_type) == PAGE);
        static_assert(alignof(node_type) == PAGE);
        static_assert(COUNT > 100);
        
        T* _begin;
        T* _end;
        
        static node_type* _node_from(T* p) {
            return reinterpret_cast<node_type*>(reinterpret_cast<std::intptr_t>(p)
                                           & MASK);
        }

        static const node_type* _node_from(const T* p) {
            return reinterpret_cast<const node_type*>(reinterpret_cast<std::intptr_t>(p)
                                                & MASK);
        }
        
        void _from_null() {
            node_type* node = new node_type;
            node->prev = node->next = node;
            _begin = _end = node->begin() + INIT;
        }
        
        void _insert_before(node_type* node) {
            node_type* p = new node_type;
            p->next = node;
            p->prev = node->prev;
            p->next->prev = p;
            p->prev->next = p;
        }
        
        node_type* _erase(node_type* node) {
            node->next->prev = node->prev;
            node->prev->next = node->next;
            node_type* after = node->next;
            delete node;
            return after;
        }
        
        static T* _advance(T* p) {
            node_type* node = _node_from(p);
            assert(p != node->end());
            ++p;
            if (p == node->end())
                p = node->next->begin();
            return p;
        }

        static T* _retreat(T* p) {
            node_type* node = _node_from(p);
            assert(p != node->end());
            if (p == node->begin()) {
                p = node->prev->end();
            }
            --p;
        }

        template<typename U>
        struct _iterator {
            
            U* current;
            
            U& operator*() const { assert(current); return *current; }
            U* operator->() const { assert(current); return current; }
            
            _iterator& operator++() {
                current = _advance(current);
                return *this;
            }
            
            _iterator& operator--() {
                current = _retreat(current);
                return *this;
            }
            
            _iterator operator++(int) {
                _iterator old{current};
                operator++();
                return old;
            }

            _iterator operator--(int) {
                _iterator old{current};
                operator--();
                return old;
            }

            bool operator!=(_iterator const&) const = default;
            
            template<typename V>
            bool operator!=(_iterator<U> const& other) const {
                return current != other.current;
            }
            
        };
        
        using iterator = _iterator<T*>;
        using const_iterator = _iterator<const T*>;
                        
        
        
        deque& swap(deque& other) {
            using std::swap;
            swap(_begin, other._begin);
            swap(_end, other._end);
            return other;
        }
        
        deque()
        : _begin(nullptr)
        , _end(nullptr) {
        }
        
        deque(const deque&) = delete;
        
        deque(deque&& other)
        : _begin(std::exchange(other._begin, nullptr))
        , _end(std::exchange(other._end, nullptr)) {
        }
        
        ~deque() {
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            while (first != last) {
                delete std::exchange(first, first->next);
            }
            delete first;
        }
        
        deque& operator=(const deque&) = delete;
        
        deque& operator=(deque&& other) {
            return deque(std::move(other)).swap(*this);
        }
        
        
        bool empty() const { return _begin == _end; }
               
        iterator begin() { return iterator{_begin}; }
        const_iterator begin() const { return const_iterator{_begin}; }

        iterator end() { return iterator{_end}; }
        const_iterator end() const { return const_iterator{_end}; }
        
        T& front() { assert(!empty()); return *_begin; }
        T const& front() const { assert(!empty()); return *_begin; }

        T& back() {
            assert(!empty());
            node_type* last = _node_from(_begin);
            T* p;
            if (_begin != last->begin()) {
                return *(_begin - 1);
            } else {
                return *(last->prev->end() - 1);
            }
        }
        
        void emplace_back(auto&&... args) {
            if (!_end) { _from_null(); }
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            assert(first->next->prev == first);
            assert(last->prev->next == last);
            assert(_end != last->end());
            new (_end++) T(std::forward<decltype(args)>(args)...);
            if (_end == last->end()) {
                if (last->next == first)
                    _insert_before(first);
                last = last->next;
                _end = last->begin();
            }
        }
        
        void push_back(const T& value) { return emplace_back(value); }
        void push_back(T&& value) { return emplace_back(std::move(value)); }
        
        void emplace_front(auto&&... args) {
            if (!_begin) _from_null();
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            T* p;
            assert(_begin != first->end());
            if (_begin == first->begin()) {
                if (first->prev == last)
                    _insert_before(first);
                p = first->prev->end();
            } else {
                p = _begin;
            }
            --p;
            new (p) T(std::forward<decltype(args)>(args)...);
            _begin = p;
        }
        
        void push_front(const T& value) { return emplace_front(value); }
        void push_front(T&& value) { return emplace_front(std::move(value)); }

        void pop_front() {
            assert(!empty());
            std::destroy_at(_begin++);
            node_type* first = _node_from(_begin);
            if (_begin == first->end()) {
                if (_begin != _end) {
                    _begin = first->next->begin();
                } else {
                    _begin = _end = first->begin() + INIT;
                }
            }
        }
        
        void pop_back() {
            assert(!empty());
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            if (_end == last->begin()) {
                last = last->prev;
                _end = last->end();
            }
            std::destroy_at(--_end);
        }
                
        void clear() {
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            while (first != last)
                delete std::exchange(first, first->next);
            if (first) {
                first->next = first;
                first->prev = first;
                _begin = _end = first->elements.begin() + INIT;
            }
        }
        
        void shrink_to_fit() {
            if (!_end)
                return;
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            if (last->next != first->prev) {
                // remember the first empty node
                node_type* cursor = last->next;
                // splice over the empty nodes
                last->next = first;
                first->prev = last;
                // delete the empty nodes
                while (cursor != first)
                    delete std::exchange(cursor, cursor->next);
            }
        }
        
        
    };
    
    using std::swap;
    
    template<typename T>
    void swap(deque<T>& r, deque<T>& s) {
        r.swap(s);
    }
    
} // namespace gc

#endif /* queue_hpp */
