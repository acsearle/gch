//
//  deque.hpp
//  gch
//
//  Created by Antony Searle on 23/4/2024.
//

#ifndef deque_hpp
#define deque_hpp

#include <cassert>
#include <algorithm>

namespace gc {
    
    // A double-ended queue with "true" O(1) operations.
    
    // Use case: mutators maintain containers of allocated objects that are
    // periodically transferred to the mutator thread.  For this task
    // std::vector::push_back(...) is (presumably) the fastest strategy in
    // the throughput sense, but occasionally (1/N pushes) the container will
    // perform an O(N) resize, causing a latency spike.
    
    // To trade off latency for throughput, we can consider std::deque or
    // std::list.
    
    // std::list is always-worst-case with an allocation every push, but this
    // worst case is truly O(1) (assuming the allocator is well-behaved).
    
    // std::deque allocates a new chunk every M elements and never moves any
    // elements, but it does maintain a std::vector-like structure of
    // metadata, which puts us back in the same situation above of occasional
    // O(N/M) = O(N) moves of the metadata.  On MSVC the chunk size is
    // notoriously too small, so this may be a real problem there.
    
    // This deque (theoretically) provides
    // - True O(1) push with a bounded worst case of fixed size allocation
    //   and some pointer writes.
    // - Same bounds as std::list
    // - Better average performance than std::list
    // - Same average performace as std::deque
    // - Better bounds than std::deque
    // The significant trade-offs are
    // - Lose std::deque's fast indexing
    // - Lose std::list's fast interior insert/erase
    //
    // The implementation is a doubly linked list of arrays, akin to
    // `std::list<std::array<T, M>>`.
    //
    // Each node contains a prev pointer, an array of T, and a next pointer.
    // The nodes are power-of-two aligned.  For pointer sized objects, we
    // can have a chunk size of 4096 bytes holding 2 8-byte pointers and 510
    // 8-byte slots that may or may not contain a valid T.  The nodes are
    // arranged into a circular doubly-linked list.
    //
    // The deque holds _begin and _end pointers to elements of the arrays of
    // two nodes (or, the same node).  By exploiting alignment, we can
    // construct pointers to the nodes simply by masking the low bits, and
    // likewise when advancing or retreating pointers we can check for them
    // falling off the array simply by checking the low bits.  This relies on
    // implementation defined behavior (naive pointers-are-addresses) but
    // we can fall back to explicit node pointers if required.
    //
    // Because the linked list is circular, a roughly-constant-sized queue being
    // push_back and pop_front can just crawl around the buffer reusing
    // existing space.  When a push would advance from below into the node that
    // holds the first element, we instead insert a new node; this avoids the
    // case when the _end pointer reaches the _begin pointer, and we have to
    // insert the new node and some of the existing elements.
    //
    // When a node becomes unoccupied, we don't delete it, so
    // the deque will have "capcity" remembering its highest occupancy, like
    // a vector and probably like std::deque's metadata.  This avoids repeated
    // allocation / deallocation as a stack repeatedly crosses node_bounday
    // boundary.  `shrink_to_fit()` can be called to purge these unused nodes.
    // An alternative strategy may be to delete unused nodes unless they are
    // the last unused node, which we can cheaply test for (if this can be
    // proved not to thrash under any circumstances).
    //
    // We have O(1) empty can have an O(1) lower bound on size, but computing
    // the size requires O(N/M) pointer chasing, or maintaining a third field
    // in the object.
    //
    // TODO: benchmark claims above
    
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
            
            bool operator==(_iterator const&) const = default;
            
            template<typename V>
            bool operator==(_iterator<U> const& other) const {
                return current != other.current;
            }
            
        };
        
        using iterator = _iterator<T>;
        using const_iterator = _iterator<const T>;
        
        
        
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
        
        std::size_t size_lower_bound() const {
            node_type* first = _node_from(_begin);
            node_type* last = _node_from(_end);
            if (first == last) {
                return _end - _begin;
            } else {
                return (first->end() - _begin) + (_end - last->begin());
            }
        }
        
    };
    
    using std::swap;
    
    template<typename T>
    void swap(deque<T>& r, deque<T>& s) {
        r.swap(s);
    }
    
} // namespace gc
#endif /* deque_hpp */
