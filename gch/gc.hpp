//
//  gc.hpp
//  gch
//
//  Created by Antony Searle on 10/4/2024.
//

#ifndef gc_hpp
#define gc_hpp

#include <pthread/pthread.h>

#include <cstdint>

#include <atomic>
#include <stack>

#include "queue.hpp"

namespace gc {
        
    using Color = std::intptr_t;
    inline constexpr Color GRAY = 1;
    
    using Order = std::memory_order;
    inline constexpr Order RELAXED = std::memory_order_relaxed;
    inline constexpr Order ACQUIRE = std::memory_order_acquire;
    inline constexpr Order RELEASE = std::memory_order_release;
    inline constexpr Order ACQ_REL = std::memory_order_acq_rel;
    
    struct Object;
    struct Global;
    struct Local;
    struct Channel;
    struct CollectionContext;
    struct ShadeContext;
    struct ScanContext;
    struct SweepContext;
    template<typename> struct StrongPtr;
    template<typename> struct Atomic;
    
    void LOG(const char* format, ...);

    void enter();
    void handshake();
    void leave();
    
    // Mutators *shade*
    // - Leaf nodes directly WHITE -> BLACK
    //   - Including weak upgrade
    // - Non-leaf nodes WHITE -> GRAY and set local.dirty
    //
    // Collectors *scan*
    // - All nodes WHITE -> BLACK and enqueue children
    //
    // Collectors *sweep*
    //
    // Are these statements true?
    //
    // Collectors never WHITE -> GRAY, never set local.dirty
    // Collectors never shade
    // Mutators never scan
    // Leaves never WHITE -> GRAY / dirty
    
    // (Weak leaves are easy)
    
    void shade(const Object* object);
    void shade(const Object* object, ShadeContext& context);

    [[noreturn]] void collect();
    
    
    struct Object {
        
        using Color = std::intptr_t;
        mutable std::atomic<Color> color;
        
        Object();
        Object(Object const&) = delete;
        Object(Object&&) = delete;
        virtual ~Object() = default;
        Object& operator=(Object const&) = delete;
        Object& operator=(Object&&) = delete;
        
        virtual void shade(ShadeContext&) const;
        virtual void scan(ScanContext&) const;
        [[nodiscard]] virtual bool sweep(SweepContext&);
        
    }; // struct Object
    
    
    struct Leaf : Object {
        
        Leaf();
        Leaf(const Leaf&) = delete;
        Leaf(const Leaf&&) = delete;
        virtual ~Leaf() = default;
        Leaf& operator=(Leaf const&) = delete;
        Leaf& operator=(Leaf&&) = delete;
        
        virtual void shade(ShadeContext&) const override final;
        virtual void scan(ScanContext&) const override final;
        
    }; // struct Leaf
    
    
    template<typename T>
    struct Atomic<StrongPtr<T>> {
        
        std::atomic<T*> ptr;
        
        Atomic() = default;
        Atomic(Atomic const&) = delete;
        Atomic(Atomic&&) = delete;
        ~Atomic() = default;
        Atomic& operator=(Atomic const&) = delete;
        Atomic& operator=(Atomic&&) = delete;
        
        explicit Atomic(std::nullptr_t);
        explicit Atomic(T*);
        
        void store(T* desired, Order order);
        T* load(Order order) const;
        T* exchange(T* desired, Order order);
        bool compare_exchange_weak(T*& expected, T* desired, Order success, Order failure);
        bool compare_exchange_strong(T*& expected, T* desired, Order success, Order failure);
        
    }; // Atomic<StrongPtr<T>>
    

    template<typename T>
    struct StrongPtr {
        
        Atomic<StrongPtr<T>> ptr;
        
        StrongPtr() = default;
        StrongPtr(StrongPtr const&);
        StrongPtr(StrongPtr&&);
        ~StrongPtr() = default;
        StrongPtr& operator=(StrongPtr const&);
        StrongPtr& operator=(StrongPtr&&);
        
        bool operator==(const StrongPtr&) const;
        
        explicit StrongPtr(std::nullptr_t);
        StrongPtr& operator=(std::nullptr_t);
        bool operator==(std::nullptr_t) const;
        
        explicit StrongPtr(T*);
        StrongPtr& operator=(T*);
        explicit operator T*() const;
        bool operator==(T*) const;
        
        explicit operator bool() const;
        
        T* operator->() const;
        T& operator*() const;
        bool operator!() const;
        
    }; // StrongPtr<T>

    
    struct Global {
        
        // public concurrent state
        
        
        // TODO: communicate the configuration to the mutators with handshakes,
        // then rely on ordinary loads from thread_local storage?
        
        // we can load-relaxed these values because the mutator is allowed to
        // use their old or new values when they change between handshakes

        std::atomic<Color> white = 0; // <-- current encoding of color white
        std::atomic<Color> alloc = 0; // <-- current encoding of new allocation color
                
        // public sequential state

        std::mutex mutex;
        std::condition_variable condition_variable;
        
        std::vector<Channel*> entrants;
        
    };
        
    
    struct Channel {
        std::mutex mutex;
        std::condition_variable condition_variable;
        // TODO: make these bit flags
        bool abandoned = false;
        bool pending = false;
        bool dirty = false;
        bool request_infants = false;
        deque<Object*> infants;

    };

    
    // Thread local state
    struct Local {
        bool dirty = false;
        deque<Object*> allocations;
        deque<Object*> roots;
        Channel* channel = nullptr;
    };
    
    // Context passed to gc operations to avoid, for example, repeated atomic
    // loads from global.white
    
    struct CollectionContext {
        Color _white;
        Color WHITE() const { return _white; }
        Color BLACK() const { return _white ^ 2; }
    };

    struct ShadeContext : CollectionContext {
    };
    
    struct ScanContext : CollectionContext {
        
        void push(Object const*const& field);
        void push(Leaf const*const& field);

        template<typename T> 
        void push(StrongPtr<T> const& field) {
            push(field.ptr.load(ACQUIRE));
        }
        
        template<typename T> 
        void push(Atomic<StrongPtr<T>> const& field) {
            push(field.load(ACQUIRE));
        }
                
        

        std::stack<Object const*> _stack;
        
    };
    
    struct SweepContext : CollectionContext {
        
    };
        
    
    
    inline Global global;
    inline thread_local Local local;


    inline void shade(const Object* object) {
        if (object) {
            ShadeContext context;
            context._white = global.white.load(gc::RELAXED);
            object->shade(context);
        }
    }
    
    inline void shade(const Object* object, ShadeContext& context) {
        if (object) {
            object->shade(context);
        }
    }
        
    inline Object::Object()
    : color(global.alloc.load(RELAXED)) {
        assert(local.channel); // <-- catch allocations that are not inside a mutator
        local.allocations.push_back(this);
    }
    
    inline void Object::shade(ShadeContext& context) const {
        Color expected = context.WHITE();
        if (color.compare_exchange_strong(expected,
                                          GRAY,
                                          RELAXED,
                                          RELAXED)) {
            local.dirty = true;
        }
    }
    
    inline void Object::scan(ScanContext& context) const {
        // no-op
    }
    
    inline bool Object::sweep(SweepContext& context) {
        Color color = this->color.load(RELAXED);
        assert(color != GRAY);
        if (color == context.WHITE()) {
            delete this;
            return true;
        }
        return false;
    }
        
    
    inline Leaf::Leaf() : Object() {
    }
    
    inline void Leaf::shade(ShadeContext& context) const {
        Color expected = context.WHITE();
        color.compare_exchange_strong(expected,
                                      context.BLACK(),
                                      RELAXED,
                                      RELAXED);
    }
    
    inline void Leaf::scan(ScanContext& context) const {
        // no-op
    }
        
    template<typename T>
    Atomic<StrongPtr<T>>::Atomic(T* desired)
    : ptr(desired) {
        shade(desired);
    }
    
    template<typename T>
    T* Atomic<StrongPtr<T>>::load(Order order) const {
        return ptr.load(order);
    }
    
    template<typename T> 
    void Atomic<StrongPtr<T>>::store(T* desired, Order order) {
        shade(desired);
        T* old = ptr.exchange(desired, order);
        shade(old);
    }

    template<typename T>
    T* Atomic<StrongPtr<T>>::exchange(T* desired, Order order) {
        shade(desired);
        T* old = ptr.exchange(desired, order);
        shade(old);
        return old;
    }

    template<typename T>
    bool Atomic<StrongPtr<T>>::compare_exchange_strong(T*& expected,
                                          T* desired,
                                          Order success,
                                          Order failure) {
        return (ptr.compare_exchange_strong(expected, desired, success, failure)
                && (shade(expected), shade(desired), true));
    }
    
    template<typename T>
    bool Atomic<StrongPtr<T>>::compare_exchange_weak(T*& expected,
                                          T* desired,
                                          Order success,
                                          Order failure) {
        return (ptr.compare_exchange_weak(expected, desired, success, failure)
                && (shade(expected), shade(desired), true));
    }

    
    
    template<typename T>
    StrongPtr<T>::StrongPtr(StrongPtr const& other)
    : ptr(other.ptr.load(RELAXED)) {
    }

    template<typename T>
    StrongPtr<T>::StrongPtr(StrongPtr&& other)
    : ptr(other.ptr.load(RELAXED)) {
    }
    
    template<typename T>
    StrongPtr<T>& StrongPtr<T>::operator=(StrongPtr<T> const& other) {
        ptr.store(other.ptr.load(RELAXED), RELEASE);
        return *this;
    }
    
    template<typename T>
    StrongPtr<T>& StrongPtr<T>::operator=(StrongPtr<T>&& other) {
        ptr.store(other.ptr.load(RELAXED), RELEASE);
        return *this;
    }
    
    template<typename T>
    bool StrongPtr<T>::operator==(const StrongPtr& other) const {
        return ptr.load(RELAXED) == other.ptr.load(RELAXED);
    }
    
    template<typename T>
    StrongPtr<T>::StrongPtr(std::nullptr_t)
    : ptr(nullptr) {
    }
    
    template<typename T>
    StrongPtr<T>& StrongPtr<T>::operator=(std::nullptr_t) {
        ptr.store(nullptr, RELEASE);
    }
    
    template<typename T>
    bool StrongPtr<T>::operator==(std::nullptr_t) const {
        return ptr.load(RELAXED) == nullptr;
    }
    
    template<typename T>
    StrongPtr<T>::StrongPtr(T* object)
    : ptr(object) {
    }
    
    template<typename T>
    StrongPtr<T>& StrongPtr<T>::operator=(T* other) {
        ptr.store(other, RELEASE);
        return *this;
    }
    
    template<typename T>
    StrongPtr<T>::operator T*() const {
        return ptr.load(RELAXED);
    }
    
    template<typename T>
    bool StrongPtr<T>::operator==(T* other) const {
        return ptr.load(RELAXED) == other;
    }
    
    template<typename T>
    StrongPtr<T>::operator bool() const {
        return ptr.load(RELAXED);
    }
    
    template<typename T>
    T* StrongPtr<T>::operator->() const {
        T* a = ptr.load(RELAXED);
        assert(a);
        return a;
    }

    template<typename T>
    T& StrongPtr<T>::operator*() const {
        T* a = ptr.load(RELAXED);
        assert(a);
        return *a;
    }

    template<typename T>
    bool StrongPtr<T>::operator!() const {
        return !ptr.load(RELAXED);
    }
    
    
    // WHITE OBJECT -> BLACK PUSH - we process it later
    // GRAY OBJECT -> NOOP - we find it later in worklist
    // BLACK OBJECT -> no need to schedule it
    // WHITE LEAF -> BLACK NOPUSH - no need to schedule
    // GRAY LEAF -> impossible?
    
    inline void ScanContext::push(Object const* const& object) {
        Color expected = _white;
        Color black = _white ^ 2;
        if (object &&
            object->color.compare_exchange_strong(expected,
                                                  black,
                                                  RELAXED,
                                                  RELAXED)) {
            _stack.push(object);
        }
    }

    inline void ScanContext::push(Leaf const* const& object) {
        Color expected = _white;
        Color black = _white ^ 2;
        if (object)
            object->color.compare_exchange_strong(expected,
                                                  black,
                                                  RELAXED,
                                                  RELAXED);
    }

} // namespace gc

#endif /* gc_hpp */
