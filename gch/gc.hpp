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
    
    // TODO: shared xor mutable
    //
    // If there are not multiple holders of  pointers to an object, GC is
    // unncessary and conventional unique_ptr or deep copy semantics +
    // destructors are sufficient
    //
    // If multiple threads hold pointers to an object, then the object must be
    // immutable, or thread-safe.
    //
    // If only a single thread holds pointers to an object, then the object
    // doesn't need to be thread-safe, but is still subject to e.g. iterator
    // invalidation when mutation through one access point causes an unexpected
    // change elsewhere.
    //
    // When concurrent collection is active, the collector thread holds a
    // pointer to every object, so no GC object is single threaded and all must
    // be immutable or thread safe.  The GC promises to only read^ through
    // those pointers, and do so via the Object interface (i.e. scan).  Even
    // this restricted access requires all mutator writes to fields that are GC
    // pointers to have release semantics, and all collector reads to have
    // acquire semantics.  This ensures that the allocation and initialization
    // of the pointed-to object happen-before we dereference the pointer.
    //
    // This can be done by making such fields atomic, with the mutator
    // performing relaxed loads and release stores, and the collector performing
    // acquire loads (and no stores).  In this case, the scan method must be
    // prepared to observe a mixture of 'new' and 'old' values of the fields,
    // which may violate the object invariants.  This is a sort of torn read
    // of the object body.
    //
    // Alternatively, the whole object can be protected by a lock to serialize
    // access to the fields; both the mutator and collector will then always
    // see the object in a consistent state, at the cost of interfering with
    // each other's access.  Neither collector or mutator should hold the lock
    // for a macroscopic time.
    //
    // In the common special cases leaf nodes, with no GC pointer fields, the
    // GC does not inspect the body of the object at all.  When there is only
    // one GC pointer field, an atomic field is strictly superior to using
    // mutual exclusion.
    //
    // ^ Collector and mutator threads read and write to the .color field, even
    // of logically immutable objects.  This is why the .color field is `mutable
    // atomic`.
    
    
    using Color = std::intptr_t;
    
    using Order = std::memory_order;

    inline constexpr Color GRAY = 1;

    inline constexpr Order RELAXED = std::memory_order_relaxed;
    inline constexpr Order ACQUIRE = std::memory_order_acquire;
    inline constexpr Order RELEASE = std::memory_order_release;
    inline constexpr Order ACQ_REL = std::memory_order_acq_rel;

    struct Object;
    struct Global;
    struct Local;
    struct Channel;
    struct ScanContext;
    template<typename> struct Strong;
    template<typename> struct AtomicStrong;

    
    void enter();
    void handshake();
    void leave();

    void push(Object*);
    void pop();

    void collect();
    
    void LOG(const char* format, ...);

    
    struct Object {
        
        using Color = std::intptr_t;
        
        static void shade(const Object*);
        
        mutable std::atomic<Color> color;
                
        Object();
        Object(Object const&) = delete;
        Object(Object&&) = delete;
        virtual ~Object() = default;
        Object& operator=(Object const&) = delete;
        Object& operator=(Object&&) = delete;

        virtual void scan(ScanContext& context) const = 0;
        virtual void cull();
                                
    }; // struct Object
    
    
    template<typename T>
    struct AtomicStrong {
        
        std::atomic<T*> ptr;
        
        AtomicStrong() = default;
        AtomicStrong(AtomicStrong const&) = delete;
        AtomicStrong(AtomicStrong&&) = delete;
        ~AtomicStrong() = default;
        AtomicStrong& operator=(AtomicStrong const&) = delete;
        AtomicStrong& operator=(AtomicStrong&&) = delete;
        
        explicit AtomicStrong(std::nullptr_t);
        explicit AtomicStrong(T*);

        void store(T* desired, Order order);
        T* load(Order order) const;
        T* exchange(T* desired, Order order);
        bool compare_exchange_weak(T*& expected, T* desired, Order success, Order failure);
        bool compare_exchange_strong(T*& expected, T* desired, Order success, Order failure);
    };
    
    
    // Strong wraps AtomicStrong in a simpler interface that assumes that
    // only one mutator thread will freely read and write the pointer.  This
    // means RELAXED loads and RELEASE stores; the collector will explicitly
    // access the inner value to perform ACQUIRE loads (and no stores)
    
    template<typename T>
    struct Strong {
        
        AtomicStrong<T> ptr;
        
        Strong() = default;
        Strong(Strong const&);
        Strong(Strong&&);
        ~Strong() = default;
        Strong& operator=(Strong const&);
        Strong& operator=(Strong&&);
        
        bool operator==(const Strong& other) const;
        
        explicit Strong(std::nullptr_t);
        Strong& operator=(std::nullptr_t);
        bool operator==(std::nullptr_t) const;

        explicit Strong(T*);
        Strong& operator=(T*);
        explicit operator T*() const;
        bool operator==(T*) const;

        explicit operator bool() const;

        T* operator->() const;
        T& operator*() const;
        bool operator!() const;
        
    };
   
    
    struct Global {
        
        // public concurrent state
        
        std::atomic<Color> white = 0; // <-- current encoding of color white
        std::atomic<Color> alloc = 0; // <-- current encoding of new allocation color
        
        // public sequential state

        std::mutex mutex;
        std::condition_variable condition_variable;
        
        enum {
            WEAK_UPGRADE_PERMITTED,
            WEAK_UPGRADE_OCCURRED,
            WEAK_UPGRADE_PROHIBITED,
        } weak_upgrade_permission = WEAK_UPGRADE_PERMITTED;

        std::vector<Channel*> mutators_entering;
        
    };
        
    
    struct Channel {
        
        // linked list pointers are protected by global.mutex
        
        Channel* next;
        
        // data is protected by self.mutex
        
        std::mutex mutex;
        std::condition_variable condition_variable;
                
        bool abandoned = false;
        bool pending = false; // collector raises to request handshake
        // Configuration configuration;
        
        bool dirty = false;
        bool request_infants = false;
        deque<Object*> infants; // <-- objects the thread has allocated since last handshake
                                // global mutex for intrusive linked list of Locals
        

    };

    
    struct Local {
        bool dirty = false;
        deque<Object*> allocations;
        std::vector<Object*> roots;
        Channel* channel = nullptr;
    };
    
    
    struct ScanContext {
        
    public:
                
        void push(Object const* field);

        template<typename T> 
        void push(Strong<T> const& field) {
            push(field.ptr.load(ACQUIRE));
        }
        
        template<typename T> 
        void push(AtomicStrong<T> const& field) {
            push(field.load(ACQUIRE));
        }

        // do these really need to be exposed to support advanced
        // implementations of Object::scan?
    
        Color white() const { return _white; }
        Color black() const { return white() ^ 2; }

    protected:
        
        // Discourage direct use
        
        std::stack<Object const*> _stack;
        Color _white;
        
    };
        
    
    
    inline Global global;
    inline thread_local Local local;

    // todo
    //
    // move Channel into Local
    //
    // do we register the Local itself with the Global list of particpants?
    // or do we...
    
    // Channel must be able to outlive a thread so threads can end without
    // getting collector's permission
    
    // The Channel is only updated when the mutator is explicitly interacting
    // with the collector, so it doesn't need...
    
    
        
    
    
    
    
    
    inline void Object::shade(const Object* object) {
        if (object) {
            Color expected = global.white.load(RELAXED);
            if (object->color.compare_exchange_strong(expected,
                                                      GRAY,
                                                      RELAXED,
                                                      RELAXED)) {
                local.dirty = true;
            }
        }
    }
    
    inline Object::Object()
    : color(global.alloc.load(RELAXED)) {
        local.allocations.push_back(this);
    }
    
    inline void Object::scan(ScanContext& context) const {
    }
    
    inline void Object::cull() {
    }
    
    
    
    
    template<typename T> 
    T* AtomicStrong<T>::load(Order order) const {
        return ptr.load(order);
    }
    
    template<typename T> 
    void AtomicStrong<T>::store(T* desired, Order order) {
        Object::shade(desired);
        T* old = ptr.exchange(desired, order);
        Object::shade(old);
    }

    template<typename T>
    T* AtomicStrong<T>::exchange(T* desired, Order order) {
        Object::shade(desired);
        T* old = ptr.exchange(desired, order);
        Object::shade(old);
        return old;
    }

    template<typename T>
    bool AtomicStrong<T>::compare_exchange_strong(T*& expected,
                                          T* desired,
                                          Order success,
                                          Order failure) {
        return (ptr.compare_exchange_strong(expected, desired, success, failure)
                && (Object::shade(expected), Object::shade(desired), true));
    }
    
    template<typename T>
    bool AtomicStrong<T>::compare_exchange_weak(T*& expected,
                                          T* desired,
                                          Order success,
                                          Order failure) {
        return (ptr.compare_exchange_weak(expected, desired, success, failure)
                && (Object::shade(expected), Object::shade(desired), true));
    }

    
    
    template<typename T>
    Strong<T>::Strong(Strong const& other) 
    : ptr(other.ptr.load(RELAXED)) {
    }

    template<typename T>
    Strong<T>::Strong(Strong&& other)
    : ptr(other.ptr.load(RELAXED)) {
    }
    
    template<typename T>
    Strong<T>& Strong<T>::operator=(Strong<T> const& other) {
        ptr.store(other.ptr.load(RELAXED), RELEASE);
        return *this;
    }
    
    template<typename T>
    Strong<T>& Strong<T>::operator=(Strong<T>&& other) {
        ptr.store(other.ptr.load(RELAXED), RELEASE);
        return *this;
    }
    
    template<typename T>
    bool Strong<T>::operator==(const Strong& other) const {
        return ptr.load(RELAXED) == other.ptr.load(RELAXED);
    }
    
    template<typename T>
    Strong<T>::Strong(std::nullptr_t)
    : ptr(nullptr) {
    }
    
    template<typename T>
    Strong<T>& Strong<T>::operator=(std::nullptr_t) {
        ptr.store(nullptr, RELEASE);
    }
    
    template<typename T>
    bool Strong<T>::operator==(std::nullptr_t) const {
        return ptr.load(RELAXED) == nullptr;
    }
    
    template<typename T>
    Strong<T>::Strong(T* object)
    : ptr(object) {
    }
    
    template<typename T>
    Strong<T>& Strong<T>::operator=(T* other) {
        ptr.store(other, RELEASE);
        return *this;
    }
    
    template<typename T>
    Strong<T>::operator T*() const {
        return ptr.load(RELAXED);
    }
    
    template<typename T>
    bool Strong<T>::operator==(T* other) const {
        return ptr.load(RELAXED) == other;
    }
    
    template<typename T>
    Strong<T>::operator bool() const {
        return ptr.load(RELAXED);
    }
    
    template<typename T>
    T* Strong<T>::operator->() const {
        T* a = ptr.load(RELAXED);
        assert(a);
        return a;
    }

    template<typename T>
    T& Strong<T>::operator*() const {
        T* a = ptr.load(RELAXED);
        assert(a);
        return *a;
    }

    template<typename T>
    bool Strong<T>::operator!() const {
        return !ptr.load(RELAXED);
    }
    
    
    inline void ScanContext::push(Object const* object) {
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

    
    
    // Weak references
    //
    // First, consider a highly restrictive case suitable for string interning
    //
    // There at most one weak pointer to any object.
    // Objects weak-pointed-at have no outgoing pointers.
    //
    // Pointer upgrading can be relatively expensive since it occurs only when
    // we are finding a string from the hash of some chars, not when we are
    // using the string.
    //
    // In a concurrent environment we will always be upgrading weak pointers to
    // strong pointers, by executing WHITE -> GRAY.  This interferes with
    // GC termination, as we are constantly discovering new nodes.
    //
    // Suppose:
    //
    // mutator on weak load takes a global lock
    //   if upgrading is permitted in this epoch
    //     look at color
    //     if non-WHITE
    //       return object
    //     upgrade WHITE -> GRAY
    //     mark the thread dirty
    //     mark a global weak-upgrade-dirty flag
    //     return object
    //   else
    //     look at color
    //     if BLACK
    //       return object
    //     else if GRAY
    //       abort
    //     else return nullptr;
    //
    // collector is clean
    // mutators were clean when handshaked
    // collector takes lock
    //   if global dirty
    //     clear dirty
    //     we can't transition, go back and find the GRAYs again
    //   else
    //     no thread has upgraded weaks since last handshake
    //     ban upgrades
    // unlock
    //
    // there are only finitely many WHITE pointers to upgrade so this will
    // terminate but it may take a long time in the worst case.  in practice,
    // upgrades are rare?
    //
    // STAGE
    //
    // there are no GRAY objects
    // no objects are changing color
    // strong pointers point to only BLACK objects
    // weak pointers point to BLACK or WHITE objects
    //
    // the collector visits all weak pointers, inspects their object, writes
    // null if they are WHITE.  Via the cull method.
    //
    // the mutator visits weak pointers, inspects their objects, returns
    // null if they are WHITE.
    //
    // the mutator may see such a pointer as null or not, but in either case it
    // returns null.
    //
    // the mutator may make or change a new weak pointer to point to a BLACK
    // object
    //
    // HANDSHAKE
    //
    // all mutators see the null versions of the weak pointers
    // all non-null weak pointers point to BLACK objects
    //
    // there are no GRAY objects
    // mutators can no longer access any WHITE object
    // mutators can access only BLACK objects
    // the mutator can read through any weak pointer and will see BLACK
    //
    // collector frees all WHITE objects
    // there are no WHITE objects
    //
    // collector turns weak reference upgrading back on
    // there are no weak pointers to WHITE objects so no upgrades occur
    //
    // collector flips BLACK and WHITE
    //
    // if the upgrade uses the wrong definition of BLACK / WHITE, then a white
    // pointer escapes.  It could be written into a Strong or Weak object and
    // then handshaking will occur, so (as with suprious GRAY) this doesn't
    // actually matter.
    
    
} // namespace gc

#endif /* gc_hpp */
