//
//  string.hpp
//  gch
//
//  Created by Antony Searle on 20/4/2024.
//

#ifndef string_hpp
#define string_hpp

#include <cassert>

#include <mutex>
#include <string>
#include <unordered_set>

#include "gc.hpp"

namespace gc {

    
    // String has no inner gc pointers, so it can go directly WHITE -> BLACK
    // without making any GRAY work to delay termination
    
    struct String : Leaf {
        
        // TODO: provide a general purpose allocator for extra? gc::extra_val_t like
        // std::align_val_t ?  For gc:: or for gc::Object?
        static void* operator new(std::size_t count, std::size_t extra) {
            return ::operator new(count + extra);
        }
        
        std::size_t _hash;
        std::size_t _size;
        char _data[0];
        
        String() = delete;
        String(String const&) = delete;
        ~String() = default; // protected?
        String& operator=(String const&) = delete;
        
        String(std::size_t hash, std::size_t size)
        : _hash(hash), _size(size) {
        }
        
        bool invariant() {
            return _hash == std::hash<std::string_view>()((std::string_view)*this);
        }
        
        explicit operator std::string_view() const {
            return std::string_view(_data, _size);
        }
        
        struct Hash {
            using is_transparent = void;
            std::size_t operator()(String* const& key) const {
                assert(key && key->invariant());
                return key->_hash;
            }
            std::size_t operator()(std::pair<std::string_view, std::size_t> const& key) const {
                assert(key.second == std::hash<std::string_view>()(key.first));
                return key.second;
            }
        };
        
        struct KeyEqual {
            using is_transparent = void;
            bool operator()(String* const& a, String* const& b) const {
                assert(a && a->invariant());
                assert(b && b->invariant());
                assert(a->_hash == b->_hash); // <-- hash table should only be calling KeyEqual after it has checked for hash equality
                assert((a == b) == ((std::string_view)*a == (std::string_view)*b));
                return a == b;
            }
            bool operator()(String* const& a, std::pair<std::string_view, std::size_t> const& b) const {
                assert(a && a->invariant());
                assert(b.second == std::hash<std::string_view>()(b.first));
                assert(a->_hash == b.second); // <-- hash table should only be calling KeyEqual after it has checked for hash equality
                return (std::string_view)*a == b.first;
            }
        };
        
        // TODO: concurrent hash map?
        // The collector only holds the lock to do a single lookup-erase, which
        // should be fast.
        // The worst case is when one mutator thread resizes, holding up
        // another thread
        
        static std::mutex mutex;
        static std::unordered_set<String*, Hash, KeyEqual> set;
        
        [[nodiscard]] static String* from(std::string_view v) {
            std::size_t h = std::hash<std::string_view>()(v);
            printf("\"%.*s\" -> %zx\n", (int) v.size(), v.data(), h);
            std::unique_lock lock{mutex};
            auto a = set.find(std::pair(v, h));
            String* b;
            if (a != set.end()) {
                b = *a;
                assert(b);
                gc::shade(b);
            } else {
                auto n = v.size();
                b = new (n) String(h, n);
                assert(b);
                std::memcpy(b->_data, v.data(), n);
                set.insert(b);
            }
            return b;
        }
        
        virtual bool sweep(SweepContext& context) override {
            // Check for the common case of early out before we take the lock
            // and delay the mutator
            Color color = this->color.load(RELAXED);
            assert(color != GRAY);
            if (color == context.BLACK())
                return false;
            {
                std::unique_lock lock{mutex};
                // A WHITE String is only shaded while this lock is held, so
                // the color test is now authoritative
                color = this->color.load(RELAXED);
                assert(color != GRAY);
                if (color == context.BLACK())
                    return false;
                // The String is not strong-reachable, so we sweep it
                auto it = set.find(this);
                assert(it != set.end());
                set.erase(it);
            }
            // we don't need to hold the lock while we free the String
            delete this;
            return true;
        }
        
    };
    
} // namespace gc

#endif /* string_hpp */
