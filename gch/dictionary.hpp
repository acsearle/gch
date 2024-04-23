//
//  dictionary.hpp
//  gch
//
//  Created by Antony Searle on 23/4/2024.
//

#ifndef dictionary_hpp
#define dictionary_hpp

#include "gc.hpp"
#include "string.hpp"

namespace gc {
    
    struct Dictionary : Object {
        
        // TODO: rely on string interning
        mutable std::mutex mutex;
        std::unordered_map<String const*, Object*, typename String::Hash, typename String::KeyEqual> map;
                
        // TODO: find must be fast
        gc::Object* load(String const* key) const {
            std::unique_lock lock{mutex};
            auto it = map.find(key);
            return it != map.end() ? it->second : nullptr;
        }
        
        // TODO: set must be fast-ish
        Object* exchange(String const* key, Object* value) {
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

        // TODO: scanning must not O(N) block the mutator
        virtual void scan(ScanContext& context) const override {
            std::unique_lock lock{mutex};
            for (const auto& kv : map) { // <-- O(N) iteration
                context.push(kv.first);
                context.push(kv.second);
            }
        }
        
    }; // struct Dictionary
    
}


#endif /* dictionary_hpp */
