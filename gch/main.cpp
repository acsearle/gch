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
#include "queue.hpp"

namespace usr {
    
    using namespace gc;
    
    constexpr std::size_t THREADS = 3;
        
    void exercise() {
        
        auto* a = new gc::_ctrie::Ctrie<const String*, const String*>;
        global.roots.push_back(a);
        
        std::stack<std::thread> mutators;
        for (std::size_t i = 0; i != THREADS; ++i) {
            LOG("spawns mutator thread");
            mutators.emplace([=](){
                char name[3] = "M1";
                name[1] += i;
                pthread_setname_np(name);
                gc::enter();
                
                for (int i = 0; i != 26; ++i) {
                    gc::handshake();
                    char c = 'a' + i;
                    char d = c + ('A' - 'a');
                    a->insert_or_assign(String::make(String::Query(std::string_view(&c, 1))),
                                        String::make(String::Query(std::string_view(&d, 1))));
                }

                for (int i = 0; i != 26; ++i) {
                    gc::handshake();
                    char c = 'a' + i;
                    const String* b = a->find(String::make(String::Query(std::string_view(&c, 1))));
                    if (b) {
                        printf("Found \"%c\" -> \"%.*s\"\n", c, (int) b->view().size(), b->view().data());
                    } else {
                        printf("Found \"%c\" -> 0\n", c);
                    }
                }

                for (int i = 0; i != 26; ++i) {
                    gc::handshake();
                    char c = 'a' + i;
                    a->erase(String::make(String::Query(std::string_view(&c, 1))));
                }

                                
                gc::leave();
            });
        }
        
        gc::leave();
        
        while (!mutators.empty()) {
            mutators.top().join();
            LOG("joined a mutator thread");
            mutators.pop();
        }
    }
}

int main(int argc, const char * argv[]) {
    pthread_setname_np("M0");
    gc::enter();
    gc::String::enter();
    gc::LOG("spawns collector thread");
    std::thread collector{gc::collect};
    usr::exercise();
    collector.join();
    gc::LOG("joined the collector thread");
    gc::leave();
}



