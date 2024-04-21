//
//  string.cpp
//  gch
//
//  Created by Antony Searle on 20/4/2024.
//

#include "string.hpp"

namespace gc {

    std::mutex String::mutex;
    std::unordered_set<String*, String::Hash, String::KeyEqual> String::set;

}
