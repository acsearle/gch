//
//  ctrie.hpp
//  gch
//
//  Created by Antony Searle on 20/4/2024.
//

#ifndef ctrie_hpp
#define ctrie_hpp

#include "gc.hpp"
#include "string.hpp"

namespace gc {
    
    namespace _ctrie {
                
        struct Query {
        
            // a char sequence and its hash

            // libc++ std::hash<std::string_view> seems to be CityHash64 which
            // is perfectly adequate

            std::string_view view;
            std::size_t hash;
            
            bool invariant() {
                return hash == std::hash<std::string_view>()(view);
            }
            
            void debug() const {
                printf("Query{\"%.*s\",%zx}",
                       (int)view.size(), view.data(),
                       hash);
            }
            
            Query(std::string_view v, std::size_t h)
            : view(v), hash(h) {
                assert(invariant());
            }
            
            explicit Query(std::string_view v)
            : Query(v, std::hash<std::string_view>()(v)) {
            }
            
            Query() 
            : Query(std::string_view()) {
            }
            
        };
        
        enum Result {
            RESTART = 0,
            OK = 1,
        };
        
        struct ANode; // Any       : Object
        struct BNode; // Branch    : Any
        struct MNode; // Main      : Any
        struct INode; // Indirect  : Branch
        struct SNode; // Singleton : Branch
        struct CNode; // Ctrie     : Main
        struct LNode; // List      : Main
        struct TNode; // Tomb      : Main
        
        struct ANode : Object {
            virtual void debug(int lev) const = 0;
        };

        struct BNode : ANode {
            virtual std::pair<Result, const SNode*> _emplace(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const = 0;
            virtual std::pair<Result, const SNode*> _erase(const INode* i, const SNode* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const  = 0;
            virtual const BNode* _resurrect() const = 0;
            virtual const MNode* _contract(const CNode* parent, int lev) const = 0;
            
        }; // struct BNode
                
        struct SNode : BNode {
            
            using Query = gc::_ctrie::Query;
            
            // class methods
                        
            static void enter();
            static void leave();

            static void* operator new(std::size_t count, std::size_t extra);
            static const SNode* make(Query q);
            
            // instance methods
            
            bool invariant() const {
                return ((color.load(RELAXED) != GRAY) &&
                        (_hash == std::hash<std::string_view>()(view())));
            }
            
            SNode() = delete;
            ~SNode() = default;
            SNode(const SNode&) = delete;
            SNode(SNode&&) = delete;
            SNode& operator=(const SNode&) = delete;
            SNode& operator=(SNode&&) = delete;
            
            explicit SNode(Query);

            // GC methods
            
            virtual void shade(ShadeContext&) const override;
            virtual void scan(ScanContext& context) const override;
            virtual bool sweep(SweepContext&) override;
            virtual void shade_weak(ShadeContext&) const override;
            virtual void scan_weak(ScanContext&) const override;

            // Ctrie node methods
            
            virtual void debug(int lev) const override;
            virtual std::pair<Result, const SNode*> _emplace(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual std::pair<Result, const SNode*> _erase(const INode* i, const SNode* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual const BNode* _resurrect() const override ;
            virtual const MNode* _contract(const CNode* cn, int lev) const override;

            std::size_t _hash;
            std::size_t _size;
            char _data[0];
            
            // string methods
            
            std::string_view view() const {
                return std::string_view(_data, _size);
            }
            
            // std::unordered_map interop
            // we can use the string hash and support query by string_view
            // or we can use the pointer itself and rely on identity, which
            // requires string_view lookup to be indirected through the
            // interning process
                        
            struct Hash {
                using is_transparent = void;
                std::size_t operator()(SNode const* const& k) const {
                    return k->_hash;
                }
                std::size_t operator()(Query q) const {
                    return q.hash;
                }
            }; // struct Hash
            
            struct KeyEqual {
                using is_transparent = void;
                bool operator()(const SNode* a, const SNode* b) const {
                    // A hash table should only he checking equality when the hashes are equal
                    assert(a->_hash == b->_hash);
                    // equivalence implies identity
                    assert((a->view() == b->view()) == (a == b));
                    return a == b;
                }
                bool operator()(const SNode* a, Query b) const {
                    // A hash table should only he checking equality when the hashes are equal
                    assert(a->_hash == b.hash);
                    return a->view() == b.view;
                }
            }; // struct KeyEqual
                        
        }; // struct SNode
        
    } // namespace _ctrie
            
} // namespae gc

#endif /* ctrie_hpp */
