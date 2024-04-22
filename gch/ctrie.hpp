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
            std::size_t hash;
            std::string_view view;
        };
        
        enum Result {
            NOTFOUND = -1, // TODO: We don't need NOTFOUND, just return {OK, nullptr}
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
            virtual std::pair<Result, const SNode*> vlookupB(const INode* i, Query q, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, const SNode*> vinsertB(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const = 0;
            virtual std::pair<Result, const SNode*> vremoveB(const INode* i, const SNode* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const  = 0;
            virtual const BNode* vresurrectA() const = 0;
            virtual const MNode* vtoContractedB(const CNode* parent, int lev) const = 0;
            
            
            virtual void maybeShade() const = 0;
            virtual void maybeScan(ScanContext&) const = 0;
            
        }; // struct BNode
                
        struct SNode : BNode {
            
            static void* operator new(std::size_t count, std::size_t extra) {
                return ::operator new(count + extra);
            }

            explicit SNode(Query);
            virtual void debug(int lev) const override;
            virtual void shade(ShadeContext&) const override;
            virtual void scan(ScanContext& context) const override;
            virtual std::pair<Result, const SNode*> vlookupB(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual std::pair<Result, const SNode*> vinsertB(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual std::pair<Result, const SNode*> vremoveB(const INode* i, const SNode* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual const BNode* vresurrectA() const override ;
            virtual const MNode* vtoContractedB(const CNode* cn, int lev) const override;

            std::size_t _hash;
            std::size_t _size;
            char _data[0];
            
            std::string_view view() const {
                return std::string_view(_data, _size);
            }
            
            virtual void maybeShade() const override;
            virtual void maybeScan(ScanContext&) const override;
            virtual bool sweep(SweepContext&) override;
            static const SNode* make(std::string_view v);
            

        }; // struct SNode
                
        
        
        struct Ctrie : Object {
            
            Ctrie();
            
            void debug();
            
            virtual void scan(ScanContext& context) const override;

            const SNode* lookup(Query q);
            const SNode* emplace(Query q);
            const SNode* remove(const SNode* k);

            INode* root;

        }; // struct Ctrie
        
    } // namespace _ctrie
    
    using _ctrie::Ctrie;
    
    inline Ctrie* global_string_ctrie = nullptr;
    
} // namespae gc

#endif /* ctrie_hpp */
