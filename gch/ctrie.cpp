//
//  ctrie.cpp
//  gch
//
//  Created by Antony Searle on 20/4/2024.
//

#include "ctrie.hpp"

namespace gc {
    
    // Concurrent hash array mapped trie
    //
    // https://en.wikipedia.org/wiki/Ctrie
    //
    // Prokopec, A., Bronson N., Bagwell P., Odersky M. (2012) Concurrent Tries
    //     with Efficient Non-Blocking Snapshots
    
    // A concurrent map with good worst-case performance
    //
    // Use case is weak set of interned strings
    
    namespace _ctrie {
                
                        
        void clean(const INode* i, int lev);
        void cleanParent(const INode* p, const INode* i, std::size_t hc, int lev);
        const MNode* entomb(const SNode* sn);
        std::pair<Result, const SNode*> ilookup(const INode* i, Query q, int lev, const INode* parent);
        std::pair<Result, const SNode*> iinsert(const INode* i, Query q, int lev, const INode* parent);
        std::pair<Result, const SNode*> iremove(const INode* i, Query q, int lev, const INode* parent);
        const BNode* resurrect(const BNode* m);
        const MNode* toCompressed(const CNode* cn, int lev);
        const MNode* toContracted(const CNode* cn, int lev);

        struct MNode : ANode {
            virtual std::pair<Result, const SNode*> vlookupA(const INode* i, Query q, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, const SNode*> vinsertA(const INode* i, Query q, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, const SNode*> vremoveA(const INode* i, Query q, int lev, const INode* parent) const = 0;
            virtual void vremoveC(const INode* i, Query q, int lev, const INode* parent) const {};
            virtual const BNode* vresurrectB(const INode* parent) const;
            virtual void vcleanA(const INode* i, int lev) const {}
            virtual bool vcleanParentA(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MNode* m) const { return true; }
            virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MNode* m,
                                       const CNode* cn, std::uint64_t flag, int pos) const { return true; }
        }; // struct MNode
        
        struct INode : BNode {
            explicit INode(const MNode* desired);
            virtual void debug(int lev) const override;
            virtual void scan(ScanContext& context) const override ;
            virtual std::pair<Result, const SNode*> vlookupB(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual std::pair<Result, const SNode*> vinsertB(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual std::pair<Result, const SNode*> vremoveB(const INode* i, Query q, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual const BNode* vresurrectA() const override;
            virtual const MNode* vtoContractedB(const CNode* cn, int lev) const override;
            mutable Atomic<StrongPtr<const MNode>> main;
        }; // struct INode
                
        struct TNode : MNode {
            virtual void debug(int lev) const override;
            virtual void scan(ScanContext& context) const override;
            virtual std::pair<Result, const SNode*> vlookupA(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual std::pair<Result, const SNode*> vinsertA(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual std::pair<Result, const SNode*> vremoveA(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual void vremoveC(const INode* i, Query q, int lev, const INode* parent) const override;
            virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MNode* m,
                                       const CNode* cn, std::uint64_t flag, int pos) const override;
            virtual const BNode* vresurrectB(const INode* parent) const override;
            const SNode* sn;
        }; // struct TNode
                
        struct LNode : MNode {
            const SNode* sn;
            const LNode* next;
            
            virtual void debug(int lev) const override {
                printf("LNode(%lx,%p): ", this->color.load(RELAXED), sn);
                if (next)
                    next->debug(lev);
                else
                    printf("\n");
            }
            
            virtual void scan(ScanContext& context) const override {
                context.push(sn);
                context.push(next);
            }
            
            std::pair<Result, const SNode*> lookup(Query q) const {
                const LNode* ln = this;
                for (;;) {
                    if (!ln)
                        return {NOTFOUND, nullptr};
                    if (ln->sn->view() == q.view)
                        return {OK, ln->sn};
                    ln = ln->next;
                }
            }
            
            virtual std::pair<Result, const SNode*> vlookupA(const INode* i, Query q, int lev, const INode* parent) const override {
                const LNode* ln = this;
                return ln->lookup(q);
            }
            
            const MNode* inserted(Query q) const {
                const LNode* a = this;
                for (;;) {
                    if (a->sn->view() == q.view) {
                        // The key exists in the list, so we must copy the
                        // first part, replace that node, and point to the
                        // second part
                        const LNode* b = this;
                        LNode* c = new LNode;
                        LNode* d = c;
                        for (;;) {
                            if (b != a) {
                                d->sn = b->sn;
                                gc::shade(d->sn);
                                b = b->next;
                                LNode* e = new LNode;
                                d->next = e;
                                d = e;
                            } else {
                                d->sn = new (q.view.size()) SNode(q);
                                d->next = b->next;
                                gc::shade(d->next);
                                return c;
                            }
                        }
                    } else {
                        a = a->next;
                    }
                    if (a == nullptr) {
                        // We did not find the same key, so just prepend it
                        LNode* b = new LNode;
                        b->sn = new (q.view.size()) SNode(q);
                        b->next = this;
                        gc::shade(b->next);
                        return b;
                    }
                }
            }
            
            std::pair<const LNode*, const SNode*> removed(Query q) const {
                if (this->sn->view() == q.view)
                    return {this->next, this->sn};
                const LNode* a = this->next;
                for (;;) {
                    if (a == nullptr) {
                        // Not found at all
                        return {this, nullptr};
                    }
                    if (a->sn->view() != q.view) {
                        // Not found yet
                        a = a->next;
                    } else {
                        // Found inside the list
                        const LNode* b = this;
                        LNode* c = new LNode; // make new head
                        LNode* d = c;
                        for (;;) {
                            d->sn = b->sn; // copy over node
                            gc::shade(d->sn);
                            b = b->next;
                            if (b == a) {
                                // we've reached the node we are erasing, skip
                                // over it
                                d->next = a->next;
                                gc::shade(d->next);
                                return {c, a->sn};
                            }
                            LNode* e = new LNode;
                            d->next = e;
                            d = e;
                        }
                    }
                }
            }
            
            
            virtual std::pair<Result, const SNode*> vinsertA(const INode* i, Query q, int lev, const INode* parent) const override {
                // printf("LNode %lx,%p iinsert\n", this->color.load(RELAXED), this);
                const MNode* expected = this;
                if (i->main.compare_exchange_strong(expected,
                                                    inserted(q),
                                                    RELEASE,
                                                    RELAXED)) {
                    return {OK, nullptr};
                } else {
                    return {RESTART, nullptr};
                }
            }
            
            virtual std::pair<Result, const SNode*> vremoveA(const INode* i, Query q, int lev, const INode* parent) const override {
                const LNode* ln = this;
                auto [nln, v] = ln->removed(q);
                assert(nln && nln->sn);
                const MNode* expected = ln;
                const MNode* desired = nln->next ? nln : entomb(nln->sn);
                if (i->main.compare_exchange_strong(expected,
                                                    desired,
                                                    RELEASE,
                                                    RELAXED)) {
                    return {OK, v};
                } else {
                    return {RESTART, nullptr};
                }
            }
            
        }; // struct LNode
        
        
        struct CNode : MNode {
            
            std::uint64_t bmp;
            const BNode* array[0];
            
            virtual void debug(int lev) const override {
                lev += 6;
                printf("CNode(%lx,%#llx):\n", this->color.load(RELAXED), bmp);
                int j = 0;
                for (int i = 0; i != 64; ++i) {
                    std::uint64_t flag = std::uint64_t{1} << i;
                    if (bmp & flag) {
                        printf("%*s[%d]: ", lev, "", i);
                        array[j]->debug(lev);
                        j++;
                    }
                }
            }
            
            virtual void scan(ScanContext& context) const override {
                int num = __builtin_popcountll(this->bmp);
                for (int i = 0; i != num; ++i) {
                    context.push(this->array[i]);
                }
            }
            
            static std::pair<std::uint64_t, int>
            flagpos(std::size_t hash, int lev, std::uint64_t bmp) {
                auto a = (hash >> lev) & 63;
                std::uint64_t flag = std::uint64_t{1} << a;
                int pos = __builtin_popcountll(bmp & (flag - 1));
                return std::pair(flag, pos);
            }
            
            const CNode* inserted(std::uint64_t flag, int pos, const BNode* child) const {
                //printf("CNode inserted\n");
                auto n = __builtin_popcountll(bmp);
                void* a = operator new(sizeof(CNode) + sizeof(BNode*) * (n + 1));
                CNode* b = new (a) CNode;
                assert(!(this->bmp & flag));
                b->bmp = this->bmp | flag;
                std::memcpy(b->array, this->array, sizeof(BNode*) * pos);
                b->array[pos] = child;
                std::memcpy(b->array + pos + 1, this->array + pos, sizeof(BNode*) * (n - pos));
                for (int i = 0; i != n + 1; ++i)
                    gc::shade(b->array[i]);
                return b;
            }
            
            const CNode* updated(int pos, const BNode* child) const {
                //printf("CNode updated\n");
                auto n = __builtin_popcountll(bmp);
                void* a = operator new(sizeof(CNode) + sizeof(BNode*) * n);
                CNode* b = new (a) CNode;
                b->bmp = this->bmp;
                std::memcpy(b->array, this->array, sizeof(BNode*) * n);
                b->array[pos] = child;
                for (int i = 0; i != n; ++i)
                    gc::shade(b->array[i]);
                return b;
            }
            
            const CNode* removed(int pos, std::uint64_t flag) const {
                assert(this->bmp & flag);
                assert(__builtin_popcountll((flag - 1) & this->bmp) == pos);
                auto n = __builtin_popcountll(bmp);
                assert(pos < n);
                void* a = operator new(sizeof(CNode) + sizeof(BNode*) * n - 1);
                CNode* b = new (a) CNode;
                b->bmp = this->bmp ^ flag;
                std::memcpy(b->array, this->array, sizeof(BNode*) * pos);
                std::memcpy(b->array + pos, this->array + pos + 1, sizeof(BNode*) * (n - 1 - pos));
                for (int i = 0; i != n - 1; ++i)
                    gc::shade(b->array[i]);
                return b;
            }
            
            CNode() : bmp{0} {}
            
            static const CNode* make(const SNode* sn1, const SNode* sn2, int lev) {
                assert(sn1->view() != sn2->view());
                // distinct keys but potentially the same hash
                auto a1 = (sn1->_hash >> lev) & 63;
                auto a2 = (sn2->_hash >> lev) & 63;
                //printf("a1 a2 %ld %ld\n", a1, a2);
                std::uint64_t flag1 = std::uint64_t{1} << a1;
                if (a1 != a2) {
                    // different hash at lev
                    std::uint64_t flag2 = std::uint64_t{1} << a2;
                    void* b = operator new(sizeof(CNode) + sizeof(BNode) * 2);
                    CNode* c = new (b) CNode;
                    c->bmp = flag1 | flag2;
                    int pos1 = a1 > a2;
                    int pos2 = a2 > a1;
                    c->array[pos1] = sn1;
                    c->array[pos2] = sn2;
                    gc::shade(sn1);
                    gc::shade(sn2);
                    return c;
                } else {
                    // same hash at lev
                    void* b = operator new(sizeof(CNode) + sizeof(BNode) * 1);
                    CNode* c = new (b) CNode;
                    c->bmp = flag1;
                    if (lev + 6 < 64) {
                        c->array[0] = new INode(make(sn1, sn2, lev + 6));
                    } else {
                        LNode* d = new LNode;
                        d->sn = sn1;
                        gc::shade(sn1);
                        d->next = nullptr;
                        LNode* e = new LNode;
                        e->sn = sn2;
                        gc::shade(sn2);
                        e->next = d;
                        c->array[0] = new INode(e);
                    }
                    return c;
                }
            }
            
            virtual std::pair<Result, const SNode*> vlookupA(const INode* i, Query q, int lev, const INode* parent) const override {
                auto [flag, pos] = flagpos(q.hash, lev, bmp);
                if (!(flag & bmp)) {
                    return {NOTFOUND, nullptr};
                }
                const BNode* bn = array[pos];
                return bn->vlookupB(i, q, lev, parent);
            }
            
            virtual std::pair<Result, const SNode*> vinsertA(const INode* i, Query q, int lev, const INode* parent) const override {
                const CNode* cn = this;
                auto [flag, pos] = flagpos(q.hash, lev, cn->bmp);
                if (!(flag & cn->bmp)) {
                    const MNode* expected = this;
                    const MNode* desired = inserted(flag, pos, new (q.view.size()) SNode(q));
                    if (i->main.compare_exchange_strong(expected, desired, RELEASE, RELAXED)) {
                        return {OK, nullptr};
                    } else {
                        return {RESTART, nullptr};
                    }
                } else {
                    return array[pos]->vinsertB(i, q, lev, parent, cn, flag, pos);
                }
            }
            
            virtual std::pair<Result, const SNode*> vremoveA(const INode* i, Query q, int lev, const INode* parent) const override {
                auto [flag, pos] = flagpos(q.hash, lev, bmp);
                if (!(flag & bmp)) {
                    return {NOTFOUND, nullptr};
                }
                const BNode* sub = array[pos];
                assert(sub);
                auto [res, value] = sub->vremoveB(i, q, lev, parent, this, flag, pos);
                if (res == OK) {
                    i->main.load(ACQUIRE)->vremoveC(i, q, lev, parent);
                }
                return {res, value};
            }
            
            virtual void vcleanA(const INode* i, int lev) const override {
                const CNode* m = this;
                const MNode* expected = m;
                const MNode* desired = toCompressed(m, lev);
                i->main.compare_exchange_strong(expected, desired, RELEASE, RELAXED);
            }
            
            virtual bool vcleanParentA(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MNode* m) const override {
                const CNode* cn = this;
                auto [flag, pos] = flagpos(hc, lev, this->bmp);
                if (!(flag & bmp))
                    return true;
                const BNode* sub = this->array[pos];
                if (sub != i)
                    return true;
                return m->vcleanParentB(p, i, hc, lev, m, cn, flag, pos);
            }
            
        }; // struct CNode
        
        const BNode* resurrect(const BNode* m) {
            return m->vresurrectA();
        }
        
        const MNode* toCompressed(const CNode* cn, int lev) {
            int num = __builtin_popcountll(cn->bmp);
            void* a = operator new(sizeof(CNode) + sizeof(BNode*) * num);
            CNode* ncn = new (a) CNode;
            ncn->bmp = cn->bmp;
            for (int i = 0; i != num; ++i) {
                ncn->array[i] = resurrect(cn->array[i]);
                gc::shade(ncn->array[i]);
            }
            return toContracted(ncn, lev);
        }
        
        const MNode* toContracted(const CNode* cn, int lev) {
            int num = __builtin_popcountll(cn->bmp);
            if (lev == 0 || num > 1)
                return cn;
            return cn->array[0]->vtoContractedB(cn, lev);
        }
        
        void clean(const INode* i, int lev) {
            i->main.load(ACQUIRE)->vcleanA(i, lev);
        }
        
        void cleanParent(const INode* p, const INode* i, std::size_t hc, int lev) {
            for (;;) {
                const MNode* m = i->main.load(ACQUIRE); // <-- TODO we only redo this if it is a TNode and therefore final
                const MNode* pm = p->main.load(ACQUIRE); // <-- TODO get this from the failed CAS
                if (pm->vcleanParentA(p, i, hc, lev, m))
                    return;
            }
        }
        
        const MNode* entomb(const SNode* sn) {
            TNode* tn = new TNode;
            tn->sn = sn;
            gc::shade(sn);
            return tn;
        }
        
        
        
        
        
        const BNode* MNode::vresurrectB(const INode* parent) const { return parent; };

        
        
        INode::INode(const MNode* desired) : main(desired) {}
        
        void INode::debug(int lev) const {
            auto p =  main.load(ACQUIRE);
            printf("INode(%lx): ",this->color.load(RELAXED));
            p->debug(lev);
            
        }
        
        void INode::scan(ScanContext& context) const {
            context.push(main);
        }
        
        std::pair<Result, const SNode*> INode::vlookupB(const INode* i, Query q, int lev, const INode* parent) const {
            const INode* sin = this;
            return ilookup(sin, q, lev + 6, i);
        }
        
        std::pair<Result, const SNode*> INode::vinsertB(const INode* i, Query q, int lev, const INode* parent,
                                                           const CNode* cn, std::uint64_t flag, int pos) const {
            return iinsert(this, q, lev + 6, i);
        }
        
        std::pair<Result, const SNode*> INode::vremoveB(const INode* i, Query q, int lev, const INode* parent,
                                                           const CNode* cn, std::uint64_t flag, int pos) const {
            return iremove(this, q, lev + 6, i);
        }
        
        const BNode* INode::vresurrectA() const {
            return this->main.load(ACQUIRE)->vresurrectB(this);
        }
        
        const MNode* INode::vtoContractedB(const CNode* cn, int lev) const {
            return cn;
        }
        
        
        
        SNode::SNode(Query q)
        : _hash(q.hash)
        , _size(q.view.size()) {
            std::memcpy(_data, q.view.data(), _size);
        }
        
        void SNode::debug(int lev) const {
            printf("SNode(%lx,\"%.*s\") %ld %ld\n",
                   this->color.load(RELAXED),
                   (int) _size, 
                   _data,
                   _hash & 63,
                   (_hash >> 6) & 63);
        }
        
        void SNode::shade(ShadeContext& context) const {
            Color expected = context.WHITE();
            color.compare_exchange_strong(expected,
                                          context.BLACK(),
                                          RELAXED,
                                          RELAXED);
        }
        
        void SNode::scan(ScanContext& context) const {
        }
        
        std::pair<Result, const SNode*> SNode::vlookupB(const INode* i, Query q, int lev, const INode* parent) const {
            const SNode* sn = this;
            if (sn->view() == q.view)
                return std::pair(OK, sn);
            else
                return std::pair(NOTFOUND, nullptr);
        }
        
        std::pair<Result, const SNode*> SNode::vinsertB(const INode* i, Query q, int lev, const INode* parent,
                                                    const CNode* cn, std::uint64_t flag, int pos) const {
            if (this->_hash == q.hash && this->view() == q.view) {
                return {OK, this};
            }
            //printf("SNode %lx,%p iinsert with lev=%d\n", this->color.load(RELAXED), this, lev);
            const SNode* nsn = new (q.view.size()) SNode(q);
            const CNode* ncn;
            //if (this->view() != q.view) {
                const INode* nin = new INode(CNode::make(this, nsn, lev + 6));
                ncn = cn->updated(pos, nin);
            //} else {
            //    ncn = cn->updated(pos, nsn);
            //}
            const MNode* expected = cn;
            if (i->main.compare_exchange_strong(expected,
                                                ncn,
                                                RELEASE,
                                                RELAXED)) {
                return {OK, nullptr};
            } else {
                return {RESTART, nullptr};
            }
        }
        
        
        
        std::pair<Result, const SNode*> SNode::vremoveB(const INode* i,Query q, int lev, const INode* parent,
                                                    const CNode* cn, std::uint64_t flag, int pos) const {
            if (this->view() != q.view)
                return std::pair(NOTFOUND, nullptr);
            const CNode* ncn = cn->removed(pos, flag);
            const MNode* cntr = toContracted(ncn, lev);
            const MNode* expected = cn;
            if (i->main.compare_exchange_strong(expected,
                                                cntr,
                                                RELEASE,
                                                RELAXED)) {
                return {OK, this};
            } else {
                return {RESTART, nullptr};
            }
        }
        
        const BNode* SNode::vresurrectA() const {
            return this;
        }
        
        const MNode* SNode::vtoContractedB(const CNode* cn, int lev) const {
            const SNode* sn = this;
            return entomb(sn);
        }
        
        
        
        
        
        void TNode::debug(int lev) const {
            printf("TNode(%lx): ", this->color.load(RELAXED));
            sn->debug(lev);
        }
        
        void TNode::scan(ScanContext& context) const {
            context.push(sn);
        }
        
        std::pair<Result, const SNode*> TNode::vlookupA(const INode* i, Query q, int lev, const INode* parent) const {
            clean(parent, lev - 6);
            return {RESTART, nullptr};
        }
        
        std::pair<Result, const SNode*> TNode::vinsertA(const INode* i, Query q, int lev, const INode* parent) const {
            clean(parent, lev - 6);
            return {RESTART, nullptr};
        }
        
        std::pair<Result, const SNode*> TNode::vremoveA(const INode* i, Query q, int lev, const INode* parent) const {
            clean(parent, lev - 6);
            return {RESTART, nullptr};
        }
        
        const BNode* TNode::vresurrectB(const INode* parent) const {
            return sn;
        }
        
        bool TNode::vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                   const MNode* m,
                                   const CNode* cn, std::uint64_t flag, int pos) const {
            const CNode* ncn = cn->updated(pos, this->sn);
            const MNode* expected = cn;
            const MNode* desired = toContracted(ncn, lev);
            return p->main.compare_exchange_strong(expected,
                                                   desired,
                                                   RELEASE,
                                                   RELAXED);
        }
        
        void TNode::vremoveC(const INode* i, Query q, int lev, const INode* parent) const {
            cleanParent(parent, i, q.hash, lev - 6);
        }
        
        
        
        Ctrie::Ctrie()
        : root(new INode(new CNode)) {
        }
        
        void Ctrie::debug() {
            printf("%p: Ctrie\n", this);
            root->debug(0);
        }
        
        void Ctrie::scan(ScanContext& context) const {
            context.push(root);
        }
        
        std::pair<Result, const SNode*> ilookup(const INode* i, Query q, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vlookupA(i, q, lev, parent);
        }
        
        const SNode* Ctrie::lookup(Query q) {
            for (;;) {
                INode* r = root;
                auto [res, v] = ilookup(r, q, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v;
            }
        }
        
        std::pair<Result, const SNode*> iinsert(const INode* i, Query q, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vinsertA(i, q, lev, parent);
        }
        
        const SNode* Ctrie::emplace(Query q) {
            for (;;) {
                INode* r = root;
                auto [res, v2] = iinsert(r, q, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v2;
            }
        }
        
        std::pair<Result, const SNode*> iremove(const INode* i, Query q, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vremoveA(i, q, lev, parent);
        }
        
        const SNode* Ctrie::remove(const SNode* k) {
            for (;;) {
                INode* r = root;
                auto [res, v] = iremove(r, Query{k->_hash, k->view()}, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v;
            }
        }
        
        
        
    } // namespace _ctrie
    
} // namespace gc
