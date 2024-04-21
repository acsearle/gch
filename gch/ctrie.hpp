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
    
    // Concurrent hash array mapped trie
    //
    // https://en.wikipedia.org/wiki/Ctrie
    //
    // Prokopec, A., Bronson N., Bagwell P., Odersky M. (2012) Concurrent Tries
    //     with Efficient Non-Blocking Snapshots
    
    // A concurrent map with good worst-case performance
    //
    // Use case is weak set of interned strings
    
    struct Ctrie : Object {
        
        enum Result {
            NOTFOUND = -1,
            RESTART = 0,
            OK = 1,
        };
        
        struct MainNode;
        struct BranchNode;

        struct CNode;
        struct INode;
        struct LNode;
        struct SNode;
        struct TNode;

        struct MainNode : Object {
            virtual void print() const = 0;
            virtual std::pair<Result, Object*> vlookupA(const INode* i, const String* k, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, Object*> vinsertA(const INode* i, const String* k, Object* v, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, Object*> vremoveA(const INode* i, const String* k, int lev, const INode* parent) const = 0;
            virtual void vremoveC(const INode* i, const String* k, int lev, const INode* parent) const {};
            virtual const BranchNode* vresurrectB(const INode* parent) const { return parent; };
            virtual void vcleanA(const INode* i, int lev) const {}
            virtual bool vcleanParentA(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MainNode* m) const { return true; }
            virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MainNode* m,
                                       const CNode* cn, std::uint64_t flag, int pos) const { return true; }
        }; // struct MainNode
        
        struct BranchNode : Object {
            virtual void print() const = 0;
            virtual std::pair<Result, Object*> vlookupB(const INode* i, const String* k, int lev, const INode* parent) const = 0;
            virtual std::pair<Result, Object*> vinsertB(const INode* i, const String* k, Object* v, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const = 0;
            virtual std::pair<Result, Object*> vremoveB(const INode* i, const String* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const  = 0;
            virtual const BranchNode* vresurrectA() const = 0;
            virtual const MainNode* vtoContractedB(const CNode* parent, int lev) const = 0;
            
        }; // struct BranchNode
        
        struct INode : BranchNode {
            
            mutable Atomic<StrongPtr<const MainNode>> main;
            
            explicit INode(const MainNode* desired)
            : main(desired) {
            }
            
            virtual void print() const override {
                auto p =  main.load(ACQUIRE);
                printf("INode(%lx): ",this->color.load(RELAXED));
                p->print();
                
            }
            
            virtual void scan(ScanContext& context) const override {
                context.push(main);
            }
            
            virtual std::pair<Result, Object*> vlookupB(const INode* i, const String* k, int lev, const INode* parent) const override {
                const INode* sin = this;
                return ilookup(sin, k, lev+6, i);
            }
            
            virtual std::pair<Result, Object*> vinsertB(const INode* i, const String* k, Object* v, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override {
                // printf("INode %p iinsert\n", this);
                return iinsert(this, k, v, lev + 6, i);
            }
            
          

            virtual std::pair<Result, Object*> vremoveB(const INode* i, const String* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override {
                return iremove(this, k, lev+6, i);
            }

            virtual const BranchNode* vresurrectA() const override {
                return this->main.load(ACQUIRE)->vresurrectB(this);
            }
            
            virtual const MainNode* vtoContractedB(const CNode* cn, int lev) const override {
                return cn;
            }
            
        }; // struct INode
        
        struct SNode : BranchNode {
            
            const String* key;
            Object* value;
            
            SNode(const String* k, Object* v) 
            : key(k), value(v) {
                gc::shade(k);
                gc::shade(v);
            }
            
            virtual void print() const override {
                printf("SNode(%lx,%p, %p) %ld %ld\n", this->color.load(RELAXED), key, value, key->_hash & 63, (key->_hash >> 6) & 63);
            }

            virtual void scan(ScanContext& context) const override {
                context.push(key);
                context.push(value);
            }
            
            virtual std::pair<Result, Object*> vlookupB(const INode* i, const String* k, int lev, const INode* parent) const override {
                const SNode* sn = this;
                if (sn->key == k)
                    return std::pair(OK, value);
                else
                    return std::pair(NOTFOUND, nullptr);
            }
            
            virtual std::pair<Result, Object*> vinsertB(const INode* i, const String* k, Object* v, int lev, const INode* parent,
                                                       const CNode* cn, std::uint64_t flag, int pos) const override {
                // printf("SNode %lx,%p iinsert with lev=%d\n", this->color.load(RELAXED), this, lev);
                const SNode* nsn = new SNode(k, v);
                const CNode* ncn;
                if (this->key != k) {
                    const INode* nin = new INode(CNode::make(this, nsn, lev + 6));
                    ncn = cn->updated(pos, nin);
                } else {
                    ncn = cn->updated(pos, nsn);
                }
                const MainNode* expected = cn;
                if (i->main.compare_exchange_strong(expected,
                                                    ncn,
                                                    RELEASE,
                                                    RELAXED)) {
                    return {OK, nullptr};
                } else {
                    return {RESTART, nullptr};
                }
            }
            
            
            
            virtual std::pair<Result, Object*> vremoveB(const INode* i, const String* k, int lev, const INode* parent,
                                                        const CNode* cn, std::uint64_t flag, int pos) const override {
                if (this->key != k)
                    return std::pair(NOTFOUND, nullptr);
                const CNode* ncn = cn->removed(pos, flag);
                const MainNode* cntr = toContracted(ncn, lev);
                const MainNode* expected = cn;
                if (i->main.compare_exchange_strong(expected,
                                                    cntr,
                                                    RELEASE,
                                                    RELAXED)) {
                    return {OK, this->value};
                } else {
                    return {RESTART, nullptr};
                }
            }
            
            virtual const BranchNode* vresurrectA() const override {
                return this;
            }

            virtual const MainNode* vtoContractedB(const CNode* cn, int lev) const override {
                const SNode* sn = this;
                return entomb(sn);
            }
            
        }; // struct SNode
        
        struct TNode : MainNode {
            
            const SNode* sn;
            
            virtual void print() const override {
                printf("TNode(%lx): ", this->color.load(RELAXED));
                sn->print();
            }
            
            virtual void scan(ScanContext& context) const override {
                context.push(sn);
            }
            
            virtual std::pair<Result, Object*> vlookupA(const INode* i, const String* k, int lev, const INode* parent) const override {
                clean(parent, lev - 6);
                return {RESTART, nullptr};
            }
            
            virtual std::pair<Result, Object*> vinsertA(const INode* i, const String* k, Object* v, int lev, const INode* parent) const override {
                clean(parent, lev - 6);
                return {RESTART, nullptr};
            }

            virtual std::pair<Result, Object*> vremoveA(const INode* i, const String* k, int lev, const INode* parent) const override {
                clean(parent, lev - 6);
                return {RESTART, nullptr};
            }
            
            virtual const BranchNode* vresurrectB(const INode* parent) const override {
                return sn;
            }
                                    
            virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MainNode* m,
                                       const CNode* cn, std::uint64_t flag, int pos) const override {
                const CNode* ncn = cn->updated(pos, this->sn);
                const MainNode* expected = cn;
                const MainNode* desired = toContracted(ncn, lev);
                return p->main.compare_exchange_strong(expected,
                                                       desired,
                                                       RELEASE,
                                                       RELAXED);
            }
            
            virtual void vremoveC(const INode* i, const String* k, int lev, const INode* parent) const override {
                cleanParent(parent, i, k->_hash, lev - 6);
            }
            
        }; // struct TNode
        
        
        struct LNode : MainNode {
            const SNode* sn;
            const LNode* next;
            
            virtual void print() const override {
                printf("LNode(%lx,%p): ", this->color.load(RELAXED), sn);
                if (next)
                    next->print();
                else
                    printf("\n");
            }
            
            virtual void scan(ScanContext& context) const override {
                context.push(sn);
                context.push(next);
            }
            
            std::pair<Result, Object*> lookup(const String* k) const {
                const LNode* ln = this;
                for (;;) {
                    if (!ln)
                        return {NOTFOUND, nullptr};
                    if (ln->sn->key == k)
                        return {OK, ln->sn->value};
                    ln = ln->next;
                }
            }
            
            virtual std::pair<Result, Object*> vlookupA(const INode* i, const String* k, int lev, const INode* parent) const override {
                const LNode* ln = this;
                return ln->lookup(k);
            }
            
            const MainNode* inserted(const String* k, Object* v) const {
                const LNode* a = this;
                for (;;) {
                    if (a->sn->key == k) {
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
                                d->sn = new SNode(k, v);
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
                        b->sn = new SNode(k, v);
                        b->next = this;
                        gc::shade(b->next);
                        return b;
                    }
                }
            }
            
            std::pair<const LNode*, Object*> removed(const String* k) const {
                if (this->sn->key == k)
                    return {this->next, this->sn->value};
                const LNode* a = this->next;
                for (;;) {
                    if (a == nullptr) {
                        // Not found at all
                        return {this, nullptr};
                    }
                    if (a->sn->key != k) {
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
                                return {c, a->sn->value};
                            }
                            LNode* e = new LNode;
                            d->next = e;
                            d = e;
                        }
                    }
                }
            }
            
            
            virtual std::pair<Result, Object*> vinsertA(const INode* i, const String* k, Object* v, int lev, const INode* parent) const override {
                // printf("LNode %lx,%p iinsert\n", this->color.load(RELAXED), this);
                const MainNode* expected = this;
                if (i->main.compare_exchange_strong(expected,
                                                    inserted(k, v),
                                                    RELEASE,
                                                    RELAXED)) {
                    return {OK, nullptr};
                } else {
                    return {RESTART, nullptr};
                }
            }
            
            virtual std::pair<Result, Object*> vremoveA(const INode* i, const String* k, int lev, const INode* parent) const override {
                const LNode* ln = this;
                auto [nln, v] = ln->removed(k);
                assert(nln && nln->sn);
                const MainNode* expected = ln;
                const MainNode* desired = nln->next ? nln : entomb(nln->sn);
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
        
        
        struct CNode : MainNode {
            
            std::uint64_t bmp;
            const BranchNode* array[0];
            
            virtual void print() const override {
                printf("CNode(%lx,%#llx):\n", this->color.load(RELAXED), bmp);
                int j = 0;
                for (int i = 0; i != 64; ++i) {
                    std::uint64_t flag = std::uint64_t{1} << i;
                    if (bmp & flag) {
                        printf("    [%d]: ", i);
                        array[j]->print();
                        j++;
                    }
                }
                printf("]\n");
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
            
            const CNode* inserted(std::uint64_t flag, int pos, const BranchNode* child) const {
                //printf("CNode inserted\n");
                auto n = __builtin_popcountll(bmp);
                void* a = operator new(sizeof(CNode) + sizeof(BranchNode*) * (n + 1));
                CNode* b = new (a) CNode;
                assert(!(this->bmp & flag));
                b->bmp = this->bmp | flag;
                std::memcpy(b->array, this->array, sizeof(BranchNode*) * pos);
                b->array[pos] = child;
                std::memcpy(b->array + pos + 1, this->array + pos, sizeof(BranchNode*) * (n - pos));
                for (int i = 0; i != n + 1; ++i)
                    gc::shade(b->array[i]);
                return b;
            }
            
            const CNode* updated(int pos, const BranchNode* child) const {
                //printf("CNode updated\n");
                auto n = __builtin_popcountll(bmp);
                void* a = operator new(sizeof(CNode) + sizeof(BranchNode*) * n);
                CNode* b = new (a) CNode;
                b->bmp = this->bmp;
                std::memcpy(b->array, this->array, sizeof(BranchNode*) * n);
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
                void* a = operator new(sizeof(CNode) + sizeof(BranchNode*) * n - 1);
                CNode* b = new (a) CNode;
                b->bmp = this->bmp ^ flag;
                std::memcpy(b->array, this->array, sizeof(BranchNode*) * pos);
                std::memcpy(b->array + pos, this->array + pos + 1, sizeof(BranchNode*) * (n - 1 - pos));
                for (int i = 0; i != n - 1; ++i)
                    gc::shade(b->array[i]);
                return b;
            }

            CNode() : bmp{0} {}
            
            static const CNode* make(const SNode* sn1, const SNode* sn2, int lev) {
                assert(sn1->key != sn2->key);
                // distinct keys but potentially the same hash
                auto a1 = (sn1->key->_hash >> lev) & 63;
                auto a2 = (sn2->key->_hash >> lev) & 63;
                //printf("a1 a2 %ld %ld\n", a1, a2);
                std::uint64_t flag1 = std::uint64_t{1} << a1;
                if (a1 != a2) {
                    // different hash at lev
                    std::uint64_t flag2 = std::uint64_t{1} << a2;
                    void* b = operator new(sizeof(CNode) + sizeof(BranchNode) * 2);
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
                    void* b = operator new(sizeof(CNode) + sizeof(BranchNode) * 1);
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
            
            virtual std::pair<Result, Object*> vlookupA(const INode* i, const String* k, int lev, const INode* parent) const override {
                auto [flag, pos] = flagpos(k->_hash, lev, bmp);
                if (!(flag & bmp)) {
                    return {NOTFOUND, nullptr};
                }
                const BranchNode* bn = array[pos];
                return bn->vlookupB(i, k, lev, parent);
            }
            
            virtual std::pair<Result, Object*> vinsertA(const INode* i, const String* k, Object* v, int lev, const INode* parent) const override {
                const CNode* cn = this;
                auto [flag, pos] = flagpos(k->_hash, lev, cn->bmp);
                if (!(flag & cn->bmp)) {
                    const MainNode* expected = this;
                    const MainNode* desired = inserted(flag, pos, new SNode(k, v));
                    if (i->main.compare_exchange_strong(expected, desired, RELEASE, RELAXED)) {
                        return {OK, nullptr};
                    } else {
                        return {RESTART, nullptr};
                    }
                } else {
                    return array[pos]->vinsertB(i, k, v, lev, parent, cn, flag, pos);
                }
            }
                        
            virtual std::pair<Result, Object*> vremoveA(const INode* i, const String* k, int lev, const INode* parent) const override {
                auto [flag, pos] = flagpos(k->_hash, lev, bmp);
                if (!(flag & bmp)) {
                    return {NOTFOUND, nullptr};
                }
                const BranchNode* sub = array[pos];
                assert(sub);
                auto [res, value] = sub->vremoveB(i, k, lev, parent, this, flag, pos);
                if (res == OK) {
                    i->main.load(ACQUIRE)->vremoveC(i, k, lev, parent);
                }
                return {res, value};
            }
            
            virtual void vcleanA(const INode* i, int lev) const override {
                const CNode* m = this;
                const MainNode* expected = m;
                const MainNode* desired = toCompressed(m, lev);
                i->main.compare_exchange_strong(expected, desired, RELEASE, RELAXED);
            }
            
            virtual bool vcleanParentA(const INode* p, const INode* i, std::size_t hc, int lev,
                                       const MainNode* m) const override {
                const CNode* cn = this;
                auto [flag, pos] = flagpos(hc, lev, this->bmp);
                if (!(flag & bmp))
                    return true;
                const BranchNode* sub = this->array[pos];
                if (sub != i)
                    return true;
                return m->vcleanParentB(p, i, hc, lev, m, cn, flag, pos);
            }
                        
        }; // struct CNode
        
        static const BranchNode* resurrect(const BranchNode* m) {
            return m->vresurrectA();
        }
                
        static const MainNode* toCompressed(const CNode* cn, int lev) {
            int num = __builtin_popcountll(cn->bmp);
            void* a = operator new(sizeof(CNode) + sizeof(BranchNode*) * num);
            CNode* ncn = new (a) CNode;
            ncn->bmp = cn->bmp;
            for (int i = 0; i != num; ++i) {
                ncn->array[i] = resurrect(cn->array[i]);
                gc::shade(ncn->array[i]);
            }
            return toContracted(ncn, lev);
        }
        
        static const MainNode* toContracted(const CNode* cn, int lev) {
            int num = __builtin_popcountll(cn->bmp);
            if (lev == 0 || num > 1)
                return cn;
            return cn->array[0]->vtoContractedB(cn, lev);
        }
        
        static void clean(const INode* i, int lev) {
            i->main.load(ACQUIRE)->vcleanA(i, lev);
        }
        
        static void cleanParent(const INode* p, const INode* i, std::size_t hc, int lev) {
            for (;;) {
                const MainNode* m = i->main.load(ACQUIRE); // <-- TODO we only redo this if it is a TNode and therefore final
                const MainNode* pm = p->main.load(ACQUIRE); // <-- TODO get this from the failed CAS
                if (pm->vcleanParentA(p, i, hc, lev, m))
                    return;
            }
        }
        
        static const MainNode* entomb(const SNode* sn) {
            TNode* tn = new TNode;
            tn->sn = sn;
            gc::shade(sn);
            return tn;
        }
        
        
        
                
        INode* root;
        
        Ctrie()
        : root(new INode(new CNode)) {
        }
        
        void print() {
            printf("%p: Ctrie\n", this);
            root->print();
        }
        
        virtual void scan(ScanContext& context) const override {
            context.push(root);
        }
        
        static std::pair<Result, Object*> ilookup(const INode* i, const String* k, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vlookupA(i, k, lev, parent);
        }
        
        Object* lookup(const String* k) {
            for (;;) {
                INode* r = root;
                auto [res, v] = ilookup(r, k, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v;
            }
        }
        
        static std::pair<Result, Object*> iinsert(const INode* i, const String* k, Object* v, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vinsertA(i, k, v, lev, parent);
        }
        
        Object* insert(const String* k, Object* v) {
            for (;;) {
                INode* r = root;
                auto [res, v2] = iinsert(r, k, v, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v2;
            }
        }
        
        static std::pair<Result, Object*>
        iremove(const INode* i, const String* k, int lev, const INode* parent) {
            return i->main.load(ACQUIRE)->vremoveA(i, k, lev, parent);
        }
        
        Object* remove(const String* k) {
            for (;;) {
                INode* r = root;
                auto [res, v] = iremove(r, k, 0, nullptr);
                if (res == RESTART)
                    continue;
                return v;
            }
        }
        
    }; // struct Ctrie
    
} // namespae gc

#endif /* ctrie_hpp */
