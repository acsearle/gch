//
//  ctrie.hpp
//  gch
//
//  Created by Antony Searle on 20/4/2024.
//

#ifndef ctrie_hpp
#define ctrie_hpp

#include "gc.hpp"

namespace gc {
    
    namespace _ctrie {
        
        template<typename Key, typename T>
        struct Ctrie : gc::Object {
            
            struct ANode; // Any       : Object
            struct BNode; // Branch    : Any
            struct MNode; // Main      : Any
            struct INode; // Indirect  : Branch
            struct SNode; // Singleton : Branch
            struct CNode; // Ctrie     : Main
            struct LNode; // List      : Main
            struct TNode; // Tomb      : Main

            enum Result {
                RESTART = 0,
                OK = 1,
            };
            
            struct ANode : Object {
                virtual void debug(int lev) const = 0;
            };
            
            struct BNode : ANode {
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent,
                                                   const CNode* cn, std::uint64_t flag, int pos) const = 0;
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent,
                                                               const CNode* cn, std::uint64_t flag, int pos) const = 0;
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent,
                                                    const CNode* cn, std::uint64_t flag, int pos) const  = 0;
                virtual const BNode* _resurrect() const = 0;
                virtual const MNode* _contract(const CNode* parent, int lev) const = 0;
                
            }; // struct BNode
            
            struct MNode : ANode {
                
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent) const = 0;
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T value, int lev, const INode* parent) const = 0;
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent) const = 0;
                virtual void _erase2(const INode* i, Key k, int lev, const INode* parent) const {}
                virtual const BNode* _resurrect(const INode* parent) const { return parent; }
                virtual void vcleanA(const INode* i, int lev) const {}
                virtual bool vcleanParentA(const INode* p, const INode* i, std::size_t hc, int lev,
                                           const MNode* m) const { return true; }
                virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                           const MNode* m,
                                           const CNode* cn, std::uint64_t flag, int pos) const { return true; }
            }; // struct MNode
            
            struct INode : BNode {
                mutable Atomic<StrongPtr<const MNode>> main;
                explicit INode(const MNode* desired) : main(desired) {}
                virtual void debug(int lev) const override {
                    auto p =  main.load(ACQUIRE);
                    printf("INode(%lx): ",this->color.load(RELAXED));
                    p->debug(lev);
                }
                virtual void scan(ScanContext& context) const override {
                    context.push(main);
                }
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent,
                                                   const CNode* cn, std::uint64_t flag, int pos) const override {
                    return Ctrie::_find(this, k, lev + 6, i);
                }
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent,
                                                               const CNode* cn, std::uint64_t flag, int pos) const override {
                    return Ctrie::_insert_or_assign(this, k, v, lev + 6, i);
                }
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent,
                                                    const CNode* cn, std::uint64_t flag, int pos) const override{
                    return Ctrie::_erase(this, k, lev + 6, i);
                }
                virtual const BNode* _resurrect() const override {
                    return this->main.load(ACQUIRE)->_resurrect(this);
                }
                virtual const MNode* _contract(const CNode* cn, int lev) const override {
                    return cn;
                }
                
                
            }; // struct INode
            
            struct SNode : BNode {
                Key key;
                T value;
                SNode(Key k, T v) : key(k), value(v) {}
                virtual void shade(ShadeContext& context) const override {
                    Color expected = context.WHITE();
                    color.compare_exchange_strong(expected,
                                                  context.BLACK(),
                                                  RELAXED,
                                                  RELAXED);
                }
                virtual void debug(int lev) const override {
                    printf("SNode(%lx)\n", this->color.load(RELAXED));
                }
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent,
                                                   const CNode* cn, std::uint64_t flag, int pos) const override {
                    if (this->key == k) {
                        return {OK, value};
                    } else {
                        return {OK, nullptr};
                    }
                }
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent,
                                                               const CNode* cn, std::uint64_t flag, int pos) const override {
                    const SNode* nsn = new SNode(k, v);
                    const CNode* ncn;
                    if (this->key != k) {
                        const INode* nin = new INode(CNode::make(this, nsn, lev + 6));
                        ncn = cn->updated(pos, nin);
                    } else {
                        ncn = cn->updated(pos, nsn);
                    }
                    const MNode* expected = cn;
                    if (i->main.compare_exchange_strong(expected,
                                                        ncn,
                                                        RELEASE,
                                                        RELAXED)) {
                        return {OK, value};
                    } else {
                        return {RESTART, nullptr};
                    }
                }
                
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent,
                                                    const CNode* cn, std::uint64_t flag, int pos) const override  {
                    if (this->key != k)
                        return std::pair(OK, nullptr);
                    const CNode* ncn = cn->removed(pos, flag);
                    const MNode* cntr = toContracted(ncn, lev);
                    const MNode* expected = cn;
                    if (i->main.compare_exchange_strong(expected,
                                                        cntr,
                                                        RELEASE,
                                                        RELAXED)) {
                        return {OK, this->value};
                    } else {
                        return {RESTART, nullptr};
                    }
                }
                
                virtual const BNode* _resurrect() const override {
                    return this;
                }
                virtual const MNode* _contract(const CNode* cn, int lev) const override {
                    const SNode* sn = this;
                    return entomb(sn);
                }
                
            }; // struct SNode
            
            struct TNode : MNode {
                const SNode* sn;

                virtual void debug(int lev) const override {
                    printf("TNode(%lx): ", this->color.load(RELAXED));
                    sn->debug(lev);
                }
                virtual void scan(ScanContext& context) const override {
                    context.push(sn);
                }
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent) const override {
                    clean(parent, lev - 6);
                    return {RESTART, nullptr};
                }
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T value, int lev, const INode* parent) const override {
                    clean(parent, lev - 6);
                    return {RESTART, nullptr};
                }
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent) const override {
                    clean(parent, lev - 6);
                    return {RESTART, nullptr};
                }
                virtual void _erase2(const INode* i, Key k, int lev, const INode* parent) const override {
                    cleanParent(parent, i, k->_hash, lev - 6);
                }
                virtual bool vcleanParentB(const INode* p, const INode* i, std::size_t hc, int lev,
                                           const MNode* m,
                                           const CNode* cn, std::uint64_t flag, int pos) const override {
                    const CNode* ncn = cn->updated(pos, this->sn);
                    const MNode* expected = cn;
                    const MNode* desired = toContracted(ncn, lev);
                    return p->main.compare_exchange_strong(expected,
                                                           desired,
                                                           RELEASE,
                                                           RELAXED);
                }
                virtual const BNode* _resurrect(const INode* parent) const override {
                    return sn;
                }
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
                std::pair<Result, T> lookup(Key k) const {
                    const LNode* ln = this;
                    for (;;) {
                        if (!ln)
                            return {OK, nullptr};
                        if (ln->sn->key == k)
                            return {OK, ln->sn->value};
                        ln = ln->next;
                    }
                }
                const MNode* inserted(Key k, T v) const  {
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
                std::pair<const LNode*, T> removed(Key k) const {
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
                
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent) const override {
                    return lookup(k);
                }
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent) const override {
                    // printf("LNode %lx,%p iinsert\n", this->color.load(RELAXED), this);
                    const MNode* expected = this;
                    if (i->main.compare_exchange_strong(expected,
                                                        inserted(k, v),
                                                        RELEASE,
                                                        RELAXED)) {
                        return {OK, nullptr};
                    } else {
                        return {RESTART, nullptr};
                    }
                }
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent) const override {
                    const LNode* ln = this;
                    auto [nln, v] = ln->removed(k);
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

                static std::pair<std::uint64_t, int> flagpos(std::size_t hash, int lev, std::uint64_t bmp) {
                    auto a = (hash >> lev) & 63;
                    std::uint64_t flag = std::uint64_t{1} << a;
                    int pos = __builtin_popcountll(bmp & (flag - 1));
                    return std::pair(flag, pos);
                }
                
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
                        CNode* c = new (extra_val_t{sizeof(BNode) * 2}) CNode;
                        c->bmp = flag1 | flag2;
                        int pos1 = a1 > a2;
                        int pos2 = a2 > a1;
                        c->array[pos1] = sn1;
                        c->array[pos2] = sn2;
                        return c;
                    } else {
                        // same hash at lev
                        CNode* c = new (extra_val_t{sizeof(BNode)}) CNode;
                        c->bmp = flag1;
                        if (lev + 6 < 64) {
                            c->array[0] = new INode(make(sn1, sn2, lev + 6));
                        } else {
                            // true hash collision,
                            // TODO: should we check for this immediately before we
                            // make the chain of CNodes?  I think it's benign to have
                            // LNodes at any level so long as they all share the hash
                            // value; we may need to add a CNode::make flavor with
                            // INodes->LNodes though?
                            LNode* d = new LNode;
                            d->sn = sn1;
                            d->next = nullptr;
                            LNode* e = new LNode;
                            e->sn = sn2;
                            e->next = d;
                            c->array[0] = new INode(e);
                        }
                        return c;
                    }
                }

                CNode() : bmp{0} {}

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
                virtual void scan(ScanContext& context) const override  {
                    int num = __builtin_popcountll(this->bmp);
                    for (int i = 0; i != num; ++i) {
                        this->array[i]->scan_weak(context);
                    }
                }
                
                const CNode* inserted(std::uint64_t flag, int pos, const BNode* child) const {
                    //printf("CNode inserted\n");
                    auto n = __builtin_popcountll(bmp);
                    CNode* b = new (extra_val_t{sizeof(BNode*) * (n + 1)}) CNode;
                    assert(!(this->bmp & flag));
                    b->bmp = this->bmp | flag;
                    std::memcpy(b->array, this->array, sizeof(BNode*) * pos);
                    b->array[pos] = child;
                    std::memcpy(b->array + pos + 1, this->array + pos, sizeof(BNode*) * (n - pos));
                    ShadeContext context;
                    context._white = global.white.load(RELAXED);
                    for (int i = 0; i != n + 1; ++i)
                        b->array[i]->shade(context);
                    return b;
                }
                
                const CNode* updated(int pos, const BNode* child) const {
                    //printf("CNode updated\n");
                    auto n = __builtin_popcountll(bmp);
                    CNode* b = new (extra_val_t{sizeof(BNode*) * n}) CNode;
                    b->bmp = this->bmp;
                    std::memcpy(b->array, this->array, sizeof(BNode*) * n);
                    b->array[pos] = child;
                    ShadeContext context;
                    context._white = global.white.load(RELAXED);
                    for (int i = 0; i != n; ++i)
                        b->array[i]->shade(context);
                    return b;
                }
                
                const CNode* removed(int pos, std::uint64_t flag) const {
                    assert(this->bmp & flag);
                    assert(__builtin_popcountll((flag - 1) & this->bmp) == pos);
                    auto n = __builtin_popcountll(bmp);
                    assert(pos < n);
                    CNode* b = new (extra_val_t{sizeof(BNode*) * (n - 1)}) CNode;
                    b->bmp = this->bmp ^ flag;
                    std::memcpy(b->array, this->array, sizeof(BNode*) * pos);
                    std::memcpy(b->array + pos, this->array + pos + 1, sizeof(BNode*) * (n - 1 - pos));
                    ShadeContext context;
                    context._white = global.white.load(RELAXED);
                    for (int i = 0; i != n - 1; ++i)
                        b->array[i]->shade_weak(context);
                    return b;
                }
                
                virtual std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent) const override {
                    const CNode* cn = this;
                    auto [flag, pos] = flagpos(k->_hash, lev, cn->bmp);
                    if (!(flag & cn->bmp)) {
                        return {OK, nullptr};
                    } else {
                        return array[pos]->_find(i, k, lev, parent, cn, flag, pos);
                    }
                }
                
                virtual std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent) const override {
                    const CNode* cn = this;
                    auto [flag, pos] = flagpos(k->_hash, lev, cn->bmp);
                    if (!(flag & cn->bmp)) {
                        const MNode* expected = this;
                        const SNode* sn =  new SNode(k, v);
                        const MNode* desired = inserted(flag, pos, sn);
                        if (i->main.compare_exchange_strong(expected, desired, RELEASE, RELAXED)) {
                            return {OK, sn->value};
                        } else {
                            return {RESTART, nullptr};
                        }
                    } else {
                        return array[pos]->_insert_or_assign(i, k, v, lev, parent, cn, flag, pos);
                    }
                }
                
                virtual std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent) const override {
                    auto [flag, pos] = flagpos(k->_hash, lev, bmp);
                    if (!(flag & bmp)) {
                        return {OK, nullptr};
                    }
                    const BNode* sub = array[pos];
                    assert(sub);
                    auto [res, value] = sub->_erase(i, k, lev, parent, this, flag, pos);
                    if (res == OK) {
                        i->main.load(ACQUIRE)->_erase2(i, k, lev, parent);
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
                        
            static const BNode* resurrect(const BNode* m) {
                return m->_resurrect();
            }
            
            static const MNode* toCompressed(const CNode* cn, int lev) {
                int num = __builtin_popcountll(cn->bmp);
                CNode* ncn = new (extra_val_t{sizeof(BNode*) * num}) CNode;
                ncn->bmp = cn->bmp;
                ShadeContext context;
                context._white = global.white.load(RELAXED);
                for (int i = 0; i != num; ++i) {
                    ncn->array[i] = resurrect(cn->array[i]);
                    ncn->array[i]->shade(context);
                }
                return toContracted(ncn, lev);
            }
            
            static const MNode* toContracted(const CNode* cn, int lev) {
                int num = __builtin_popcountll(cn->bmp);
                if (lev == 0 || num > 1)
                    return cn;
                return cn->array[0]->_contract(cn, lev);
            }
            
            static void clean(const INode* i, int lev) {
                i->main.load(ACQUIRE)->vcleanA(i, lev);
            }
            
            static void cleanParent(const INode* p, const INode* i, std::size_t hc, int lev) {
                for (;;) {
                    const MNode* m = i->main.load(ACQUIRE); // <-- TODO we only redo this if it is a TNode and therefore final
                    const MNode* pm = p->main.load(ACQUIRE); // <-- TODO get this from the failed CAS
                    if (pm->vcleanParentA(p, i, hc, lev, m))
                        return;
                }
            }
            
            static const MNode* entomb(const SNode* sn) {
                TNode* tn = new TNode;
                tn->sn = sn;
                gc::shade(sn);
                return tn;
            }

            static std::pair<Result, T> _find(const INode* i, Key k, int lev, const INode* parent) {
                return i->main.load(ACQUIRE)->_find(i, k, lev, parent);
            }

            static std::pair<Result, T> _insert_or_assign(const INode* i, Key k, T v, int lev, const INode* parent) {
                return i->main.load(ACQUIRE)->_insert_or_assign(i, k, v, lev, parent);
            }
            
            static std::pair<Result, T> _erase(const INode* i, Key k, int lev, const INode* parent) {
                return i->main.load(ACQUIRE)->_erase(i, k, lev, parent);
            }
                        
            Ctrie() : root(new INode(new CNode)) {
            }
            
            void debug() {
                printf("%p: Ctrie\n", this);
                root->debug(0);
            }
            
            void scan(ScanContext& context) const {
                context.push(root);
            }
            
            T find(Key k) {
                for (;;) {
                    INode* r = root;
                    auto [res, v] = _find(r, k, 0, nullptr);
                    if (res == RESTART)
                        continue;
                    return v;
                }
            }
            
            T insert_or_assign(Key k, T v) {
                for (;;) {
                    INode* r = root;
                    auto [res, v2] = _insert_or_assign(r, k, v, 0, nullptr);
                    if (res == RESTART)
                        continue;
                    assert(v2);
                    return v2;
                }
            }
            
            T erase(Key k) {
                for (;;) {
                    INode* r = root;
                    auto [res, v] = _erase(r, k, 0, nullptr);
                    if (res == RESTART)
                        continue;
                    return v;
                }
            }
                        
            INode* root;
            
            
           
            


        }; // Ctrie
        
        

        
        
        
        
        
        
        
        
       
        
        
        
        
        
        
        
        
        
    } // _ctrie
    
    using _ctrie::Ctrie;
    
} // namespace gc

#endif /* ctrie_hpp */
