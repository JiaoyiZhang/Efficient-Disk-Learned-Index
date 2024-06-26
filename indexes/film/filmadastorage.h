

//
// Created by chaochao on 2021/12/23.
//

#ifndef EXPERIMENTCC12_FILMADASTORAGE_H
#define EXPERIMENTCC12_FILMADASTORAGE_H

#include <fcntl.h>
#include <unistd.h>
#include <memory.h>
//#define _GNU_SOURCE
#include <vector>
#include "filmadalru.h"
#include "film.h"

using namespace std;
typedef unsigned short lruOff_type;
typedef unsigned short int pageOff_type;
typedef unsigned int pageid_type;
namespace filmstorage {
    typedef std::map<std::string,double> infomap;
    template <class key_type,class data_type,class index_type, class lru_type,class disk_type>
    class filmmemory{
    public:
        int totalnum;
        pageid_type inpageid = 0;
        double threshold;
        double reserveMem;
        index_type *index;  //
        lru_type *lru;
        vector<pair<pageid_type ,pageOff_type>> evicttable;  // pageid , offset in page
        vector<key_type> evictkey;   // the evicted key, used in debug
        vector<pair<pageid_type ,pageOff_type>*> evictPoss;

        filmmemory()=default;


        filmmemory( unsigned long int numkey,double Threshold, index_type *filmtree , lru_type *LRU){
            totalnum = 0;
            threshold = Threshold;
            evicttable.reserve(numkey);
            index = filmtree;
            lru = LRU;

        }


        void insert(const std::vector<key_type> keys,key_type* payload,const int error,const int error_recursize ){
            index->build(keys,payload,error,error_recursize,lru);
        }

        void append(const std::vector<key_type> keys,key_type* payload,const int error,const int error_recursize ){
            index->update_append(keys,payload,error,error_recursize,lru);
        }

//        void update(const std::vector<key_type> keys,const std::vector<key_type> payload){
//            index->update_random(keys,payload);
//        }

        void update(const std::vector<key_type> keys,key_type* payload){
            index->update_random(keys,payload,lru);    // out-of-order insertion
        }


        struct memoryusage
        {
            /// the total used memory
            double  totalusemem;
            /// the usage of each component
            infomap meminfo;
            /// Constructor
            inline memoryusage(double total,infomap eachmem)
                    : totalusemem(total), meminfo(eachmem)
            { }
            inline memoryusage()
                    : totalusemem(), meminfo()
            { }

        };

        struct inmempage  // a buffer page in memory
        {
            pageid_type pageid;
            //vector<key_type> inmemdata;
            key_type* inmemdata = NULL;
            int pagesize;
            int freespace;
            int recordnum;
            inmempage(pageid_type idinpage,int sizepage,int numrecord){
                pageid = idinpage;
                pagesize = sizepage;
                inmemdata = new key_type[pagesize];
                //freespace = pagesize;
                freespace = 0;
                recordnum = numrecord;
            }
            inmempage(){
                //freespace = pagesize;
            }
            ~inmempage(){
                delete [] inmemdata;
            }


        };

        // define the current page in memory, when the page is full, write to disk
        inmempage *inpage = NULL;
        vector<inmempage*> inmempages ;

        inline void createinmempage(int sizepage,int numrecord){

            inpage = new inmempage(inpageid,sizepage,numrecord);  //create a page in memory;
            inpageid += 1;

        }

        // compute the usage of memory, that the total used memory by index,lru,data.addusage;

        memoryusage simu_computeMemUsage(int transnum){

            infomap indexdatausage = index->show_verify();
            double dataV = ( index->valuesize+1)*8;
            double datausage = double((index->inkeynum-transnum)*(dataV))/1024/1024;
            double addusage = double((index->exkeynum+index->inkeynum)*(1+8)+(index->exkeynum+transnum)*(8+4)+(index->exkeynum+transnum)*sizeof(key_type) )/1024/1024;
            double indexusage = indexdatausage["indexusage"];
            double hashlrusize = sizeof(key_type) + 8+ 16 + 16;
            double no_hashsize = 16+4;  //prev,next, slot-short int
            double lruusage = double(no_hashsize*(index->inkeynum-transnum) + hashlrusize * index->vstats.leaves)/1024/1024;
            double totalmem = lruusage + datausage + indexusage + addusage;
            double leaves = indexdatausage["leaves"];
            double levels = indexdatausage["levels"];
            double innernodes = indexdatausage["inners"];
            double exkeynum = index->exkeynum;
            double inkeynum = index->inkeynum;

            infomap devied_mem;
            devied_mem.insert(pair<std::string,double>("datausage",datausage));
            devied_mem.insert(pair<std::string,double>("indexusage",indexusage));
            devied_mem.insert(pair<std::string,double>("lruusage",lruusage));
            devied_mem.insert(pair<std::string,double>("addusage",addusage));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("leaves",leaves));
            devied_mem.insert(pair<std::string,double>("levels",levels));
            devied_mem.insert(pair<std::string,double>("innernodes",innernodes));
            devied_mem.insert(pair<std::string,double>("totalusage",totalmem));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("exkeynum",exkeynum));
            devied_mem.insert(pair<std::string,double>("inkeynum",inkeynum));
            memoryusage res_memusage(totalmem,devied_mem);
            return res_memusage;

        }

        memoryusage runtimecomputeMemUsage(){

            infomap indexdatausage = index->runtimeshow_verify();
            double lruusage = indexdatausage["lruusage"] ;
            double indexusage = indexdatausage["indexusage"];
            double datausage = indexdatausage["datausage"];
            double addusage = indexdatausage["addusage"];
            double totalmem = lruusage + datausage + indexusage + addusage;
            double leaves = indexdatausage["leaves"];
            double levels = indexdatausage["levels"];
            double innernodes = indexdatausage["inners"];
            double exkeynum = index->exkeynum;
            double inkeynum = index->inkeynum;

            infomap devied_mem;
            devied_mem.insert(pair<std::string,double>("datausage",datausage));
            devied_mem.insert(pair<std::string,double>("indexusage",indexusage));
            devied_mem.insert(pair<std::string,double>("lruusage",lruusage));
            devied_mem.insert(pair<std::string,double>("addusage",addusage));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("leaves",leaves));
            devied_mem.insert(pair<std::string,double>("levels",levels));
            devied_mem.insert(pair<std::string,double>("innernodes",innernodes));
            devied_mem.insert(pair<std::string,double>("totalusage",totalmem));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("exkeynum",exkeynum));
            devied_mem.insert(pair<std::string,double>("inkeynum",inkeynum));
            memoryusage res_memusage(totalmem,devied_mem);
            return res_memusage;

        }


        memoryusage computeMemUsage(){

            infomap indexdatausage = index->show_verify();
            double lruusage = indexdatausage["lruusage"] ;
            double indexusage = indexdatausage["indexusage"];
            double datausage = indexdatausage["datausage"];
            double addusage = indexdatausage["addusage"];
            double totalmem = lruusage + datausage + indexusage + addusage;
            double leaves = indexdatausage["leaves"];
            double levels = indexdatausage["levels"];
            double innernodes = indexdatausage["inners"];
            double exkeynum = index->exkeynum;
            double inkeynum = index->inkeynum;

            infomap devied_mem;
            devied_mem.insert(pair<std::string,double>("datausage",datausage));
            devied_mem.insert(pair<std::string,double>("indexusage",indexusage));
            devied_mem.insert(pair<std::string,double>("lruusage",lruusage));
            devied_mem.insert(pair<std::string,double>("addusage",addusage));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("leaves",leaves));
            devied_mem.insert(pair<std::string,double>("levels",levels));
            devied_mem.insert(pair<std::string,double>("innernodes",innernodes));
            devied_mem.insert(pair<std::string,double>("totalusage",totalmem));  //key,flag,pageID,offset
            devied_mem.insert(pair<std::string,double>("exkeynum",exkeynum));
            devied_mem.insert(pair<std::string,double>("inkeynum",inkeynum));
            memoryusage res_memusage(totalmem,devied_mem);
            return res_memusage;

        }

        pair<pageid_type ,pageOff_type>* writeevicttable(pair<pageid_type ,pageOff_type> pospage,pair<pageid_type ,pageOff_type>* evictpos){
            pair<pageid_type ,pageOff_type>* oldpospage = evictpos;
            evictpos->first = pospage.first;
            evictpos->second = pospage.second;
            return oldpospage;

//            evictpos->first = pospage.first;
//            evictpos->second = pospage.second;
        }

        pair<pageid_type ,pageOff_type>*  writeevicttable(pair<pageid_type ,pageOff_type> pospage){
            unsigned long int evictpos = evicttable.size();
//            evictkey.push_back(key);
            evicttable.push_back(pospage);
            auto eptr = &evicttable[evictpos];
            return eptr;
        }


        unsigned short int runtimeevictpagestodisk(disk_type *diskpage){
            unsigned short int pnum = inmempages.size();
            unsigned short int num = inmempages.size();  // 此次要写入磁盘的页的数目
            int fd = open(diskpage->filename.c_str(), O_RDWR | O_DIRECT, 0755);
            unsigned long int fixed_buf_size = diskpage->pagesize * sizeof(key_type);  // 磁盘页固定的大小
            unsigned long seekdis =  fixed_buf_size * diskpage->nextpageid;
            lseek(fd, seekdis, SEEK_SET);
            int i = 0;
            while (pnum > diskpage->blocknum) {
                pnum -= diskpage->blocknum;    //diskpage->blocknum 表示 写出block 包含多少个page
                key_type *buf = new key_type[diskpage->pagesize* diskpage->blocknum];

                unsigned long buf_size = fixed_buf_size * diskpage->blocknum;
                int ret = posix_memalign((void **) &buf, 512, buf_size);

                int offset = 0;
                for(int k = 0;k<diskpage->blocknum;k++){
                    memcpy(buf + offset * diskpage->pagesize, &inmempages[i*diskpage->blocknum+k]->inmemdata[0], fixed_buf_size);
                    ++offset;
                }
                i += 1;
                ret = write(fd, buf, buf_size);
//                free(buf);
                delete [] buf;
            }

            key_type *buf = new key_type[diskpage->pagesize*pnum];
            unsigned long int buf_size = diskpage->pagesize * sizeof(key_type) * pnum;
            int ret = posix_memalign((void **) &buf, 512, buf_size);
            unsigned int offset = 0;
            for(int k = 0;k<pnum;k++){
                memcpy(buf + offset * diskpage->pagesize, &inmempages[i*diskpage->blocknum+k]->inmemdata[0], fixed_buf_size);
                ++offset;
            }
            ret = write(fd, buf, buf_size);
//            free(buf);
            delete[] buf;
            diskpage->nextpageid += num;
            close(fd);

            for (int mi = 0;mi < inmempages.size();mi++){
                delete inmempages[mi];
                inmempages[mi] = NULL;
            }
            inmempages.resize(0);
            return num;
        }

        pair<pageid_type ,pageOff_type>* evictkeytoinpage(key_type ekey,data_type edata, disk_type *diskpage){

            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就向该页写出，如果不能，那么就将当前的inpage写出disk，创建新的inpage
            {
                inmempages.emplace_back(inpage);
//                diskpage->odirectenterpage(inpage->inmemdata);// write to disk
                createinmempage(diskpage->pagesize,diskpage->recordnum);
                inpage->recordnum -=1;

            }

            auto evictpos = writeevicttable(pair<pageid_type ,pageOff_type>(inpage->pageid, inpage->freespace));
            inpage->inmemdata[inpage->freespace++] = ekey;
            for (int vi = 0;vi < index->valuesize;vi++){
                inpage->inmemdata[inpage->freespace++] = edata[vi];
            }

            return evictpos;

        }


        void evictkeytoinpage(key_type ekey,data_type edata, disk_type *diskpage,pair<pageid_type ,pageOff_type>* evictpos){

            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就向该页写出，如果不能，那么就将当前的inpage写出disk，创建新的inpage
            {
                inmempages.emplace_back(inpage);
//                diskpage->odirectenterpage(inpage->inmemdata);// write to disk
                createinmempage(diskpage->pagesize,diskpage->recordnum);
                inpage->recordnum -=1;

            }
            writeevicttable(pair<pageid_type ,pageOff_type>(inpage->pageid, inpage->freespace),evictpos);
            inpage->inmemdata[inpage->freespace++] = ekey;
            for (int vi = 0;vi < index->valuesize;vi++){
                inpage->inmemdata[inpage->freespace++] = edata[vi];
            }

        }


/*
        pair<int,short int>* evictkeytoinpage(key_type ekey,data_type edata, disk_type *diskpage,pair<int,short int>* evictpos){
            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
            {
                diskpage->odirectenterpage(inpage->inmemdata);// write to disk
                createinmempage(diskpage->pagesize,diskpage->recordnum);
                inpage->recordnum -=1;
            }
            short int offset = inpage->inmemdata.size();
            inpage->inmemdata.push_back(ekey);
            inpage->freespace -= 1;
            inpage->inmemdata.insert( inpage->inmemdata.begin()+(inpage->pagesize - inpage->freespace),edata.begin(),edata.end());
            inpage->freespace -= (index->valuesize);
//            short int offset2 = (inpage->pagesize - index->valuesize-1 - inpage->freespace);
            pair<int,short int>* olppagepos = writeevicttable(pair<int,short int>(inpage->pageid, offset),evictpos);
            return olppagepos;
        }
        */
/*
        pair<int,short int> evictkeytoinpage(key_type ekey,data_type edata, disk_type *diskpage,int evictpos){
            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
            {
                diskpage->odirectenterpage(inpage->inmemdata);// write to disk
                createinmempage(diskpage->pagesize,index->recordsize+1);
                inpage->recordnum -=1;
            }
            inpage->inmemdata.push_back(ekey);
            inpage->freespace -= 1;
            inpage->inmemdata.insert( inpage->inmemdata.begin()+(inpage->pagesize - inpage->freespace),edata.second.begin(),edata.second.end());
            inpage->freespace -= index->recordsize;

            pair<int,short int> olppagepos = writeevicttable(pair<int,short int>(inpage->pageid, (inpage->pagesize - index->recordsize -1 - inpage->freespace)),evictpos);
            return olppagepos;
        }
*/

//        pair<pageid_type ,pageOff_type>* evictkeytoinpage(key_type ekey,data_type edata, disk_type *diskpage){
//
//            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
//            {
//                diskpage->odirectenterpage(inpage->inmemdata);// write to disk
//                createinmempage(diskpage->pagesize,index->recordsize);
//                inpage->recordnum -=1;
//            }
//            inpage->inmemdata.push_back(ekey);
//            inpage->freespace -= 1;
//            inpage->inmemdata.insert( inpage->inmemdata.begin()+(inpage->pagesize - inpage->freespace),edata.begin(),edata.end());
//            inpage->freespace -= (index->recordsize-1);
//
//            auto posevict = writeevicttable(pair<pageid_type ,pageOff_type>(inpage->pageid, (inpage->pagesize - index->recordsize - inpage->freespace)));
//            return posevict;
//        }



        unsigned long filmtransfer(unsigned int transleaves,disk_type *diskpage){   //perform the transfer procedure,
            //
            timeval initw1,initw2;
            unsigned long initwtime = 0;
            if (inpage == NULL){
                createinmempage(diskpage->pagesize,diskpage->recordnum);    //create a page in memory;
            }
            if (index->m_transleaf == NULL){
                index->m_transleaf = index->leaflevel.leafpieces[0];
            }
            /*
            if  ( this->index->Error > 256 && transleaves == 1 )
            {// 如果是这种情况，需要在一个leaf 中 批量写出，因为一次性写出leaf 所有的数据 会使得 内存的usage not full
                // 判断该页全部数据写出去 的usage
                auto midtransflag = simu_computeMemUsage(index->m_transleaf->slotkey.size());
                if (midtransflag.totalusemem > threshold ){
                    for (int k = 0; k < index->m_transleaf->slotkey.size();k++)
                    {
                        if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
                        {
                            gettimeofday(&initw1, NULL);
                            diskpage->odirectenterpage(inpage->inmemdata);// write to disk , 这里是初始化阶段，所以没有采用 odirect 的方式 ，如果采用，只需要将在enterpage 前加上odirect， 去掉则不是直接操作磁盘
                            delete inpage;
                            inpage = NULL;
                            createinmempage(diskpage->pagesize,diskpage->recordnum);
                            gettimeofday(&initw2, NULL);
                            initwtime += (initw2.tv_sec - initw1.tv_sec) + (double) (initw2.tv_usec - initw1.tv_usec) / 1000000.0;
                            inpage->recordnum -=1;
                        }

                        auto eptr = writeevicttable(pair<unsigned long int,unsigned int>(inpage->pageid, inpage->freespace));
                        inpage->inmemdata[inpage->freespace++] = index->m_transleaf->slotkey[k];
                        auto transdata = (adalru::Node<unsigned int, key_type* >*) index->m_transleaf->slotdata[k];
                        for (int vi = 0; vi < index->valuesize; vi++){
                            inpage->inmemdata[inpage->freespace++] = transdata->value[vi];
                        }

                        index->m_transleaf->intrachain.remove_node(transdata);//pop from intrachain
                        transdata = NULL;
                        index->m_transleaf->slotflag[k] = false;
                        index->m_transleaf->slotdata[k]  = eptr ;  //
                    }
                    lru->remove(index->m_transleaf->startkey);// pop from interchain
                    index->inkeynum -= index->m_transleaf->slotkey.size();
                    index->exkeynum += index->m_transleaf->slotkey.size();
                    index->m_transleaf = index->leaflevel.leafpieces[index->leafsplit+1];//evict data from the first leaf
                    index->leafsplit += transleaves;
                }
                else{  // 一点一点的写出
                    auto midtrans = computeMemUsage();
                    int split = 0;
                    while (midtrans.totalusemem > threshold){
                        double ratio = (midtrans.totalusemem-threshold)/(midtrans.totalusemem-midtransflag.totalusemem ) ;
                        int trannum = ceil(index->m_transleaf->slotkey.size() * ratio+100);
                        for (int k = 0; k < trannum;k++)
                        {
                            if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
                            {
                                gettimeofday(&initw1, NULL);
                                diskpage->odirectenterpage(inpage->inmemdata);// write to disk , 这里是初始化阶段，所以没有采用 odirect 的方式 ，如果采用，只需要将在enterpage 前加上odirect， 去掉则不是直接操作磁盘
                                delete inpage;
                                inpage = NULL;
                                createinmempage(diskpage->pagesize,diskpage->recordnum);
                                gettimeofday(&initw2, NULL);
                                initwtime += (initw2.tv_sec - initw1.tv_sec) + (double) (initw2.tv_usec - initw1.tv_usec) / 1000000.0;
                                inpage->recordnum -=1;
                            }

                            auto transdata = (adalru::Node<unsigned int, key_type* >*) index->m_transleaf->slotdata[split+k].second;
                            auto eptr = writeevicttable(pair<unsigned long int,unsigned int>(inpage->pageid, inpage->freespace));
                            inpage->inmemdata[inpage->freespace++] = index->m_transleaf->slotkey[split+k];

                            for (int vi = 0; vi < index->valuesize; vi++){
                                inpage->inmemdata[inpage->freespace++] = transdata->value[vi];
                            }
                            index->m_transleaf->intrachain.remove_node(transdata);//pop from intrachain
                            transdata = NULL;
                            index->m_transleaf->slotdata[split+k].first = false;
                            index->m_transleaf->slotdata[split+k].second  = eptr ;  //
                        }
                        split += trannum;
                        index->inkeynum -= trannum;
                        index->exkeynum += trannum;
                        midtrans = computeMemUsage();
                    }
                }
            }
            else{
                */
            for (int i = 0; i< transleaves; i++){
                for (unsigned int k = 0; k < index->m_transleaf->slotkey.size();k++)
                {
                    if (!(inpage->recordnum --))  //如果还能容纳一个record，那么就执行transfer，如果不能，那么就将当前的inpage写出disk，创建新的inpage
                    {

                        gettimeofday(&initw1, NULL);
                        diskpage->odirectenterpage(inpage->inmemdata);// write to disk , 这里是初始化阶段，所以没有采用 odirect 的方式 ，如果采用，只需要将在enterpage 前加上odirect， 去掉则不是直接操作磁盘
                        gettimeofday(&initw2, NULL);
                        initwtime += (initw2.tv_sec - initw1.tv_sec) * 1000000 + (initw2.tv_usec - initw1.tv_usec);
                        delete inpage;
                        inpage = NULL;
                        createinmempage(diskpage->pagesize,diskpage->recordnum);
                        inpage->recordnum -=1;
                    }

                    auto eptr = writeevicttable(pair<pageid_type ,pageOff_type>(inpage->pageid, inpage->freespace));
                    inpage->inmemdata[inpage->freespace++] = index->m_transleaf->slotkey[k];
                    auto transdata = (adalru::Node<lruOff_type , key_type* >*) index->m_transleaf->slotdata[k];
                    for (int vi = 0; vi < index->valuesize; vi++){
                        inpage->inmemdata[inpage->freespace++] = transdata->value[vi];
                    }

                    index->m_transleaf->intrachain.remove_node(transdata);//pop from intrachain
//                        delete transdata;
                    transdata = NULL;
                    index->m_transleaf->slotflag[k] = false;
                    index->m_transleaf->slotdata[k]  = eptr ;  //
                }
                lru->remove(index->m_transleaf->startkey);// pop from interchain
                index->inkeynum -= index->m_transleaf->slotkey.size();
                index->exkeynum += index->m_transleaf->slotkey.size();
                index->m_transleaf = index->leaflevel.leafpieces[index->leafsplit+i+1];//evict data from the first leaf
            }
            index->leafsplit += transleaves;

            diskpage->initwtime += initwtime;
            return initwtime;
        }

        unsigned long filmtransfer(double totalusemem,disk_type *diskpage){   //perform the transfer procedure,
            //
            timeval initw1,initw2;
            unsigned long initwtime = 0;

            // 大致计算出此次要evict 的数量
            double ratio = (0.3) * (totalusemem - threshold) / threshold;
            unsigned int estimate_evict = ceil(index->inkeynum * ratio) + 300;
            unsigned int batch_evict = (1280 * diskpage->pagesize / (index->valuesize + 1));
            if (estimate_evict < batch_evict)
                batch_evict = estimate_evict;
            gettimeofday(&initw1, NULL);
            for (unsigned int i = 0; i < batch_evict; i++)   // the number of records to be evicted in batches
            {
                // evict data, get the tail node, remove the tail of intrachain
                auto evictleaf = lru->get_tail();
                auto evictslotV = evictleaf->intrachain.poptail();   // the tail of the accessed leaf
                auto writeevict = evictkeytoinpage(evictleaf->slotkey[evictslotV->key], evictslotV->value,
                                                            diskpage);
                evictleaf->slotflag[evictslotV->key] = false;
                evictleaf->slotdata[evictslotV->key] = writeevict;
                delete evictslotV;
                evictslotV = NULL;

            }

            runtimeevictpagestodisk(diskpage);
            gettimeofday(&initw2, NULL);
            initwtime += (initw2.tv_sec - initw1.tv_sec)* 1000000 + (initw2.tv_usec - initw1.tv_usec);

            index->inkeynum -= batch_evict;
            index->exkeynum += batch_evict;
            diskpage->initwtime += initwtime;
            return initwtime;
        }



        template<typename stat_type>
        pair<bool,double > runtimejudgetrans(stat_type query_stats){
            struct timeval ct1, ct2;
            double ctimeuse;
            gettimeofday(&ct1, NULL);
            memoryusage res_memusage = runtimecomputeMemUsage();
            gettimeofday(&ct2, NULL);
            ctimeuse = (ct2.tv_sec - ct1.tv_sec) + (double) (ct2.tv_usec - ct1.tv_usec) / 1000000.0;
            query_stats->computetimeuse += ctimeuse;
            if (res_memusage.totalusemem > threshold) //perform transfer process
                // 判断如果全部数据写出去都不能满足larger-than-memory data set
                return pair<bool,double > (true,res_memusage.totalusemem);
            else
                return  pair<bool,double > (false,res_memusage.totalusemem);
        }

        template<typename stat_type>
        pair<bool,memoryusage> judgetransfer(stat_type range_stats ){
            memoryusage res_memusage;
            struct timeval ct1, ct2;
            double ctimeuse;
            gettimeofday(&ct1, NULL);
            res_memusage = computeMemUsage();
            gettimeofday(&ct2, NULL);
            ctimeuse = (ct2.tv_sec - ct1.tv_sec) + (double) (ct2.tv_usec - ct1.tv_usec) / 1000000.0;
            range_stats->computetimeuse += ctimeuse;
            map<string,double>::iterator iter;

            if (res_memusage.totalusemem > threshold){//perform transfer process
            for(iter = res_memusage.meminfo.begin(); iter != res_memusage.meminfo.end(); iter++)
            //     cout<<iter->first<<" "<<iter->second<<" *** ";
            // cout<<endl;
                return pair<bool,memoryusage>(true,res_memusage);
            }
            return  pair<bool,memoryusage>(false,res_memusage);
        }


        pair<bool,memoryusage> judgetransfer( ){
            memoryusage res_memusage;
            res_memusage = computeMemUsage();

            map<string,double>::iterator iter;

            if (res_memusage.totalusemem > threshold){//perform transfer process
            for(iter = res_memusage.meminfo.begin(); iter != res_memusage.meminfo.end(); iter++)
            //     cout<<iter->first<<" "<<iter->second<<" *** ";
            // cout<<endl;
                return pair<bool,memoryusage>(true,res_memusage);
            }
            return  pair<bool,memoryusage>(false,res_memusage);
        }
    };

    typedef std::map<std::string,double> infomap;
    template <class key_type>
    class filmdisk{
    public:
        // const char *pagefile;
        std::string filename;
        int pagesize;
        pageid_type nextpageid;
        int recordnum;  // 一个页最多容纳的 record  的数量
        int recordsize;
        int blocknum;
        unsigned long initwtime;
        filmdisk()=default;

        filmdisk(std::string fn, int sizepage,int numrecord,int sizerecord) {
            filename=fn;
            // pagefile = diskfile;
            pagesize = sizepage;
            recordnum = numrecord;
            blocknum = 1024*1024/(sizepage*8);
            nextpageid = 0;
            initwtime = 0.0;
            recordsize = sizerecord;
        }

        filmdisk& operator=(const filmdisk& other) {
            filename = other.filename;
            // pagefile = other.pagefile;
            pagesize = other.pagesize;
            recordnum = other.recordnum;
            blocknum = other.blocknum;
            nextpageid = other.nextpageid;
            initwtime = other.initwtime;
            recordsize = other.recordsize;
            return *this;
        }

        vector<key_type> readfromdisk(pair<pageid_type ,pageOff_type> diskpos ,int sizerecord){   // use the buffer of memory
            FILE *fdisk;// 读取磁盘文件
            fdisk = fopen(filename.c_str(),"rb+");
            fseek(fdisk,static_cast<unsigned long>(diskpos.first*pagesize*8),SEEK_SET);
            vector<key_type> pagedata;
            key_type tmp;

            for (int i = 0; i < pagesize; i ++){
                fread(&tmp, sizeof(long int), 1, fdisk); // 从文件中读数
                pagedata.push_back(tmp);
//                cout<< tmp << " ";
            }

            vector<key_type> res;

            for (int i = 0; i < sizerecord; i ++){
                res.push_back(pagedata[diskpos.second+i]);
            }

//            cout<<"Jesus, You are my refuge! "<<endl;
            fclose(fdisk);
            return res;

        }

        // use the system o_direct mode to read file
        vector<key_type> odirectreadfromdisk(pair<pageid_type ,pageOff_type> diskpos ,int sizerecord){
            int fd;
            key_type *buf;
            vector<key_type> res;
            unsigned long int buf_size = pagesize*8;
            int ret = posix_memalign((void **)&buf, 512, buf_size);
            memset(buf, 'c', buf_size);

            fd = open(filename.c_str(), O_RDWR | O_DIRECT , 0755);
            /*
            if (fd < 0){
                perror("open ./direct_io.data failed");
                exit(1);
            }
             */
            unsigned long seekdis = buf_size * diskpos.first;
            ret = pread(fd, buf, buf_size,seekdis);
            if (ret <= 0){
                cout << "Jesus, i need You!" << endl;
            }
            for (int i = 0; i < sizerecord; i ++){
                res.push_back(buf[diskpos.second+i]);
            }
//            cout<<"Jesus, You are my refuge! "<<endl;
//            free(buf);
            delete[] buf;
            close(fd);
            return res;
        }

        // use the system o_direct mode to read file
        pair<key_type,key_type*> odirectreadfromdisk(pair<pageid_type ,pageOff_type>* diskpos){
            int fd;
            key_type *buf;
            pair<key_type,key_type*> res;
            uint64_t buf_size = pagesize*8;
            int ret = posix_memalign((void **)&buf, 4096, buf_size);
            // int ret = posix_memalign((void **)&buf, 512, buf_size);
            memset(buf, 'c', buf_size);

            fd = open(filename.c_str(), O_RDWR | O_DIRECT , 0755);
            uint64_t aoff = diskpos->first*buf_size;
            ret = pread(fd, buf, buf_size,aoff);
            if (ret <= 0){
                cout << "Jesus, i need You!" << endl;
            }

            res.first = buf[diskpos->second];
            res.second = new key_type[recordsize-1];
            for (int i = 0; i < recordsize-1; i ++){
                res.second[i] = buf[diskpos->second+1+i];
            }
//            cout<<"Jesus, You are my refuge! "<<endl;

            delete[] buf;
            close(fd);
            return res;
        }

        int enterpage(vector<key_type> datas)   //将一个内存页，写入磁盘, doesn't consider the odirect
        {

            FILE *fdisk;// 读取磁盘文件
            fdisk = fopen(filename.c_str(),"rb+");
            fseek(fdisk,static_cast<unsigned long>(nextpageid * (pagesize)*8),SEEK_SET);
            nextpageid += 1;
//            cout<<datas.size()<<endl;
            for(int i = 0 ; i < datas.size() ; i++) {
                fwrite(&datas[i], sizeof(key_type), 1, fdisk);  //就是把id里面的值读到f里面，每次读8个字节，一共读size次
//                cout << datas[i] <<" ";
            }

            fflush(fdisk);
            fclose(fdisk);
//            cout<< "*********************** Jesus, finished one transfer*************************"<<endl;

//            cout<< datas.size()<<endl;
//            cout<< sizeof(datas)<<endl;
//            vector<long int>  reads(datas.size());
////            cout<< reads.size()<<endl;
////            cout<< sizeof(reads)<<endl;
//            FILE *fdisk2;
//            fdisk2 = fopen(pagefile,"rb");
//            fread(&reads ,  sizeof(reads) , 1 ,fdisk2); // 从文件中读数据
//            fclose(fdisk2);

        }


        void odirectenterpage(key_type* datas)   //将一个内存页，写入磁盘,consider the odirect

        {

            int fd;
            key_type *buf = new key_type[pagesize];

            unsigned long int buf_size = pagesize*sizeof(key_type);
            int ret = posix_memalign((void **)&buf, 512, buf_size);
//            memcpy(buf, &datas[0], datas.size()*sizeof(key_type));
//            int ret = posix_memalign((void **)&buf, 512, buf_size);
//            memset(buf, 'c', 4096);
            memcpy(buf, &datas[0],buf_size);

            fd = open(filename.c_str(), O_RDWR | O_DIRECT , 0755);
            /*
            if (fd < 0){
                perror("open ./direct_io.data failed");
                exit(1);
            }
             */
            unsigned long seekdis = buf_size * nextpageid;
            ret = pwrite(fd, buf, buf_size,seekdis);
            if (ret <= 0){
                cout << "Jesus, i need You!" << endl;
            }
            nextpageid += 1;

//            cout<<"Jesus, You are my refuge! odirect write "<<endl;

//            free(buf);
            delete[] buf;
            close(fd);

//            cout<< "*********************** Jesus, finished one transfer*************************"<<endl;
        }

    };
}




#endif //EXPERIMENTCC12_FILMADASTORAGE_H
