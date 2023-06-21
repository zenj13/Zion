#ifndef BABYLON_CONSTANTS_H_INCLUDED
#define BABYLON_CONSTANTS_H_INCLUDED



#endif // BABYLON_CONSTANTS_H_INCLUDED
 //CONSTATNTS

 #include<deque>
 #include<vector>
 #include<future>
 #include<string.h>

 #include "zion_utils.h"

 mutex global_sock_lock;
 int8_t def_batch_nos[5]={1,2,3,4,5};

 const uint8_t CTRL_CMD = 69;
 const uint8_t DATA_CMD = 96;



 uint8_t CR_INIT_CMD_HEAD[4] = {'x','x','x','x'};
 uint8_t CT_INIT_CMD_HEAD[4] = {'x','X','X','x'};
 uint8_t INIT_CMD_TAIL[4] = {'x',22,22,'x'};
 uint8_t CR_INIT_CMD_FIN[4] = {'x',44,44,'x'};
 uint8_t CR_INIT_CMD_FIN_OK[4] = {'X',44,44,'x'};
 uint8_t CT_INIT_CMD_FIN[4] = {'X',44,44,'X'};

 uint8_t TRESET_HEAD[4] = {11,'x','x',10};
 uint8_t TRESET_TAIL[4] = {99,'x','x',90};

 uint8_t BACK_HEAD[4] = {19,'x','x',98};
 uint8_t BACK_TAIL[4] = {19,'x','x',99};

 uint8_t RRESET_HEAD[4] = {11,'x','x',11};
 uint8_t RRESET_TAIL[4] = {99,'x','x',99};

 const int MAX_RCOUNT = 50;

 const u_int DEFAULT_MTU = 1500;
 const u_int DEFAULT_MSS = DEFAULT_MTU - 40;


 struct export_pod
 {
     sockaddr export_addr;
     int export_addr_len;

     array<uint8_t,DEFAULT_MTU> exports;
     int exports_size;
 };

 struct client_pod
 {
    int zion_socket;
    int client_ipaddr;
    double tx_timeout;
    double RX_TIME= 0;


    mutex mps_lock;
    bool missin_pdus_sent=false;

    int MAX_TCOUNT = 5;

    mutex rcount_lock;
    int RCOUNT=1;

    mutex tcount_lock;
    int TCOUNT=1;

    map<uint8_t,array<uint8_t,DEFAULT_MSS>> pdu_cache;
    map<uint8_t,array<uint8_t,DEFAULT_MSS>> xpdu_cache;

    uint8_t *babylon_inports;

    uint8_t batch_no=1;
    uint8_t batch_index=0;

    uint8_t rbatch_no;
    uint8_t rbatch_index;

    sockaddr export_addr;
    int export_addr_len;
    mutex export_addr_lock;

    sockaddr inport_addr;
    int inport_addr_len;
    mutex inport_addr_lock;

    deque<array<uint8_t,DEFAULT_MTU>> exports;
    mutex exports_lock;

    deque<array<uint8_t,DEFAULT_MTU>> inports;
    mutex inports_lock;

    future<double> timer_thread;
    bool kill_timer;
    mutex kill_timer_lock;

    future<double> rreset_timer;
    bool kill_rresettimer;
    mutex kill_rresettimer_lock;

    future<double> ct_timer_thread;
    bool ct_kill_timer;
    double ct_timeval;
    mutex ct_kill_timer_lock;

    future<double> cr_timer_thread;
    bool cr_kill_timer;
    double cr_timeval;
    mutex cr_kill_timer_lock;

    bool ct_init_ok;
    bool cr_init_ok;


    deque<uint8_t> missed_batch_nos;
    vector<uint8_t> rbatch_nos;

    vector<uint8_t> babylon_exports;

    uint8_t back_head_data[DEFAULT_MTU];
    int back_head_data_len = 0;

    //packs a packet into a babylon pdu

    void initZion(int sock_fd)
    {
      zion_socket = sock_fd;
    }


    int packCMDLoad(uint8_t *export_load, int export_raw_weight,vector<uint8_t> &cmdpdu)
    {
        cmdpdu.clear();
        cmdpdu.push_back(CTRL_CMD);



        cmdpdu.insert(cmdpdu.end(),export_load,export_load+export_raw_weight);
        int temp_bexp_len=cmdpdu.size();

        return temp_bexp_len;
        //copy(begin(export_load),end(export_load),);
    }

    void packExportLoad(uint8_t *export_load, int export_raw_weight)
    {
        cout<<"\n\nexport raw weight: "<<export_raw_weight<<endl<<endl;
        cout.flush();
        babylon_exports.clear();
        babylon_exports.push_back(DATA_CMD);
        babylon_exports.push_back(batch_index);
        babylon_exports.push_back(batch_no);

        int temp_bexp_len=babylon_exports.size();

        babylon_exports.insert(babylon_exports.end(),export_load,export_load+export_raw_weight);
        //copy(begin(export_load),end(export_load),);
    }



    bool sendMissingPDUs()//uint8_t *back_pdu,int payload_len)
    {
        xpdu_cache = pdu_cache;
       // pdu_cache.clear();
        cout<<endl<<"started pdu back handler: "<<endl;
        cout.flush();
        if(back_head_data_len>0)
        {
            pdu_cache.clear();
            cout<<endl<<"\t\tgetting back data"<<endl;
            cout.flush();

        //    cin.get();
        for(int pdu_indx=0;pdu_indx<back_head_data_len;pdu_indx++)
        {
            uint8_t pind = back_head_data[pdu_indx];
            array<uint8_t,DEFAULT_MSS> &dtum = xpdu_cache[pind];

            cout<<endl<<"\tsending pdu: "<<0+pind<<" datm: "<<(char*)dtum.data()<<endl;
            cout.flush();
            sendExports(dtum.data(),dtum.size());
        }
        memset(back_head_data,0,sizeof(back_head_data));
        back_head_data_len = 0;
        xpdu_cache.clear();
        }
        else
        {
            cout<<endl<<"no back data"<<endl;
            cout.flush();

        //    cin.get();
        }

        return true;
    }



    int sendExports(uint8_t *export_load, int export_raw_weight)
    {
        sockaddr inports_city;
        int inports_city_len = sizeof(inports_city);
        int exports_weight = 0;
        cout<<"\npreparing data of len: "<<export_raw_weight<<endl;
        cout.flush();
        tcount_lock.lock();
        int xTCOUNT = TCOUNT;
        tcount_lock.unlock();
        if(xTCOUNT<=MAX_TCOUNT)
        {
            /*ghosted pdu
            if(batch_no==3)
            {
                
                pdu_cache.insert({{batch_no,array<uint8_t,DEFAULT_MSS>()}});
                array<uint8_t,DEFAULT_MSS> &sent_pdu_ref = pdu_cache.at(batch_no);
                copy(export_load,export_load+export_raw_weight,sent_pdu_ref.begin());

                cout<<endl<<"ghosted pdu: "<<(0+batch_no)<<" with payload: "<<export_raw_weight<<endl;
                cout.flush();
                TCOUNT= TCOUNT + 1;
                batch_no = batch_no + 1;
            }
            else
            {
                */
            packExportLoad(export_load,export_raw_weight);
            cout<<"\npacking data of len: "<<export_raw_weight<<endl;
        cout.flush();
            int bout_len = babylon_exports.size();
            global_sock_lock.lock();
            exports_weight=sendto(zion_socket, (char*)babylon_exports.data(),bout_len, 0,&export_addr,export_addr_len);
            global_sock_lock.unlock();
            cout<<"\nsending data of len: "<<bout_len<<endl;
        cout.flush();
            if(exports_weight<=0)
            {
                perror("No load to port");
            }
            else
            {

                pdu_cache.insert({{batch_no,array<uint8_t,DEFAULT_MSS>()}});
                cout<<"\npreparing pdu_cache "<<endl<<endl;
        cout.flush();
                array<uint8_t,DEFAULT_MSS> &sent_pdu_ref = pdu_cache[batch_no];//.at(batch_no);
                cout<<"\ngeting batch no of : "<<0+batch_no;
        cout.flush();
                copy(export_load,export_load+exports_weight,sent_pdu_ref.begin());
                cout<<"\nCopying data of len: "<<exports_weight<<endl;
        cout.flush();

                tcount_lock.lock();
                TCOUNT= TCOUNT + 1;
                tcount_lock.unlock();
                batch_no = batch_no + 1;
                cout<<endl<<"payload sent: "<<(char*)babylon_exports.data()<<" with len: "<<babylon_exports.size()<<endl;
                cout.flush();

            }
           // }
        }
        tcount_lock.lock();
        xTCOUNT = TCOUNT;
        tcount_lock.unlock();
        if(xTCOUNT>MAX_TCOUNT)
        {
            bool reseted = resetTCOUNT();
            /*if(reseted==true)
            {
                TCOUNT= TCOUNT + 1;
                batch_no = batch_no + 1;
            }
            */
        }

        return exports_weight;
    }

    void ctrlBACKHEAD()
    {
        cout<<endl<<"received back head"<<endl;
        cout.flush();
        uint8_t inport_load[DEFAULT_MTU];
        kill_rresettimer_lock.lock();
        kill_rresettimer = true;
        kill_rresettimer_lock.unlock();

       // rreset_timer.get();

        cout<<endl<<"looper killeed"<<endl;
        cout.flush();

        kill_rresettimer = false;

        vector<uint8_t> cmd_pdu;
        int cmd_pdu_len = packCMDLoad(BACK_TAIL,sizeof(BACK_TAIL),cmd_pdu);
        sendRawExports(cmd_pdu.data(),cmd_pdu_len);
    }

    void getMissingPDUs()
    {
        missed_batch_nos.clear();
        uint8_t *serie = rbatch_nos.data();
        int ss=rbatch_nos.size();
        int ds=sizeof(def_batch_nos);
        for(int x=0;x<ss;x++)
        {
            uint8_t snum = serie[x];
            for(int xl=0;xl<ds;xl++)
            {
                if(snum==def_batch_nos[xl])
                {
                    def_batch_nos[xl]=0;
                }
            }
        }

        for(int xp=0;xp<ds;xp++)
        {
            uint8_t dm=def_batch_nos[xp];
            if(dm!=0)
            {
                missed_batch_nos.push_back(dm);
                cout<<endl<<"missing: "<<0+dm<<endl;
            }
        }
    }


    bool resetTCOUNT()
    {
        /*TCOUNT = 1;
        batch_no = 1;*/
        //uint8_t inport_load[1460];

           // memset(inport_load,0,sizeof(inport_load));
            //sendRawExports(RRESET_HEAD,sizeof(RRESET_HEAD));

             vector<uint8_t> rreset_head_pdu;

            rreset_head_pdu.push_back(CTRL_CMD);
        // copy(begin(BACK_HEAD),end(BACK_HEAD),back_head_pdu.begin()+1);

            rreset_head_pdu.insert(rreset_head_pdu.end(),begin(RRESET_HEAD),end(RRESET_HEAD));

            int temp_backhead_size=rreset_head_pdu.size();
        // copy(missed_batch_nos.begin(),missed_batch_nos.end(),back_head_pdu.begin()+temp_backhead_size);


            sendRawExports(rreset_head_pdu.data(),rreset_head_pdu.size());
            cout<<endl<<"\t\treseting pdu count\n";
            cout.flush();
            //sendRawExports(RRESET_HEAD,sizeof(RRESET_HEAD));

//            rreset_timer = async(launch::async,doTimer,ref(kill_rresettimer_lock),ref(kill_rresettimer),tx_timeout,zion_socket,export_addr,export_addr_len,rreset_head_pdu,rreset_head_pdu.size());

            /*int inported_len=getRawInportsxX(inport_load,DEFAULT_MSS);//inport_load,sizeof(inport_load));
            if(memcmp(inport_load,BACK_HEAD,sizeof(BACK_HEAD))>0)
            {
                inportsCtrl(inported_len);
                break;
            }
            else
            {
                inportsCtrl(inported_len);
            }
            */
        
       // tcount_lock.lock();
       // TCOUNT = 1;
       // tcount_lock.unlock();
       while(true)
       {
            mps_lock.lock();
            bool mps_fin = missin_pdus_sent;
            mps_lock.unlock();

            if(mps_fin==true)
            {
                mps_lock.lock();
                missin_pdus_sent = false;
                mps_lock.unlock();
                break;
            }
        
       }
      //  batch_no = 1;
        cout<<endl<<"reset ok"<<endl;
        cout.flush();
        return true;

    }

    int sendRawExports(uint8_t *raw_export,int raw_export_weight)
    {
        sockaddr inports_city;
        int inports_city_len= sizeof(inports_city);


        global_sock_lock.lock();
        int exported_weight=sendto(zion_socket, (char*)raw_export,raw_export_weight, 0,&export_addr,export_addr_len);

        if(exported_weight<=0)
        {
            perror("No load to port");
        }


        return exported_weight;
    }


    int sendRawExportsp(uint8_t *raw_export,int raw_export_weight)
    {
        sockaddr inports_city;
        int inports_city_len= sizeof(inports_city);

        vector<uint8_t> export_pdu;

        export_pdu.push_back(CTRL_CMD);


        export_pdu.insert(export_pdu.end(),raw_export,raw_export+raw_export_weight);

        int temp_backhead_size=export_pdu.size();


        int exported_weight=sendto(zion_socket, (char*)export_pdu.data(),temp_backhead_size, 0,&export_addr,export_addr_len);

        if(exported_weight<=0)
        {
            perror("No load to port");
        }


        return exported_weight;
    }

    int sendRawExportsX(uint8_t *raw_export,int raw_export_weight)
    {
        sockaddr inports_city;
        int inports_city_len = sizeof(inports_city);

        cout<<"\nAttempint export to client: "<<inet_ntoa(getIpAddr(inport_addr))<<" with port: "<<ntohs(getPortNo(inport_addr))<<endl;
        cout.flush();
        int exported_weight=sendto(zion_socket, (char*)raw_export,raw_export_weight, 0,&inport_addr,inport_addr_len);

        if(exported_weight<=0)
        {
            perror("No load to port");
        }


        return exported_weight;
    }

    void ctrlTRESETHEAD()//int batch_nox)
    {
        getMissingPDUs();
        int missed_batchnos_len = missed_batch_nos.size();

        cout<<"\nResetting client: "<<inet_ntoa(getIpAddr(inport_addr))<<" with port: "<<ntohs(getPortNo(inport_addr))<<endl;
        cout.flush();

        cout<<endl<<"missing: "<<missed_batchnos_len<<" pdus";
        cout.flush();

        for(int miss_ind=0;miss_ind<missed_batchnos_len;miss_ind++)
        {
         cout<<endl<<"missing pdu: "<<0+missed_batch_nos[miss_ind];
         cout.flush();
        }
        vector<uint8_t> back_head_pdu;


        back_head_pdu.push_back(CTRL_CMD);
        // copy(begin(BACK_HEAD),end(BACK_HEAD),back_head_pdu.begin()+1);

        back_head_pdu.insert(back_head_pdu.end(),begin(BACK_HEAD),end(BACK_HEAD));

        int temp_backhead_size=back_head_pdu.size();
        // copy(missed_batch_nos.begin(),missed_batch_nos.end(),back_head_pdu.begin()+temp_backhead_size);
        back_head_pdu.insert(back_head_pdu.end(),missed_batch_nos.begin(),missed_batch_nos.end());
        cout<<endl<<"sending back head: "<<(char*)back_head_pdu.data();
        cout.flush();
        sendRawExportsX(back_head_pdu.data(),back_head_pdu.size());


    }

    void ctrlRRESETTAIL()
    {
        //resend missing batches
        cout<<endl<<"received rreset tail"<<endl;
        cout.flush();
        batch_no = 1;
        tcount_lock.lock();
        TCOUNT = 1;
        tcount_lock.unlock();
        bool miss_sent = sendMissingPDUs();
        mps_lock.lock();
        missin_pdus_sent = miss_sent;
        mps_lock.unlock();
    }

    void ctrlBACKTAIL()
    {
        vector<uint8_t> treset_tail_pdu;

        treset_tail_pdu.push_back(CTRL_CMD);
        // copy(begin(BACK_HEAD),end(BACK_HEAD),back_head_pdu.begin()+1);

        treset_tail_pdu.insert(treset_tail_pdu.end(),begin(TRESET_TAIL),end(TRESET_TAIL));


        int temp_backhead_size=treset_tail_pdu.size();
        // copy(missed_batch_nos.begin(),missed_batch_nos.end(),back_head_pdu.begin()+temp_backhead_size);
        //rreset_tail_pdu.insert(rreset_tail_pdu.end(),missed_batch_nos.begin(),missed_batch_nos.end());

        sendRawExportsX(treset_tail_pdu.data(),treset_tail_pdu.size());
        RCOUNT=0;

    }


    void inportsCtrl(uint8_t *b_inports,uint32_t inport_weight,sockaddr &a_addr,int &a_addr_len)
    {
        babylon_inports = b_inports;
        uint8_t cmd_byte= babylon_inports[0];
        uint8_t cmd[4];
        int cmd_size=sizeof(cmd);

        switch(cmd_byte)
        {

            case CTRL_CMD:
             memcpy(cmd,&babylon_inports[1],cmd_size);

            ctrlCmd(cmd,cmd_size,inport_weight, a_addr, a_addr_len);
            break;

            case DATA_CMD:

            ctrlData(babylon_inports[1],babylon_inports[2],&babylon_inports[3],(inport_weight-3));
//            logit("data in");
            break;

            default:
              cout<<endl<<"lost cause"<<endl;
              cout.flush();
            break;
        }
    }


    void ctrlRRESETHEAD()//int batch_nox)
    {
        getMissingPDUs();
        int missed_batchnos_len = missed_batch_nos.size();
        vector<uint8_t> back_head_pdu;

        back_head_pdu.push_back(CTRL_CMD);
   // copy(begin(BACK_HEAD),end(BACK_HEAD),back_head_pdu.begin()+1);

        back_head_pdu.insert(back_head_pdu.end(),begin(BACK_HEAD),end(BACK_HEAD));

        int temp_backhead_size=back_head_pdu.size();
        // copy(missed_batch_nos.begin(),missed_batch_nos.end(),back_head_pdu.begin()+temp_backhead_size);
        back_head_pdu.insert(back_head_pdu.end(),missed_batch_nos.begin(),missed_batch_nos.end());

        sendRawExports(back_head_pdu.data(),back_head_pdu.size());

    }


    void ctrlData(uint8_t batch_index, uint8_t batch_no, uint8_t *payload,int payload_len)
    {
        rbatch_nos.push_back(batch_no);
        RCOUNT=RCOUNT+1;

        cout<<endl<<"data payload: "<<(char*)payload<<" {with len: "<<payload_len<<"}"<<endl;
        cout.flush();



        inports_lock.lock();

        inports.push_front(array<uint8_t,DEFAULT_MTU>());
        array<uint8_t,DEFAULT_MTU> &datum = inports.front();
        datum.fill(0);
        copy(payload,payload+(payload_len),datum.begin());
        inports_lock.unlock();


    }

    void ctrlTRESETTAIL()
    {


    }

    void ctrlCmd(uint8_t *cmd,int cmd_len,uint32_t inports_weight,sockaddr &a_addr,int &a_addrlen)
    {
        if(memcmp(cmd,RRESET_HEAD,cmd_len)==0)
        {
            uint8_t *payload= &babylon_inports[cmd_len];
            int payload_len= inports_weight - (1+cmd_len);
            ctrlRRESETHEAD();
        }
        else if(memcmp(cmd,RRESET_TAIL,cmd_len)==0)
        {
            ctrlRRESETTAIL();
        }
        else if(memcmp(cmd,BACK_HEAD,cmd_len)==0)
        {
            uint8_t *payload= &babylon_inports[cmd_len+1];
            int payload_len= inports_weight - (1+cmd_len);
            memcpy(back_head_data,payload,payload_len);
            back_head_data_len = payload_len;

            cout<<endl<<"missing payload: "<<payload_len<<endl;
            cout.flush();

            cout<<endl<<"reset head"<<endl;
            cout.flush();
            export_addr = a_addr;
            export_addr_len = a_addrlen;

            ctrlBACKHEAD();
        }
        else if(memcmp(cmd,BACK_TAIL,cmd_len)==0)
        {
            ctrlBACKTAIL();
        }
        else if(memcmp(cmd,TRESET_HEAD,cmd_len)==0)
        {
            cout<<endl<<"reset head"<<endl;
            cout.flush();
            inport_addr = a_addr;
            inport_addr_len = a_addrlen;
            ctrlTRESETHEAD();
        }
        else if(memcmp(cmd,TRESET_TAIL,cmd_len)==0)
        {
            ctrlTRESETTAIL();
        }
        else
        {
            cout<<endl<<"\t\t no command error: "<<(char*)cmd<<endl;
            cout.flush();
        }


    }

    int getInport(uint8_t *inport_store,int inport_store_size)
    {
        inports_lock.lock();
            int inports_len = inports.size();
        inports_lock.unlock();

        if(inports_len>0)
        {
            inports_lock.lock();
            array<uint8_t,DEFAULT_MTU> &temp_inport = inports.back();
            copy(temp_inport.begin(),temp_inport.end(),inport_store);

       
            inports.pop_back();
            inports_lock.unlock();
        }

        return inports_len;
    }

 };
