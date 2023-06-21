#include<iostream>

using namespace std;
//starting project babylon
#include "Babylon.h"

void pukeInports(Babylon &asock)
{
    while(true)
    {
        uint8_t datum[DEFAULT_MTU];
        memset(datum,0,DEFAULT_MTU);
        int datum_sz =  asock.getInport(datum,DEFAULT_MTU);

        if(datum_sz>0)
        {
            cout<<"received datum: "<<(char*)datum<<" with data len: "<<datum_sz<<endl<<endl;
        }
    }


}
 future<void> pukeman;

int main()
{
    Babylon bsock;
    bsock.initBabylon("192.168.1.151",8055);
    ///Babylon bsock("192.168.43.164",8055);
   /// Babylon bsock("3.111.75.190",8058);
   Babylon asock;
    asock.initBabylon("192.168.1.151",8055);

    pukeman = async(launch::async,pukeInports,ref(bsock));
    while(true)
    {
        char data[DEFAULT_MTU];
        memset(data,0,DEFAULT_MTU);
        cout<<endl<<"PLEASE INPUT: ";
        cin>>data;
        cout<<"\n\t data len: "<<strlen(data)<<endl;
        bsock.sendExports((uint8_t*)data,strlen(data));
    }


   /* while(true)
    {
        char data[DEFAULT_MTU];
        memset(data,0,DEFAULT_MTU);
        bsock.getInports();//(uint8_t*)data,DEFAULT_MTU);

       // cout<<"\n\t recieved len: "<<data<<endl;

    }
    */
}
