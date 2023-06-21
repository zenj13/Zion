#include "zion.h"

using namespace std;

int main()
{
    start_zion();
    int zind = zlisten();

    cout<<"\nclient connected: "<<zind<<endl;
    cout.flush();

    while(true)
    {
        
    }
}