/*#include <iostream>
#include <fstream>
#include <vector>
*/
#include "zion.h"
int main() {
    start_zion();
    int zind = zlisten();
    // Open the file in binary mode
    std::ifstream file("max.mp3", std::ios::binary);
    
    if (!file) {
        std::cout << "Error opening file." << std::endl;
        return 1;
    }
    
    // Determine the file size
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    file.seekg(0, std::ios::beg);
    
    // Create a buffer to store the file contents
    std::vector<char> buffer(1024);
    
    // Read the file contents into the buffer
    std::streampos bytesRead = 0;
    int wr_index = 0;
    while (bytesRead < fileSize) {
        file.read(buffer.data(), 1024);
        
        // Get the actual number of bytes read
        std::streamsize bytesReadThisIteration = file.gcount();
        cout<<"bytes to read: "<<bytesReadThisIteration;
        pushZexport(zind,buffer.data(),bytesReadThisIteration);
        // Process the bytes in the buffer here
        /*
        for (int i = 0; i < bytesReadThisIteration; ++i) {
            char byte = buffer[i];
            std::cout << static_cast<int>(byte) << " ";
        }
        */
        bytesRead += bytesReadThisIteration;
    }
    cout<<"\ntotal bytes read: "<<bytesRead<<endl<<endl;
    // Close the file
    file.close();
    
    std::cout << std::endl;
    
    return 0;
}