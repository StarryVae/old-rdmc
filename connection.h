
#ifndef CONNECTION_H
#define CONNECTION_H

#include <cassert>
#include <map>
#include <memory>
#include <string>

namespace tcp{

    class socket{
        int sock;
        
        socket(int _sock): sock(_sock) {}

        friend class connection_listener;
    public:
        socket() : sock(-1) {}
        socket(std::string servername, int port);
        socket(socket&& s);

        socket& operator=(socket& s) = delete;
        socket& operator=(socket&& s);

        ~socket();

            
        bool read(char* buffer, size_t size);
        bool write(char* buffer, size_t size);

        template<class T>
        T exchange(T local){
            static_assert(std::is_pod<T>::value,
                          "Can't send non-pod type over TCP");

            assert(sock >= 0);
            
            T remote;
            bool b = write((char*)&local, sizeof(T));
            assert(b);
            b = read((char*)&remote, sizeof(T));
            assert(b);
            return remote;
        }
    };

    class connection_listener{
        std::unique_ptr<int, std::function<void(int*)>> fd;
    public:
        connection_listener(int port);
        socket accept();
    };
    
}

#endif /* CONNECTION_H */
