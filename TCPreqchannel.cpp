#include "common.h"
#include "TCPreqchannel.h"
using namespace std;

/*--------------------------------------------------------------------------*/
/* CONSTRUCTOR/DESTRUCTOR FOR CLASS   R e q u e s t C h a n n e l  */
/*--------------------------------------------------------------------------*/

TCPRequestChannel::TCPRequestChannel(const string host, const string port){
	if(host == "") { // server side
		struct addrinfo hints, *serv;
		struct sockaddr_storage their_addr; // connector's address information
		socklen_t sin_size;
		char s[INET6_ADDRSTRLEN];
		int rv;

		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_PASSIVE; // use my IP

		if ((rv = getaddrinfo(NULL, port.c_str(), &hints, &serv)) != 0) {
			cerr  << "getaddrinfo: " << gai_strerror(rv) << endl;
			exit(-1);
		}
		if ((sockfd = socket(serv->ai_family, serv->ai_socktype, serv->ai_protocol)) == -1) {
			perror("server: socket");
			exit(-1);
		}
		//cout << sockfd << endl;
		if (::bind(sockfd, serv->ai_addr, serv->ai_addrlen) == -1) {
			close(sockfd);
			perror("server: bind");
			exit(-1);
		}
		freeaddrinfo(serv); // all done with this structure

		if (listen(sockfd, 20) == -1) {
			perror("listen");
			exit(1);
		}
		cout << "Server is ready to accept connections in port " << port << endl;
	}else { // client side
		struct addrinfo hints, *res;

		// first, load up address structs with getaddrinfo():
		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		int status;
		//getaddrinfo("www.example.com", "3490", &hints, &res);
		if ((status = getaddrinfo (host.c_str(), port.c_str(), &hints, &res)) != 0) {
			cerr << "getaddrinfo: " << gai_strerror(status) << endl;
			exit(-1);
		}

		// make a socket:
		sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		//cout << sockfd << endl;
		if (sockfd < 0){
			perror ("Cannot create socket");	
			exit(-1);
		}

		// connect!
		if (connect(sockfd, res->ai_addr, res->ai_addrlen)<0){
			perror ("Cannot Connect");
			exit(-1);
		}
		freeaddrinfo(res);
		cout << "Connected " << endl;
	}
}

TCPRequestChannel::TCPRequestChannel(int fd) {
	sockfd = fd;
}

TCPRequestChannel::~TCPRequestChannel(){ 
	close(sockfd);
}

int TCPRequestChannel::cread(void* msgbuf, int bufcapacity){
	return recv(sockfd, msgbuf, bufcapacity, 0); 
}

int TCPRequestChannel::cwrite(void* msgbuf, int len){
	return send(sockfd, msgbuf, len, 0);
}

int TCPRequestChannel::getfd() {
	return sockfd;
}

