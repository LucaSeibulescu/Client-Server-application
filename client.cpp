#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "TCPreqchannel.h"
#include <time.h>
#include <thread>
#include <sys/epoll.h>
using namespace std;

void patient_thread_function(int n, int pno, BoundedBuffer* request_buffer){
    datamsg d(pno, 0.0, 1);
    double resp = 0;
    for(int i = 0; i < n; i++) {
        request_buffer->push((char *) &d, sizeof(datamsg));
        d.seconds += 0.004;
    }
}

void file_thread_function(string fname, BoundedBuffer* request_buffer, TCPRequestChannel* chan, int mb) {
    string recvfname = "recv/" + fname;
    char buf[1024];
    filemsg f(0,0);
    memcpy(buf, &f, sizeof(f));
    strcpy(buf + sizeof(f), fname.c_str());
    chan->cwrite(buf, sizeof(f) + fname.size() + 1);
    __int64_t filelength;
    chan->cread(&filelength, sizeof(filelength));
    FILE* fp = fopen(recvfname.c_str(), "w");
    fseek(fp, filelength, SEEK_SET);
    fclose(fp);

    filemsg* fm = (filemsg*) buf;
    __int64_t remlen = filelength;

    while(remlen > 0) {
        fm->length = min(remlen, (__int64_t) mb);
        request_buffer->push(buf, sizeof(filemsg) + fname.size() + 1);
        fm->offset += fm->length;
        remlen -= fm->length;
    }
}

/*void worker_thread_function(TCPRequestChannel* chan, BoundedBuffer* request_buffer, HistogramCollection* hc, int mb){
    char buf[1024];
    double resp = 0;

    char recvbuf[mb];
    while(true) {
        request_buffer->pop(buf, 1024);
        MESSAGE_TYPE* m = (MESSAGE_TYPE *) buf;
        if(*m == DATA_MSG) {
            chan->cwrite(buf, sizeof(datamsg));
            chan->cread(&resp, sizeof(double));
            hc->update(((datamsg *)buf)->person, resp);
        }
        else if(*m == FILE_MSG) {
            filemsg* fm = (filemsg*) buf;
            string fname = (char *)(fm + 1);
            int sz = sizeof(filemsg) + fname.size() + 1;
            chan->cwrite(buf, sz);
            chan->cread(recvbuf, mb);

            string recvfname = "recv/" + fname;
            FILE* fp = fopen(recvfname.c_str(), "r+");
            fseek(fp, fm->offset, SEEK_SET);
            fwrite(recvbuf, 1, fm->length, fp);
            fclose(fp);
        }
        else if(*m == QUIT_MSG) {
            chan->cwrite(m, sizeof(MESSAGE_TYPE));
            delete chan;
            break;
        }
    }
}*/

void event_polling_function(TCPRequestChannel** chan, BoundedBuffer* request_buffer, HistogramCollection* hc, int mb, int w){
    char buf[1024];
    double resp = 0;

    char recvbuf[mb];

    struct epoll_event ev;
    struct epoll_event events[w];

    // create an empty epoll list
    int epollfd = epoll_create1(0);
    if(epollfd == -1) {
        EXITONERROR("epoll_create1");
    }

    unordered_map<int, int> fd_to_index;
    vector<vector<char>> state(w);

    bool quit_recv = false;

    // priming + adding each rfd to the list
    int nsent = 0, nrecv = 0;
    for(int i = 0; i < w; i++){
        int sz = request_buffer->pop(buf, 1024);
        if(*(MESSAGE_TYPE*) buf == QUIT_MSG) {
            quit_recv = true;
            break;
        }
        chan[i]->cwrite(buf, sz);
        state[i] = vector<char>(buf, buf+sz); // record the state [i]
        nsent++;
        int rfd = chan[i]->getfd();
        fd_to_index[rfd] = i;
        fcntl(rfd, F_SETFL, O_NONBLOCK);

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = rfd;
        
        //cout << rfd << endl;
        if(epoll_ctl(epollfd, EPOLL_CTL_ADD, rfd, &ev) == -1) {
            EXITONERROR("epoll_ctl: listen_sock");
        }
    }

    // nsent = w, nrecv = 0

    while(true) {
        if(quit_recv && nsent == nrecv) {
            break;
        }
        int nfds = epoll_wait(epollfd, events, w, -1);
        if(nfds == -1) {
            EXITONERROR("epoll_wait");
        }
        for(int i = 0; i < nfds; i++) {
            int rfd = events[i].data.fd;
            int index = fd_to_index[rfd];

            int resp_sz = chan[index]->cread(recvbuf, mb);
            nrecv++;

            // process (recvbuf)
            vector<char> req = state[index];
            char* request = req.data();
            // processing the response
            MESSAGE_TYPE* m = (MESSAGE_TYPE *) request;
            if(*m == DATA_MSG){
                hc->update(((datamsg *)request)->person, *(double*)recvbuf);
            }
            else if(*m == FILE_MSG){
                filemsg* fm = (filemsg*) request;
                string fname = (char *)(fm + 1);
                int sz = sizeof(filemsg) + fname.size() + 1;
                //chan[index]->cwrite(buf, sz);
                //chan[index]->cread(recvbuf, mb);

                string recvfname = "recv/" + fname;
                FILE* fp = fopen(recvfname.c_str(), "r+");
                fseek(fp, fm->offset, SEEK_SET);
                fwrite(recvbuf, 1, fm->length, fp);
                fclose(fp);
            }

            // reuse
            if(!quit_recv){
                int req_sz = request_buffer->pop(buf, sizeof(buf));
                if (*(MESSAGE_TYPE*) buf == QUIT_MSG) {
                    quit_recv = true;
                }
                else {
                    chan[index]->cwrite(buf, req_sz);
                    state[index] = vector<char> (buf, buf+req_sz);
                    nsent++;
                }
            }
        }
    }

    /*while(true) {
        request_buffer->pop(buf, 1024);
        MESSAGE_TYPE* m = (MESSAGE_TYPE *) buf;
        if(*m == DATA_MSG) {
            chan->cwrite(buf, sizeof(datamsg));
            chan->cread(&resp, sizeof(double));
            hc->update(((datamsg *)buf)->person, resp);
        }
        else if(*m == FILE_MSG) {
            filemsg* fm = (filemsg*) buf;
            string fname = (char *)(fm + 1);
            int sz = sizeof(filemsg) + fname.size() + 1;
            chan->cwrite(buf, sz);
            chan->cread(recvbuf, mb);

            string recvfname = "recv/" + fname;
            FILE* fp = fopen(recvfname.c_str(), "r+");
            fseek(fp, fm->offset, SEEK_SET);
            fwrite(recvbuf, 1, fm->length, fp);
            fclose(fp);
        }
        else if(*m == QUIT_MSG) {
            chan->cwrite(m, sizeof(MESSAGE_TYPE));
            delete chan;
            break;
        }
    }*/
}



int main(int argc, char *argv[])
{
    int n = 100;    //default number of requests per "patient"
    int p = 10;     // number of patients [1,15]
    int w = 100;    //default number of worker threads
    int b = 100; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the message buffer
    string f;
    srand(time_t(NULL));
    
    int opt = -1;
    string host, port;
    while((opt = getopt(argc, argv, "m:n:b:w:p:h:r:f:")) != -1) {
        switch(opt) {
            case 'm':
                m = atoi(optarg);
                break;
            case 'n':
                n = atoi(optarg);
                break;
            case 'p':
                p = atoi(optarg);
                break;
            case 'b':
                b = atoi(optarg);
                break;
            case 'w':
                w = atoi(optarg);
                break;
            case 'h':
                host = optarg;
                break;
            case 'r':
                port = optarg;
                break;
            case 'f':
                f = optarg;
                break;
        }
    }
    string fname = f;
    string newM = to_string(m);
    
    BoundedBuffer request_buffer(b);
	HistogramCollection hc;
	
	for(int i = 0; i < p; i++) {
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }

    TCPRequestChannel** wchans;
    wchans = new TCPRequestChannel*[w];
    for(int i = 0; i < w; i++) {
        wchans[i] = new TCPRequestChannel(host, port);
        //cout << wchans[i]->getfd() << endl;
    }
    struct timeval start, end;
    gettimeofday (&start, 0);

    /* Start all threads here */
    //thread workers[w];
    thread evp;
    if(f.size() != 0) {
        thread filethread(file_thread_function, fname, &request_buffer, wchans[0], m);
        evp = thread(event_polling_function, wchans, &request_buffer, &hc, m, w);
        /*for(int i = 0; i < w; i++) {
            workers[i] = thread(worker_thread_function, wchans[i], &request_buffer, &hc, m);
        }*/
        filethread.join();
        //cout << "HERE" << endl;
    }
    else {
        thread patient[p];
        for(int i = 0; i < p; i++) {
            patient[i] = thread(patient_thread_function, n, i+1, &request_buffer);
        }
        evp = thread(event_polling_function, wchans, &request_buffer, &hc, m, w);
        /*for(int i = 0; i < w; i++) {
            workers[i] = thread(worker_thread_function, wchans[i], &request_buffer, &hc, m);
        }*/
        for(int i = 0; i < p; i++) {
            patient[i].join();
        }
    }

    //thread filethread(file_thread_function, fname, &request_buffer, chan, m);
    

	/* Join all threads here */
    

    cout << "Patient threads/file thread finished" << endl;
    /*for(int i = 0; i < w; i++) {
        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char*) &q, sizeof(q));
    }*/

    /*for(int i = 0; i < w; i++) {
        workers[i].join();
    }*/

    MESSAGE_TYPE q = QUIT_MSG;
    request_buffer.push((char*) &q, sizeof(q));

    evp.join();

    //cout << "Worker threads finished" << endl;

    gettimeofday (&end, 0);
    // print the results
	hc.print ();
    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    //MESSAGE_TYPE q = QUIT_MSG;

    for(int i = 0; i < w; i++) {
        wchans[i]->cwrite((char *) &q, sizeof (MESSAGE_TYPE));
        delete wchans[i];
    }
    delete[] wchans;
    cout << "All Done!!!" << endl;
}
